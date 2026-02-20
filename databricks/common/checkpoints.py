"""
Checkpoint Tracking for Incremental Processing

Manages watermarks/checkpoints for Bronze, Silver, and Gold layers to enable
efficient incremental data processing. Tracks:
- Last processed event timestamp
- Last processed Delta Lake version
- Processing metrics and statistics

Storage: JSON file in MinIO (s3a://bronze/checkpoints/)
Alternative: Could use SQL Server table for production

Usage:
    from databricks.common.checkpoints import CheckpointManager
    
    manager = CheckpointManager(spark)
    
    # Get last checkpoint
    last_time = manager.get_checkpoint('bronze', 'last_event_time')
    
    # Save new checkpoint
    manager.save_checkpoint('bronze', 'last_event_time', datetime.utcnow())
"""

import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pathlib import Path
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


# ========================================
# Configuration
# ========================================

CHECKPOINT_PATH = "s3a://bronze/checkpoints"
LOCAL_FALLBACK_PATH = "/tmp/endymion_ai_checkpoints.json"


# ========================================
# Checkpoint Manager
# ========================================

class CheckpointManager:
    """
    Manages checkpoints for incremental processing.
    
    Checkpoints track the last successfully processed point in each layer,
    allowing the pipeline to resume from where it left off instead of
    reprocessing all data.
    """
    
    def __init__(self, spark: Optional[SparkSession] = None, use_local: bool = False):
        """
        Initialize checkpoint manager.
        
        Args:
            spark: SparkSession for S3/MinIO access (None for local mode)
            use_local: Force local file storage (for testing)
        """
        self.spark = spark
        self.use_local = use_local or spark is None
        self.checkpoints = self._load_checkpoints()
    
    def _get_checkpoint_path(self) -> str:
        """Get checkpoint storage path."""
        if self.use_local:
            return LOCAL_FALLBACK_PATH
        return f"{CHECKPOINT_PATH}/checkpoints.json"
    
    def _load_checkpoints(self) -> Dict[str, Any]:
        """Load checkpoints from storage."""
        try:
            if self.use_local:
                # Load from local file
                path = Path(LOCAL_FALLBACK_PATH)
                if path.exists():
                    with open(path, 'r') as f:
                        data = json.load(f)
                        logger.info(f"Loaded checkpoints from {LOCAL_FALLBACK_PATH}")
                        return data
                else:
                    logger.info("No local checkpoint file found, starting fresh")
                    return {}
            else:
                # Load from S3/MinIO using Spark
                checkpoint_path = self._get_checkpoint_path()
                try:
                    df = self.spark.read.text(checkpoint_path)
                    json_str = df.first()[0]
                    data = json.loads(json_str)
                    logger.info(f"Loaded checkpoints from {checkpoint_path}")
                    return data
                except Exception as e:
                    logger.info(f"No checkpoint found in MinIO (creating new): {e}")
                    return {}
        except Exception as e:
            logger.warning(f"Error loading checkpoints: {e}, starting fresh")
            return {}
    
    def _save_checkpoints(self):
        """Save checkpoints to storage."""
        try:
            # Add metadata
            data = {
                **self.checkpoints,
                "_metadata": {
                    "last_updated": datetime.utcnow().isoformat(),
                    "version": "1.0"
                }
            }
            
            json_str = json.dumps(data, indent=2, default=str)
            
            if self.use_local:
                # Save to local file
                path = Path(LOCAL_FALLBACK_PATH)
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, 'w') as f:
                    f.write(json_str)
                logger.info(f"Saved checkpoints to {LOCAL_FALLBACK_PATH}")
            else:
                # Save to S3/MinIO using Spark
                checkpoint_path = self._get_checkpoint_path()
                
                # Create temporary RDD and save as text file
                rdd = self.spark.sparkContext.parallelize([json_str])
                df = self.spark.createDataFrame(rdd.map(lambda x: (x,)), ["value"])
                
                # Overwrite checkpoint file
                df.coalesce(1).write.mode("overwrite").text(CHECKPOINT_PATH)
                logger.info(f"Saved checkpoints to {checkpoint_path}")
        except Exception as e:
            logger.error(f"Error saving checkpoints: {e}")
            raise
    
    def get_checkpoint(self, layer: str, key: str, default: Any = None) -> Any:
        """
        Get checkpoint value.
        
        Args:
            layer: Layer name ('bronze', 'silver', 'gold')
            key: Checkpoint key (e.g., 'last_event_time', 'last_version')
            default: Default value if checkpoint doesn't exist
        
        Returns:
            Checkpoint value or default
        
        Example:
            last_time = manager.get_checkpoint('bronze', 'last_event_time')
        """
        layer_checkpoints = self.checkpoints.get(layer, {})
        value = layer_checkpoints.get(key, default)
        
        # Convert ISO string back to datetime if it looks like a timestamp
        if isinstance(value, str) and 'T' in value:
            try:
                value = datetime.fromisoformat(value.replace('Z', '+00:00'))
            except:
                pass
        
        return value
    
    def save_checkpoint(self, layer: str, key: str, value: Any):
        """
        Save checkpoint value.
        
        Args:
            layer: Layer name ('bronze', 'silver', 'gold')
            key: Checkpoint key (e.g., 'last_event_time', 'last_version')
            value: Checkpoint value (will be JSON-serialized)
        
        Example:
            manager.save_checkpoint('bronze', 'last_event_time', datetime.utcnow())
        """
        if layer not in self.checkpoints:
            self.checkpoints[layer] = {}
        
        # Convert datetime to ISO string for JSON serialization
        if isinstance(value, datetime):
            value = value.isoformat()
        
        self.checkpoints[layer][key] = value
        self._save_checkpoints()
        
        logger.info(f"Saved checkpoint: {layer}.{key} = {value}")
    
    def get_all_checkpoints(self, layer: Optional[str] = None) -> Dict[str, Any]:
        """
        Get all checkpoints for a layer or all layers.
        
        Args:
            layer: Layer name, or None for all layers
        
        Returns:
            Dictionary of checkpoints
        """
        if layer:
            return self.checkpoints.get(layer, {})
        return self.checkpoints
    
    def reset_checkpoint(self, layer: str, key: Optional[str] = None):
        """
        Reset checkpoint(s).
        
        Args:
            layer: Layer name
            key: Specific key to reset, or None to reset entire layer
        
        Example:
            # Reset specific checkpoint
            manager.reset_checkpoint('bronze', 'last_event_time')
            
            # Reset all Bronze checkpoints
            manager.reset_checkpoint('bronze')
        """
        if key:
            if layer in self.checkpoints and key in self.checkpoints[layer]:
                del self.checkpoints[layer][key]
                logger.info(f"Reset checkpoint: {layer}.{key}")
        else:
            if layer in self.checkpoints:
                del self.checkpoints[layer]
                logger.info(f"Reset all checkpoints for layer: {layer}")
        
        self._save_checkpoints()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get checkpoint statistics.
        
        Returns:
            Dictionary with checkpoint metadata and stats
        """
        stats = {
            "layers": list(self.checkpoints.keys()),
            "checkpoint_count": sum(len(v) for v in self.checkpoints.values() if isinstance(v, dict)),
            "storage_mode": "local" if self.use_local else "minio",
            "path": self._get_checkpoint_path()
        }
        
        if "_metadata" in self.checkpoints:
            stats["last_updated"] = self.checkpoints["_metadata"].get("last_updated")
        
        return stats


# ========================================
# Helper Functions
# ========================================

def get_bronze_checkpoint(spark: SparkSession) -> Optional[datetime]:
    """
    Get last processed event timestamp for Bronze layer.
    
    Returns:
        Last processed timestamp or None if no checkpoint exists
    """
    manager = CheckpointManager(spark)
    return manager.get_checkpoint('bronze', 'last_event_time')


def save_bronze_checkpoint(spark: SparkSession, event_time: datetime):
    """
    Save last processed event timestamp for Bronze layer.
    
    Args:
        spark: SparkSession
        event_time: Last successfully processed event timestamp
    """
    manager = CheckpointManager(spark)
    manager.save_checkpoint('bronze', 'last_event_time', event_time)


def get_silver_checkpoint(spark: SparkSession) -> Optional[int]:
    """
    Get last processed Bronze version for Silver layer.
    
    Returns:
        Last processed Delta version or None
    """
    manager = CheckpointManager(spark)
    return manager.get_checkpoint('silver', 'last_bronze_version')


def save_silver_checkpoint(spark: SparkSession, version: int):
    """
    Save last processed Bronze version for Silver layer.
    
    Args:
        spark: SparkSession
        version: Last successfully processed Bronze Delta version
    """
    manager = CheckpointManager(spark)
    manager.save_checkpoint('silver', 'last_bronze_version', version)
