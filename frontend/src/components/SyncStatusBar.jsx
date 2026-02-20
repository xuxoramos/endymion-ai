import React, { useState, useEffect } from 'react';
import { cowApi } from '../services/api';

export default function SyncStatusBar() {
  const [syncStatus, setSyncStatus] = useState({
    lastChecked: null,
    cowCount: 0,
    status: 'healthy',
  });

  useEffect(() => {
    const fetchSyncStatus = async () => {
      try {
        // Get cows to check if sync is working
        const cows = await cowApi.getCows();
        
        // Check the most recent sync time from the cows
        let mostRecentSync = null;
        if (cows.length > 0) {
          mostRecentSync = cows.reduce((latest, cow) => {
            const cowSync = cow.last_synced_at ? new Date(cow.last_synced_at) : null;
            if (!cowSync) return latest;
            if (!latest) return cowSync;
            return cowSync > latest ? cowSync : latest;
          }, null);
        }
        
        setSyncStatus({
          lastChecked: new Date(),
          mostRecentSync: mostRecentSync,
          cowCount: cows.length,
          status: 'healthy',
        });
      } catch (error) {
        console.error('Failed to fetch sync status:', error);
        setSyncStatus(prev => ({
          ...prev,
          status: 'error',
          lastChecked: new Date(),
        }));
      }
    };

    fetchSyncStatus();
    const interval = setInterval(fetchSyncStatus, 1000); // Check every 1s for responsive updates

    return () => clearInterval(interval);
  }, []);

  const getSyncStatusColor = () => {
    if (syncStatus.status === 'error') return 'critical';
    if (!syncStatus.mostRecentSync) return 'warning';
    
    const now = new Date();
    const syncAge = Math.floor((now - syncStatus.mostRecentSync) / 1000);
    
    if (syncAge < 10) return 'healthy';
    if (syncAge < 60) return 'warning';
    return 'critical';
  };

  const getSyncStatusText = () => {
    if (syncStatus.status === 'error') return 'Sync Error';
    if (!syncStatus.mostRecentSync) return 'Waiting for sync';
    
    const now = new Date();
    const syncAge = Math.floor((now - syncStatus.mostRecentSync) / 1000);
    
    if (syncAge < 10) return '✅ Active';
    if (syncAge < 60) return 'Recently synced';
    return 'Sync delayed';
  };

  const formatTimestamp = (timestamp) => {
    if (!timestamp) return 'Never';
    const date = new Date(timestamp);
    const now = new Date();
    const diffSeconds = Math.floor((now - date) / 1000);
    
    if (diffSeconds < 10) return 'Just now';
    if (diffSeconds < 60) return `${diffSeconds}s ago`;
    if (diffSeconds < 3600) return `${Math.floor(diffSeconds / 60)}m ago`;
    return date.toLocaleTimeString();
  };

  return (
    <div className="sync-status-bar">
      <div className="sync-indicator">
        <div className={`sync-dot ${getSyncStatusColor()}`}></div>
        <div>
          <div style={{ fontWeight: 600, fontSize: '1.125rem' }}>
            {getSyncStatusText()}
          </div>
          <div style={{ fontSize: '0.875rem', color: 'var(--gray-600)' }}>
            Last synced: {formatTimestamp(syncStatus.mostRecentSync)}
          </div>
        </div>
      </div>

      <div className="sync-info">
        <div className="sync-metric">
          <div className="sync-metric-label">Sync Service</div>
          <div className="sync-metric-value" style={{ 
            color: syncStatus.mostRecentSync ? 'var(--success)' : 'var(--warning)' 
          }}>
            {syncStatus.mostRecentSync ? 'Running' : 'Starting'}
          </div>
        </div>

        <div className="sync-metric">
          <div className="sync-metric-label">Cows in Projection</div>
          <div className="sync-metric-value">
            {syncStatus.cowCount}
          </div>
        </div>

        <div className="sync-metric">
          <div className="sync-metric-label">Sync Interval</div>
          <div className="sync-metric-value">5s</div>
        </div>

        <div className="sync-metric">
          <div className="sync-metric-label">Last Check</div>
          <div className="sync-metric-value" style={{ fontSize: '0.875rem' }}>
            {syncStatus.lastChecked ? formatTimestamp(syncStatus.lastChecked) : 'N/A'}
          </div>
        </div>
      </div>
    </div>
  );
}
