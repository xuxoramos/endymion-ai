"""
Pydantic request models for API endpoints.

These models validate incoming requests for write operations.
All write operations create events in cow_events table (event sourcing).
"""

from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class CowCreate(BaseModel):
    """
    Request model for creating a new cow.
    
    This creates a cow_created event in the cow_events table.
    The cow will appear in the cows projection table after sync.
    
    Architecture: Pure Projection Pattern A
    - API writes to cow_events (event sourcing)
    - Databricks Silver layer syncs to cows (projection)
    """
    
    tag_number: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Unique tag/identifier for the cow",
        examples=["US-001", "COW-2024-001"]
    )
    
    name: Optional[str] = Field(
        None,
        max_length=255,
        description="Optional name for the cow",
        examples=["Bessie", "Holstein #1"]
    )
    
    breed: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Breed of the cow",
        examples=["Holstein", "Jersey", "Angus", "Hereford"]
    )
    
    birth_date: date = Field(
        ...,
        description="Date the cow was born",
        examples=["2022-03-15"]
    )
    
    sex: str = Field(
        ...,
        pattern="^(male|female)$",
        description="Sex of the cow",
        examples=["female", "male"]
    )
    
    dam_id: Optional[UUID] = Field(
        None,
        description="UUID of mother cow (dam)"
    )
    
    sire_id: Optional[UUID] = Field(
        None,
        description="UUID of father cow (sire)"
    )
    
    weight_kg: Optional[float] = Field(
        None,
        gt=0,
        le=2000,
        description="Initial weight in kilograms",
        examples=[450.5, 325.0]
    )
    
    current_location: Optional[str] = Field(
        None,
        max_length=255,
        description="Current location or paddock",
        examples=["Barn A", "Paddock 3", "North Field"]
    )
    
    notes: Optional[str] = Field(
        None,
        max_length=1000,
        description="Additional notes about the cow"
    )
    
    @field_validator('birth_date')
    @classmethod
    def validate_birth_date(cls, v: date) -> date:
        """Ensure birth date is not in the future."""
        if v > date.today():
            raise ValueError('Birth date cannot be in the future')
        
        # Reasonable limit: not more than 30 years old
        max_age_years = 30
        min_birth_date = date.today().replace(year=date.today().year - max_age_years)
        if v < min_birth_date:
            raise ValueError(f'Birth date cannot be more than {max_age_years} years ago')
        
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "tag_number": "US-001",
                "name": "Bessie",
                "breed": "Holstein",
                "birth_date": "2022-01-15",
                "sex": "female",
                "weight_kg": 450.5,
                "current_location": "Barn A",
                "notes": "Excellent milk producer"
            }
        }


class CowUpdate(BaseModel):
    """
    Request model for updating an existing cow.
    
    This creates a cow_updated event in the cow_events table.
    Changes will appear in the cows projection table after sync.
    
    All fields are optional - only provided fields are updated.
    """
    
    name: Optional[str] = Field(
        None,
        max_length=255,
        description="Update cow name"
    )
    
    breed: Optional[str] = Field(
        None,
        min_length=1,
        max_length=100,
        description="Update breed (rare, but possible)"
    )
    
    weight_kg: Optional[float] = Field(
        None,
        gt=0,
        le=2000,
        description="Update weight in kilograms"
    )
    
    current_location: Optional[str] = Field(
        None,
        max_length=255,
        description="Update current location"
    )
    
    notes: Optional[str] = Field(
        None,
        max_length=1000,
        description="Update notes"
    )
    
    status: Optional[str] = Field(
        None,
        pattern="^(active|sold|deceased|transferred)$",
        description="Update status"
    )
    
    @field_validator('status')
    @classmethod
    def validate_status(cls, v: Optional[str]) -> Optional[str]:
        """Validate status transitions."""
        # Note: In production, you'd want to validate allowed transitions
        # e.g., can't go from deceased back to active
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "weight_kg": 475.0,
                "current_location": "Barn B",
                "notes": "Moved to new location after weaning"
            }
        }


class CowDeactivate(BaseModel):
    """
    Request model for deactivating (logical delete) a cow.
    
    This creates a cow_deactivated event in the cow_events table.
    The cow will be marked as inactive in the projection after sync.
    """
    
    reason: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Reason for deactivation",
        examples=["Sold to another farm", "Deceased", "Transferred to another location"]
    )
    
    deactivation_date: Optional[date] = Field(
        None,
        description="Date of deactivation (defaults to today)"
    )
    
    notes: Optional[str] = Field(
        None,
        max_length=1000,
        description="Additional notes about deactivation"
    )
    
    @field_validator('deactivation_date')
    @classmethod
    def validate_deactivation_date(cls, v: Optional[date]) -> Optional[date]:
        """Ensure deactivation date is not in the future."""
        if v and v > date.today():
            raise ValueError('Deactivation date cannot be in the future')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "reason": "Sold to Smith Farm",
                "deactivation_date": "2024-06-15",
                "notes": "Sold for $2,500"
            }
        }


class CowWeightRecord(BaseModel):
    """
    Request model for recording a cow weight measurement.
    
    This creates a cow_weight_recorded event in the cow_events table.
    The projection will show the most recent weight after sync.
    """
    
    weight_kg: float = Field(
        ...,
        gt=0,
        le=2000,
        description="Weight in kilograms",
        examples=[475.5, 520.0]
    )
    
    measurement_date: Optional[datetime] = Field(
        None,
        description="When measurement was taken (defaults to now)"
    )
    
    measured_by: Optional[str] = Field(
        None,
        max_length=255,
        description="Person who took the measurement",
        examples=["John Smith", "Veterinarian"]
    )
    
    measurement_method: Optional[str] = Field(
        None,
        max_length=100,
        description="Method used for measurement",
        examples=["Scale", "Weight tape", "Visual estimate"]
    )
    
    notes: Optional[str] = Field(
        None,
        max_length=500,
        description="Additional notes about measurement"
    )
    
    @field_validator('measurement_date')
    @classmethod
    def validate_measurement_date(cls, v: Optional[datetime]) -> Optional[datetime]:
        """Ensure measurement date is not in the future."""
        if v:
            # Make comparison timezone-aware
            now = datetime.now(v.tzinfo) if v.tzinfo else datetime.utcnow()
            if v > now:
                raise ValueError('Measurement date cannot be in the future')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "weight_kg": 475.5,
                "measurement_date": "2024-06-15T10:30:00Z",
                "measured_by": "John Smith",
                "measurement_method": "Scale",
                "notes": "Healthy weight gain"
            }
        }


class CowHealthEvent(BaseModel):
    """
    Request model for recording a cow health event.
    
    This creates a cow_health_event in the cow_events table.
    Health events are tracked separately for audit and analysis.
    """
    
    event_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Type of health event",
        examples=["Vaccination", "Treatment", "Illness", "Checkup", "Injury"]
    )
    
    event_date: Optional[datetime] = Field(
        None,
        description="When event occurred (defaults to now)"
    )
    
    description: str = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="Description of the health event"
    )
    
    veterinarian: Optional[str] = Field(
        None,
        max_length=255,
        description="Veterinarian name"
    )
    
    treatment: Optional[str] = Field(
        None,
        max_length=500,
        description="Treatment administered"
    )
    
    medication: Optional[str] = Field(
        None,
        max_length=255,
        description="Medication name and dosage"
    )
    
    cost: Optional[float] = Field(
        None,
        ge=0,
        description="Cost of treatment/medication"
    )
    
    follow_up_required: bool = Field(
        False,
        description="Whether follow-up is required"
    )
    
    follow_up_date: Optional[date] = Field(
        None,
        description="Date for follow-up"
    )
    
    @field_validator('event_date')
    @classmethod
    def validate_event_date(cls, v: Optional[datetime]) -> Optional[datetime]:
        """Ensure event date is not in the future."""
        if v and v > datetime.utcnow():
            raise ValueError('Event date cannot be in the future')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "Vaccination",
                "event_date": "2024-06-15T14:00:00Z",
                "description": "Annual vaccination for common cattle diseases",
                "veterinarian": "Dr. Jane Smith",
                "medication": "Bovine vaccine combo - 2ml",
                "cost": 45.00,
                "follow_up_required": True,
                "follow_up_date": "2024-07-15"
            }
        }
