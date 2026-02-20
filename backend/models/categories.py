"""
Category model - Reference data for categorization.

Categories provide a flexible taxonomy system for organizing cows
and other entities. Supports both global (system-wide) and tenant-specific
categories with hierarchical relationships.
"""

from datetime import datetime
from typing import Optional, List
from uuid import UUID, uuid4

from sqlalchemy import String, Boolean, Index, CheckConstraint, ForeignKey
from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TenantBaseModel


class Category(Base):
    """
    Category reference data for flexible taxonomy.
    
    Categories can be:
    - Global: tenant_id = NULL, visible to all tenants
    - Tenant-specific: tenant_id = UUID, visible only to that tenant
    - Hierarchical: parent_category_id allows tree structures
    
    Use Cases:
    - Cow categorization (breeding stock, dairy, beef)
    - Health categories (vaccination, treatment types)
    - Location categories (barns, paddocks, regions)
    - Custom tenant-defined taxonomies
    
    Architecture Note:
    This is NOT a projection table. Categories can be created/updated
    directly via the API (for tenant-specific categories) or managed
    by administrators (for global categories).
    """
    
    __tablename__ = "categories"
    __table_args__ = (
        CheckConstraint(
            "(tenant_id IS NULL AND is_global = 1) OR (tenant_id IS NOT NULL AND is_global = 0)",
            name="CK_categories_global_tenant"
        ),
        Index("IX_categories_tenant_id", "tenant_id", "category_type"),
        Index("IX_categories_parent", "parent_category_id", "tenant_id"),
        Index("IX_categories_type", "category_type", "tenant_id", "is_active"),
        Index("UQ_categories_name_type_tenant", "name", "category_type", "tenant_id", unique=True),
        {"schema": "operational"}
    )
    
    # Primary Key (using standard GUID without mixin)
    id: Mapped[UUID] = mapped_column(
        UNIQUEIDENTIFIER,
        primary_key=True,
        default=uuid4,
        doc="Unique category identifier"
    )
    
    # Tenant Association (nullable for global categories)
    tenant_id: Mapped[Optional[UUID]] = mapped_column(
        UNIQUEIDENTIFIER,
        nullable=True,
        doc="Tenant ID (NULL for global categories)"
    )
    
    # Category Information
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        doc="Category name"
    )
    
    category_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        doc="Type of category (e.g., 'cow_type', 'health', 'location')"
    )
    
    description: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
        doc="Detailed description of the category"
    )
    
    # Hierarchy
    parent_category_id: Mapped[Optional[UUID]] = mapped_column(
        UNIQUEIDENTIFIER,
        ForeignKey("operational.categories.id"),
        nullable=True,
        doc="Parent category for hierarchical structures"
    )
    
    # Status
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        doc="Whether category is currently active"
    )
    
    is_global: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        doc="Whether category is global (visible to all tenants)"
    )
    
    # Display Properties
    display_order: Mapped[int] = mapped_column(
        nullable=False,
        default=0,
        doc="Sort order for displaying categories"
    )
    
    color_hex: Mapped[Optional[str]] = mapped_column(
        String(7),
        nullable=True,
        doc="Hex color code for UI display (e.g., #FF5733)"
    )
    
    icon_name: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        doc="Icon identifier for UI display"
    )
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default=datetime.utcnow,
        doc="When category was created"
    )
    
    updated_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        doc="When category was last updated"
    )
    
    # Metadata
    created_by: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        doc="User who created the category"
    )
    
    # Relationships
    parent = relationship(
        "Category",
        remote_side=[id],
        backref="children",
        doc="Parent category relationship"
    )
    
    def __repr__(self) -> str:
        """String representation for debugging."""
        scope = "global" if self.is_global else f"tenant:{self.tenant_id}"
        return (
            f"<Category("
            f"id={self.id}, "
            f"name={self.name}, "
            f"type={self.category_type}, "
            f"scope={scope}"
            f")>"
        )
    
    def __str__(self) -> str:
        """Human-readable string representation."""
        parent_str = f" > {self.parent.name}" if self.parent_category_id else ""
        return f"{self.category_type}: {self.name}{parent_str}"
    
    @property
    def full_path(self) -> str:
        """
        Get full hierarchical path of category.
        
        Returns:
            String like "Parent > Child > Grandchild"
        """
        if not self.parent_category_id:
            return self.name
        
        # Note: In production, you'd want to avoid N+1 queries here
        # Use eager loading or caching
        path_parts = [self.name]
        current = self.parent
        
        while current:
            path_parts.insert(0, current.name)
            current = current.parent if hasattr(current, 'parent') else None
        
        return " > ".join(path_parts)
    
    @property
    def depth(self) -> int:
        """
        Get depth level in hierarchy (0 = root).
        
        Returns:
            Depth level (0 for root categories)
        """
        if not self.parent_category_id:
            return 0
        
        depth = 1
        current = self.parent
        
        while current and hasattr(current, 'parent_category_id') and current.parent_category_id:
            depth += 1
            current = current.parent if hasattr(current, 'parent') else None
        
        return depth
    
    def is_descendant_of(self, category_id: UUID) -> bool:
        """
        Check if this category is a descendant of another category.
        
        Args:
            category_id: ID of potential ancestor category
            
        Returns:
            True if this category is a descendant
        """
        if not self.parent_category_id:
            return False
        
        if self.parent_category_id == category_id:
            return True
        
        if self.parent:
            return self.parent.is_descendant_of(category_id)
        
        return False
    
    def is_visible_to_tenant(self, tenant_id: UUID) -> bool:
        """
        Check if category is visible to a specific tenant.
        
        Args:
            tenant_id: Tenant identifier
            
        Returns:
            True if global or owned by the tenant
        """
        return self.is_global or self.tenant_id == tenant_id
    
    def to_dict(self, include_hierarchy: bool = False) -> dict:
        """
        Convert category to dictionary for serialization.
        
        Args:
            include_hierarchy: Whether to include hierarchy information
            
        Returns:
            Dictionary representation of the category
        """
        result = {
            "id": str(self.id),
            "tenant_id": str(self.tenant_id) if self.tenant_id else None,
            "name": self.name,
            "category_type": self.category_type,
            "description": self.description,
            "parent_category_id": str(self.parent_category_id) if self.parent_category_id else None,
            "is_active": self.is_active,
            "is_global": self.is_global,
            "display_order": self.display_order,
            "color_hex": self.color_hex,
            "icon_name": self.icon_name,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "created_by": self.created_by,
        }
        
        if include_hierarchy:
            result["hierarchy"] = {
                "full_path": self.full_path,
                "depth": self.depth,
                "has_parent": self.parent_category_id is not None,
                "has_children": len(self.children) > 0 if hasattr(self, 'children') else False,
            }
        
        return result
    
    @classmethod
    def get_global_categories(cls, session, category_type: Optional[str] = None):
        """
        Get all global categories.
        
        Args:
            session: SQLAlchemy session
            category_type: Optional filter by category type
            
        Returns:
            Query for global categories
        """
        query = session.query(cls).filter(cls.is_global == True, cls.is_active == True)
        
        if category_type:
            query = query.filter(cls.category_type == category_type)
        
        return query.order_by(cls.display_order.asc(), cls.name.asc())
    
    @classmethod
    def get_tenant_categories(
        cls,
        session,
        tenant_id: UUID,
        category_type: Optional[str] = None,
        include_global: bool = True
    ):
        """
        Get categories visible to a tenant.
        
        Args:
            session: SQLAlchemy session
            tenant_id: Tenant identifier
            category_type: Optional filter by category type
            include_global: Whether to include global categories
            
        Returns:
            Query for tenant-visible categories
        """
        filters = [cls.is_active == True]
        
        if include_global:
            filters.append((cls.tenant_id == tenant_id) | (cls.is_global == True))
        else:
            filters.append(cls.tenant_id == tenant_id)
        
        if category_type:
            filters.append(cls.category_type == category_type)
        
        query = session.query(cls).filter(*filters)
        return query.order_by(cls.display_order.asc(), cls.name.asc())
    
    @classmethod
    def get_root_categories(cls, session, tenant_id: Optional[UUID] = None):
        """
        Get root-level categories (no parent).
        
        Args:
            session: SQLAlchemy session
            tenant_id: Optional tenant filter
            
        Returns:
            Query for root categories
        """
        query = (
            session.query(cls)
            .filter(
                cls.parent_category_id == None,
                cls.is_active == True
            )
        )
        
        if tenant_id:
            query = query.filter((cls.tenant_id == tenant_id) | (cls.is_global == True))
        
        return query.order_by(cls.display_order.asc(), cls.name.asc())
    
    @classmethod
    def get_by_type(cls, session, category_type: str, tenant_id: UUID):
        """
        Get all categories of a specific type for a tenant.
        
        Args:
            session: SQLAlchemy session
            category_type: Type of category
            tenant_id: Tenant identifier
            
        Returns:
            Query for categories of specified type
        """
        return cls.get_tenant_categories(
            session,
            tenant_id=tenant_id,
            category_type=category_type,
            include_global=True
        )
    
    @classmethod
    def create_category(
        cls,
        name: str,
        category_type: str,
        tenant_id: Optional[UUID] = None,
        is_global: bool = False,
        parent_category_id: Optional[UUID] = None,
        description: Optional[str] = None,
        created_by: Optional[str] = None,
        **kwargs
    ) -> "Category":
        """
        Factory method to create a new category.
        
        Args:
            name: Category name
            category_type: Type of category
            tenant_id: Tenant ID (None for global)
            is_global: Whether category is global
            parent_category_id: Parent category ID for hierarchy
            description: Category description
            created_by: User creating the category
            **kwargs: Additional fields
            
        Returns:
            New Category instance (not yet persisted)
            
        Raises:
            ValueError: If global category has tenant_id or vice versa
        """
        if is_global and tenant_id is not None:
            raise ValueError("Global categories cannot have a tenant_id")
        
        if not is_global and tenant_id is None:
            raise ValueError("Non-global categories must have a tenant_id")
        
        return cls(
            name=name,
            category_type=category_type,
            tenant_id=tenant_id,
            is_global=is_global,
            parent_category_id=parent_category_id,
            description=description,
            created_by=created_by,
            **kwargs
        )
