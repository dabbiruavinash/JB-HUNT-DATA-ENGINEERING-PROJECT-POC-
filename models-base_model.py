from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any

class BaseModel(ABC):
    """Abstract base model for all data models"""
    
    def __init__(self, created_date: datetime = None, modified_date: datetime = None):
        self.created_date = created_date or datetime.now()
        self.modified_date = modified_date or datetime.now()
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary"""
        pass
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Create model from dictionary"""
        pass
    
    def update_modified_date(self):
        """Update modified date"""
        self.modified_date = datetime.now()

class Shipment(BaseModel):
    """Shipment model representing JB Hunt shipments"""
    
    def __init__(self, shipment_id: str, origin: str, destination: str, 
                 weight: float, status: str, **kwargs):
        super().__init__(kwargs.get('created_date'), kwargs.get('modified_date'))
        self.shipment_id = shipment_id
        self.origin = origin
        self.destination = destination
        self.weight = weight
        self.status = status
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'shipment_id': self.shipment_id,
            'origin': self.origin,
            'destination': self.destination,
            'weight': self.weight,
            'status': self.status,
            'created_date': self.created_date.isoformat(),
            'modified_date': self.modified_date.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        return cls(
            shipment_id=data['shipment_id'],
            origin=data['origin'],
            destination=data['destination'],
            weight=data['weight'],
            status=data['status'],
            created_date=datetime.fromisoformat(data['created_date']) if 'created_date' in data else None,
            modified_date=datetime.fromisoformat(data['modified_date']) if 'modified_date' in data else None
        )