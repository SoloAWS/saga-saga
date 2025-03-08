from dataclasses import dataclass, field
from datetime import datetime
import uuid
from typing import List, Optional, Dict, Any

from .enums import SagaStatus, StepStatus, ServiceType, StepType

@dataclass
class SagaStep:
    """
    Representa un paso individual en una saga.
    """
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    saga_id: uuid.UUID = None
    step_type: StepType = None
    service: ServiceType = None
    status: StepStatus = StepStatus.PENDING
    event_id: Optional[str] = None
    event_type: Optional[str] = None
    entity_id: Optional[str] = None  # ID de la entidad afectada (ej: image_id, task_id)
    metadata: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    compensation_timestamp: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "saga_id": str(self.saga_id) if self.saga_id else None,
            "step_type": self.step_type.value if self.step_type else None,
            "service": self.service.value if self.service else None,
            "status": self.status.value,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "entity_id": self.entity_id,
            "metadata": self.metadata,
            "error_message": self.error_message,
            "timestamp": self.timestamp.isoformat(),
            "compensation_timestamp": self.compensation_timestamp.isoformat() if self.compensation_timestamp else None
        }

@dataclass
class SagaLog:
    """
    Representa el registro completo de una saga, incluyendo todos sus pasos.
    """
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    correlation_id: Optional[str] = None
    saga_type: str = "IMAGE_PROCESSING"  # Tipo de saga, puede ampliarse en el futuro
    status: SagaStatus = SagaStatus.STARTED
    steps: List[SagaStep] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_step(self, step: SagaStep) -> None:
        """Añade un paso a la saga"""
        step.saga_id = self.id
        self.steps.append(step)
    
    def mark_completed(self) -> None:
        """Marca la saga como completada"""
        self.status = SagaStatus.COMPLETED
        self.end_time = datetime.now()
    
    def mark_failed(self, error_message: str) -> None:
        """Marca la saga como fallida"""
        self.status = SagaStatus.FAILED
        self.error_message = error_message
        self.end_time = datetime.now()
    
    def start_compensation(self) -> None:
        """Inicia el proceso de compensación"""
        self.status = SagaStatus.COMPENSATING
    
    def mark_compensation_succeeded(self) -> None:
        """Marca la compensación como exitosa"""
        self.status = SagaStatus.COMPENSATION_SUCCEEDED
        self.end_time = datetime.now()
    
    def mark_compensation_failed(self, error_message: str) -> None:
        """Marca la compensación como fallida"""
        self.status = SagaStatus.COMPENSATION_FAILED
        self.error_message = error_message
        self.end_time = datetime.now()
    
    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "correlation_id": self.correlation_id,
            "saga_type": self.saga_type,
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "error_message": self.error_message,
            "metadata": self.metadata,
            "steps": [step.to_dict() for step in self.steps]
        }