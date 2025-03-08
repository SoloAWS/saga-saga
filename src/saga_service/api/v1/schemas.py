from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum

# Enumeraciones
class SagaStatusEnum(str, Enum):
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPENSATION_SUCCEEDED = "COMPENSATION_SUCCEEDED"
    COMPENSATION_FAILED = "COMPENSATION_FAILED"

class StepStatusEnum(str, Enum):
    PENDING = "PENDING"
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    COMPENSATION_FAILED = "COMPENSATION_FAILED"

class ServiceTypeEnum(str, Enum):
    DATA_RETRIEVAL = "DATA_RETRIEVAL"
    ANONYMIZATION = "ANONYMIZATION"
    PROCESSING = "PROCESSING"

class StepTypeEnum(str, Enum):
    DATA_RETRIEVAL_CREATE = "DATA_RETRIEVAL_CREATE"
    DATA_RETRIEVAL_START = "DATA_RETRIEVAL_START"
    DATA_RETRIEVAL_UPLOAD = "DATA_RETRIEVAL_UPLOAD"
    ANONYMIZATION_REQUEST = "ANONYMIZATION_REQUEST"
    ANONYMIZATION_COMPLETE = "ANONYMIZATION_COMPLETE"
    PROCESSING_REQUEST = "PROCESSING_REQUEST"
    PROCESSING_COMPLETE = "PROCESSING_COMPLETE"

# Modelos de respuesta
class SagaStepResponse(BaseModel):
    id: str
    saga_id: str
    step_type: StepTypeEnum
    service: ServiceTypeEnum
    status: StepStatusEnum
    event_id: Optional[str] = None
    event_type: Optional[str] = None
    entity_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    timestamp: datetime
    compensation_timestamp: Optional[datetime] = None

class SagaLogResponse(BaseModel):
    id: str
    correlation_id: Optional[str] = None
    saga_type: str
    status: SagaStatusEnum
    steps: List[SagaStepResponse]
    start_time: datetime
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class SagaLogSummaryResponse(BaseModel):
    id: str
    correlation_id: Optional[str] = None
    saga_type: str
    status: SagaStatusEnum
    steps_count: int = Field(..., description="Número total de pasos")
    failed_steps_count: int = Field(..., description="Número de pasos fallidos")
    start_time: datetime
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None

class PaginatedSagaLogsResponse(BaseModel):
    total: int
    offset: int
    limit: int
    items: List[SagaLogSummaryResponse]

# Modelos de solicitud
class SagaFilterParams(BaseModel):
    status: Optional[SagaStatusEnum] = None
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    task_id: Optional[str] = None
    image_id: Optional[str] = None
    limit: int = 20
    offset: int = 0

class HealthCheckResponse(BaseModel):
    status: str
    version: str
    database_status: str
    pulsar_publisher_status: str
    pulsar_consumer_status: str