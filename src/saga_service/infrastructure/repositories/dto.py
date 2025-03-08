from sqlalchemy import Column, String, JSON, DateTime, Boolean, ForeignKey, Table
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime

from ...config.database import Base

class SagaLogDTO(Base):
    """DTO para la entidad SagaLog"""
    __tablename__ = "saga_logs"

    id = Column(UUID(as_uuid=True), primary_key=True)
    correlation_id = Column(String, nullable=True, index=True)
    saga_type = Column(String, nullable=False)
    status = Column(String, nullable=False)
    start_time = Column(DateTime, default=datetime.now)
    end_time = Column(DateTime, nullable=True)
    error_message = Column(String, nullable=True)
    _metadata = Column(JSON, nullable=True)
    
    # Relación con los pasos
    steps = relationship("SagaStepDTO", back_populates="saga", cascade="all, delete-orphan")

class SagaStepDTO(Base):
    """DTO para la entidad SagaStep"""
    __tablename__ = "saga_steps"

    id = Column(UUID(as_uuid=True), primary_key=True)
    saga_id = Column(UUID(as_uuid=True), ForeignKey("saga_logs.id"), nullable=False)
    step_type = Column(String, nullable=False)
    service = Column(String, nullable=False)
    status = Column(String, nullable=False)
    event_id = Column(String, nullable=True)
    event_type = Column(String, nullable=True)
    entity_id = Column(String, nullable=True)
    _metadata = Column(JSON, nullable=True)
    error_message = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.now)
    compensation_timestamp = Column(DateTime, nullable=True)
    
    # Relación con la saga
    saga = relationship("SagaLogDTO", back_populates="steps")