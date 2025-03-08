import uuid
from typing import List, Optional, Dict, Any
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from ...domain.entities import SagaLog, SagaStep
from ...domain.enums import SagaStatus, StepStatus, ServiceType, StepType
from .dto import SagaLogDTO, SagaStepDTO

class SagaLogRepository:
    """Repositorio para la entidad SagaLog"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def create(self, saga_log: SagaLog) -> SagaLog:
        """Crea un nuevo registro de saga"""
        saga_dto = self._to_dto(saga_log)
        self.session.add(saga_dto)
        await self.session.flush()  # Para obtener el ID generado
        await self.session.commit()
        
        # Recuperar la entidad completa
        return await self.get_by_id(saga_log.id)
    
    async def update(self, saga_log: SagaLog) -> SagaLog:
        """Actualiza un registro de saga existente"""
        saga_dto = await self.session.get(SagaLogDTO, saga_log.id)
        
        if not saga_dto:
            raise ValueError(f"No se encontró SagaLog con ID {saga_log.id}")
        
        # Actualizar atributos
        saga_dto.correlation_id = saga_log.correlation_id
        saga_dto.saga_type = saga_log.saga_type
        saga_dto.status = saga_log.status.value
        saga_dto.start_time = saga_log.start_time
        saga_dto.end_time = saga_log.end_time
        saga_dto.error_message = saga_log.error_message
        saga_dto._metadata = saga_log._metadata
        
        # Gestionar pasos existentes y añadir nuevos
        existing_step_ids = {step.id for step in saga_dto.steps}
        
        for step in saga_log.steps:
            if step.id in existing_step_ids:
                # Actualizar paso existente
                step_dto = await self.session.get(SagaStepDTO, step.id)
                step_dto.step_type = step.step_type.value
                step_dto.service = step.service.value
                step_dto.status = step.status.value
                step_dto.event_id = step.event_id
                step_dto.event_type = step.event_type
                step_dto.entity_id = step.entity_id
                step_dto._metadata = step._metadata
                step_dto.error_message = step.error_message
                step_dto.timestamp = step.timestamp
                step_dto.compensation_timestamp = step.compensation_timestamp
            else:
                # Añadir nuevo paso
                step_dto = SagaStepDTO(
                    id=step.id,
                    saga_id=saga_log.id,
                    step_type=step.step_type.value,
                    service=step.service.value,
                    status=step.status.value,
                    event_id=step.event_id,
                    event_type=step.event_type,
                    entity_id=step.entity_id,
                    metadata=step._metadata,
                    error_message=step.error_message,
                    timestamp=step.timestamp,
                    compensation_timestamp=step.compensation_timestamp
                )
                self.session.add(step_dto)
        
        await self.session.commit()
        
        # Recuperar la entidad actualizada
        return await self.get_by_id(saga_log.id)
    
    async def get_by_id(self, saga_id: uuid.UUID) -> Optional[SagaLog]:
        """Obtiene un registro de saga por su ID"""
        saga_dto = await self.session.get(SagaLogDTO, saga_id)
        
        if not saga_dto:
            return None
        
        return self._to_entity(saga_dto)
    
    async def get_by_correlation_id(self, correlation_id: str) -> Optional[SagaLog]:
        """Obtiene un registro de saga por su ID de correlación"""
        stmt = select(SagaLogDTO).where(SagaLogDTO.correlation_id == correlation_id)
        result = await self.session.execute(stmt)
        saga_dto = result.scalars().first()
        
        if not saga_dto:
            return None
        
        return self._to_entity(saga_dto)
    
    async def get_all(self, limit: int = 100, offset: int = 0) -> List[SagaLog]:
        """Obtiene todos los registros de saga paginados"""
        stmt = select(SagaLogDTO).order_by(desc(SagaLogDTO.start_time)).limit(limit).offset(offset)
        result = await self.session.execute(stmt)
        saga_dtos = result.scalars().all()
        
        return [self._to_entity(dto) for dto in saga_dtos]
    
    async def get_by_status(self, status: SagaStatus, limit: int = 100) -> List[SagaLog]:
        """Obtiene registros de saga por estado"""
        stmt = select(SagaLogDTO).where(SagaLogDTO.status == status.value).order_by(desc(SagaLogDTO.start_time)).limit(limit)
        result = await self.session.execute(stmt)
        saga_dtos = result.scalars().all()
        
        return [self._to_entity(dto) for dto in saga_dtos]
    
    async def add_step(self, saga_id: uuid.UUID, step: SagaStep) -> SagaStep:
        """Añade un paso a un registro de saga existente"""
        # Verificar que la saga existe
        saga_dto = await self.session.get(SagaLogDTO, saga_id)
        
        if not saga_dto:
            raise ValueError(f"No se encontró SagaLog con ID {saga_id}")
        
        # Crear DTO del paso
        step.saga_id = saga_id
        step_dto = SagaStepDTO(
            id=step.id,
            saga_id=saga_id,
            step_type=step.step_type.value,
            service=step.service.value,
            status=step.status.value,
            event_id=step.event_id,
            event_type=step.event_type,
            entity_id=step.entity_id,
            metadata=step._metadata,
            error_message=step.error_message,
            timestamp=step.timestamp,
            compensation_timestamp=step.compensation_timestamp
        )
        
        self.session.add(step_dto)
        await self.session.commit()
        
        # Recuperar el paso guardado
        return self._step_to_entity(step_dto)
    
    async def update_step(self, step: SagaStep) -> SagaStep:
        """Actualiza un paso existente"""
        step_dto = await self.session.get(SagaStepDTO, step.id)
        
        if not step_dto:
            raise ValueError(f"No se encontró SagaStep con ID {step.id}")
        
        # Actualizar atributos
        step_dto.step_type = step.step_type.value
        step_dto.service = step.service.value
        step_dto.status = step.status.value
        step_dto.event_id = step.event_id
        step_dto.event_type = step.event_type
        step_dto.entity_id = step.entity_id
        step_dto._metadata = step._metadata
        step_dto.error_message = step.error_message
        step_dto.timestamp = step.timestamp
        step_dto.compensation_timestamp = step.compensation_timestamp
        
        await self.session.commit()
        
        # Recuperar el paso actualizado
        return self._step_to_entity(step_dto)
    
    def _to_dto(self, saga_log: SagaLog) -> SagaLogDTO:
        """Convierte una entidad SagaLog a DTO"""
        saga_dto = SagaLogDTO(
            id=saga_log.id,
            correlation_id=saga_log.correlation_id,
            saga_type=saga_log.saga_type,
            status=saga_log.status.value,
            start_time=saga_log.start_time,
            end_time=saga_log.end_time,
            error_message=saga_log.error_message,
            metadata=saga_log.metadata
        )
        
        # Convertir pasos
        for step in saga_log.steps:
            step_dto = SagaStepDTO(
                id=step.id,
                saga_id=saga_log.id,
                step_type=step.step_type.value,
                service=step.service.value,
                status=step.status.value,
                event_id=step.event_id,
                event_type=step.event_type,
                entity_id=step.entity_id,
                metadata=step._metadata,
                error_message=step.error_message,
                timestamp=step.timestamp,
                compensation_timestamp=step.compensation_timestamp
            )
            saga_dto.steps.append(step_dto)
        
        return saga_dto
    
    def _to_entity(self, saga_dto: SagaLogDTO) -> SagaLog:
        """Convierte un DTO a entidad SagaLog"""
        # Crear la saga
        saga = SagaLog(
            id=saga_dto.id,
            correlation_id=saga_dto.correlation_id,
            saga_type=saga_dto.saga_type,
            status=SagaStatus(saga_dto.status),
            start_time=saga_dto.start_time,
            end_time=saga_dto.end_time,
            error_message=saga_dto.error_message,
            metadata=saga_dto._metadata or {}
        )
        
        # Añadir los pasos
        for step_dto in saga_dto.steps:
            step = self._step_to_entity(step_dto)
            saga.steps.append(step)
        
        return saga
    
    def _step_to_entity(self, step_dto: SagaStepDTO) -> SagaStep:
        """Convierte un DTO a entidad SagaStep"""
        return SagaStep(
            id=step_dto.id,
            saga_id=step_dto.saga_id,
            step_type=StepType(step_dto.step_type),
            service=ServiceType(step_dto.service),
            status=StepStatus(step_dto.status),
            event_id=step_dto.event_id,
            event_type=step_dto.event_type,
            entity_id=step_dto.entity_id,
            metadata=step_dto._metadata or {},
            error_message=step_dto.error_message,
            timestamp=step_dto.timestamp,
            compensation_timestamp=step_dto.compensation_timestamp
        )