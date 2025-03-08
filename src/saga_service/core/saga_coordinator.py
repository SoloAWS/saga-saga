import logging
import uuid
from typing import Optional, Dict, Any, List
from datetime import datetime

from ..domain.entities import SagaLog, SagaStep
from ..domain.enums import SagaStatus, StepStatus, ServiceType, StepType
from ..infrastructure.repositories.saga_log_repository import SagaLogRepository
from ..config.database import create_session, get_db

logger = logging.getLogger(__name__)

class SagaCoordinator:
    """
    Coordinador principal de sagas. Se encarga de la gestión del ciclo de vida
    de una saga, incluyendo la creación, seguimiento y compensación cuando sea necesario.
    """
    
    def __init__(self):
        pass
    
    async def create_saga(self, correlation_id: Optional[str] = None) -> SagaLog:
        """
        Crea una nueva saga y la persiste en la base de datos.
        
        Args:
            correlation_id: ID de correlación para seguimiento (opcional)
            
        Returns:
            SagaLog: La saga creada
        """
        # Generar ID de correlación si no se proporcionó
        if not correlation_id:
            correlation_id = str(uuid.uuid4())
        
        # Crear la saga
        saga = SagaLog(
            correlation_id=correlation_id,
            saga_type="IMAGE_PROCESSING",
            status=SagaStatus.STARTED,
            start_time=datetime.now()
        )
        
        # Persistir en la base de datos
        async with get_db() as session:
            repository = SagaLogRepository(session)
            return await repository.create(saga)
    
    async def add_step(self, saga_id: uuid.UUID, step_data: Dict[str, Any]) -> SagaStep:
        """
        Añade un paso a una saga existente.
        
        Args:
            saga_id: ID de la saga
            step_data: Datos del paso a añadir
            
        Returns:
            SagaStep: El paso añadido
        """
        # Crear paso
        step = SagaStep(
            saga_id=saga_id,
            step_type=step_data.get("step_type"),
            service=step_data.get("service"),
            status=step_data.get("status", StepStatus.STARTED),
            event_id=step_data.get("event_id"),
            event_type=step_data.get("event_type"),
            entity_id=step_data.get("entity_id"),
            metadata=step_data.get("metadata", {}),
            error_message=step_data.get("error_message"),
            timestamp=datetime.now()
        )
        
        # Persistir en la base de datos
        async with get_db() as session:
            repository = SagaLogRepository(session)
            return await repository.add_step(saga_id, step)
    
    async def update_step_status(self, step_id: uuid.UUID, status: StepStatus, error_message: Optional[str] = None) -> SagaStep:
        """
        Actualiza el estado de un paso existente.
        
        Args:
            step_id: ID del paso
            status: Nuevo estado
            error_message: Mensaje de error opcional
            
        Returns:
            SagaStep: El paso actualizado
        """
        async with get_db() as session:
            repository = SagaLogRepository(session)
            
            # Obtener el saga log que contiene el paso
            from sqlalchemy import select
            from ..infrastructure.repositories.dto import SagaStepDTO
            
            stmt = select(SagaStepDTO).where(SagaStepDTO.id == step_id)
            result = await session.execute(stmt)
            step_dto = result.scalars().first()
            
            if not step_dto:
                raise ValueError(f"No se encontró SagaStep con ID {step_id}")
            
            # Obtener el SagaLog completo
            saga = await repository.get_by_id(step_dto.saga_id)
            
            # Encontrar y actualizar el paso
            step_to_update = None
            for step in saga.steps:
                if step.id == step_id:
                    step_to_update = step
                    break
            
            if not step_to_update:
                raise ValueError(f"No se encontró SagaStep con ID {step_id} en SagaLog {saga.id}")
            
            # Actualizar estado y mensaje de error
            step_to_update.status = status
            if error_message:
                step_to_update.error_message = error_message
            
            # Si es un estado de compensación, registrar timestamp
            if status in [StepStatus.COMPENSATING, StepStatus.COMPENSATED, StepStatus.COMPENSATION_FAILED]:
                step_to_update.compensation_timestamp = datetime.now()
            
            # Actualizar en la base de datos
            return await repository.update_step(step_to_update)
    
    async def mark_saga_completed(self, saga_id: uuid.UUID) -> SagaLog:
        """
        Marca una saga como completada.
        
        Args:
            saga_id: ID de la saga
            
        Returns:
            SagaLog: La saga actualizada
        """
        async with get_db() as session:
            repository = SagaLogRepository(session)
            saga = await repository.get_by_id(saga_id)
            
            if not saga:
                raise ValueError(f"No se encontró SagaLog con ID {saga_id}")
            
            saga.mark_completed()
            return await repository.update(saga)
    
    async def mark_saga_failed(self, saga_id: uuid.UUID, error_message: str) -> SagaLog:
        """
        Marca una saga como fallida.
        
        Args:
            saga_id: ID de la saga
            error_message: Mensaje de error
            
        Returns:
            SagaLog: La saga actualizada
        """
        async with get_db() as session:
            repository = SagaLogRepository(session)
            saga = await repository.get_by_id(saga_id)
            
            if not saga:
                raise ValueError(f"No se encontró SagaLog con ID {saga_id}")
            
            saga.mark_failed(error_message)
            return await repository.update(saga)
    
    async def start_compensation(self, saga_id: uuid.UUID) -> SagaLog:
        """
        Inicia el proceso de compensación para una saga.
        
        Args:
            saga_id: ID de la saga
            
        Returns:
            SagaLog: La saga actualizada
        """
        async with get_db() as session:
            repository = SagaLogRepository(session)
            saga = await repository.get_by_id(saga_id)
            
            if not saga:
                raise ValueError(f"No se encontró SagaLog con ID {saga_id}")
            
            saga.start_compensation()
            return await repository.update(saga)
    
    async def mark_compensation_succeeded(self, saga_id: uuid.UUID) -> SagaLog:
        """
        Marca la compensación de una saga como exitosa.
        
        Args:
            saga_id: ID de la saga
            
        Returns:
            SagaLog: La saga actualizada
        """
        async with get_db() as session:
            repository = SagaLogRepository(session)
            saga = await repository.get_by_id(saga_id)
            
            if not saga:
                raise ValueError(f"No se encontró SagaLog con ID {saga_id}")
            
            saga.mark_compensation_succeeded()
            return await repository.update(saga)
    
    async def mark_compensation_failed(self, saga_id: uuid.UUID, error_message: str) -> SagaLog:
        """
        Marca la compensación de una saga como fallida.
        
        Args:
            saga_id: ID de la saga
            error_message: Mensaje de error
            
        Returns:
            SagaLog: La saga actualizada
        """
        async with get_db() as session:
            repository = SagaLogRepository(session)
            saga = await repository.get_by_id(saga_id)
            
            if not saga:
                raise ValueError(f"No se encontró SagaLog con ID {saga_id}")
            
            saga.mark_compensation_failed(error_message)
            return await repository.update(saga)
    
    async def get_saga_by_id(self, saga_id: uuid.UUID) -> Optional[SagaLog]:
        """
        Obtiene una saga por su ID.
        
        Args:
            saga_id: ID de la saga
            
        Returns:
            SagaLog: La saga encontrada o None
        """
        async with get_db() as session:
            repository = SagaLogRepository(session)
            return await repository.get_by_id(saga_id)
    
    async def get_saga_by_correlation_id(self, correlation_id: str) -> Optional[SagaLog]:
        """
        Obtiene una saga por su ID de correlación.
        
        Args:
            correlation_id: ID de correlación
            
        Returns:
            SagaLog: La saga encontrada o None
        """
        async with get_db() as session:
            repository = SagaLogRepository(session)
            return await repository.get_by_correlation_id(correlation_id)
    
    async def get_all_sagas(self, limit: int = 100, offset: int = 0) -> List[SagaLog]:
        """
        Obtiene todas las sagas paginadas.
        
        Args:
            limit: Límite de resultados
            offset: Desplazamiento
            
        Returns:
            List[SagaLog]: Lista de sagas
        """
        async with get_db() as session:
            repository = SagaLogRepository(session)
            return await repository.get_all(limit, offset)
    
    async def get_sagas_by_status(self, status: SagaStatus, limit: int = 100) -> List[SagaLog]:
        """
        Obtiene sagas por estado.
        
        Args:
            status: Estado de las sagas
            limit: Límite de resultados
            
        Returns:
            List[SagaLog]: Lista de sagas
        """
        async with get_db() as session:
            repository = SagaLogRepository(session)
            return await repository.get_by_status(status, limit)