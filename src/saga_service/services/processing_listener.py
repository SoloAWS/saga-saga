import logging
import uuid
from typing import Dict, Any, Optional

from ..core.saga_coordinator import SagaCoordinator
from ..domain.enums import ServiceType, StepType, StepStatus
from .compensation_service import CompensationService

logger = logging.getLogger(__name__)

class ProcessingListener:
    """
    Servicio que escucha eventos del servicio de Processing
    y los registra en el Saga Log.
    """
    
    def __init__(self, coordinator: SagaCoordinator, compensation_service: CompensationService):
        """
        Inicializa el listener con un coordinador de saga y servicio de compensación.
        
        Args:
            coordinator: Coordinador de saga para registrar eventos
            compensation_service: Servicio de compensación para manejar fallos
        """
        self.coordinator = coordinator
        self.compensation_service = compensation_service
    
    async def handle_processing_started(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de inicio de procesamiento.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling ProcessingStarted event: {event_data}")
        
        task_id = event_data.get("task_id")
        if not task_id:
            logger.error("Missing task_id in ProcessingStarted event")
            return
        
        # Extraer información adicional
        metadata = {
            "task_id": task_id,
            "image_type": event_data.get("image_type"),
            "region": event_data.get("region")
        }
        
        # Buscar una saga existente por los metadatos
        saga = await self._find_saga_by_metadata(metadata)
        
        if not saga:
            logger.warning(f"No saga found for metadata {metadata}")
            return
        
        # Registrar el paso de inicio de procesamiento
        step_data = {
            "step_type": StepType.PROCESSING_REQUEST,
            "service": ServiceType.PROCESSING,
            "status": StepStatus.STARTED,
            "event_id": event_data.get("id"),
            "event_type": "ProcessingStarted",
            "entity_id": task_id,
            "metadata": metadata
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added PROCESSING_REQUEST step to saga {saga.id}")
    
    async def handle_processing_completed(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de finalización de procesamiento.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling ProcessingCompleted event: {event_data}")
        
        task_id = event_data.get("task_id")
        if not task_id:
            logger.error("Missing task_id in ProcessingCompleted event")
            return
        
        # Extraer información adicional
        metadata = {
            "task_id": task_id,
            "status": event_data.get("status"),
            "message": event_data.get("message"),
            "region": event_data.get("region")
        }
        
        # Buscar una saga existente
        saga = await self._find_saga_by_task_id(task_id)
        
        if not saga:
            logger.warning(f"No saga found for task_id {task_id}")
            return
        
        # Registrar el paso de finalización de procesamiento
        step_data = {
            "step_type": StepType.PROCESSING_COMPLETE,
            "service": ServiceType.PROCESSING,
            "status": StepStatus.COMPLETED,
            "event_id": event_data.get("id"),
            "event_type": "ProcessingCompleted",
            "entity_id": task_id,
            "metadata": metadata
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added PROCESSING_COMPLETE step to saga {saga.id}")
        
        # Marcar la saga como completada
        await self.coordinator.mark_saga_completed(saga.id)
        logger.info(f"Marked saga {saga.id} as completed")
    
    async def handle_processing_failed(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de fallo de procesamiento.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling ProcessingFailed event: {event_data}")
        
        task_id = event_data.get("task_id")
        if not task_id:
            logger.error("Missing task_id in ProcessingFailed event")
            return
        
        # Extraer información adicional
        error_message = event_data.get("error_message", "Unknown error")
        metadata = {
            "task_id": task_id,
            "error_message": error_message,
            "region": event_data.get("region")
        }
        
        # Buscar una saga existente
        saga = await self._find_saga_by_task_id(task_id)
        
        if not saga:
            logger.warning(f"No saga found for task_id {task_id}")
            return
        
        # Registrar el paso de fallo de procesamiento
        step_data = {
            "step_type": StepType.PROCESSING_COMPLETE,
            "service": ServiceType.PROCESSING,
            "status": StepStatus.FAILED,
            "event_id": event_data.get("id"),
            "event_type": "ProcessingFailed",
            "entity_id": task_id,
            "error_message": error_message,
            "metadata": metadata
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added failed PROCESSING_COMPLETE step to saga {saga.id}")
        
        # Iniciar proceso de compensación
        await self.compensation_service.handle_processing_failure(event_data)
    
    async def _find_saga_by_task_id(self, task_id: str) -> Optional[Any]:
        """
        Busca una saga que tenga algún paso relacionado con el task_id dado.
        
        Args:
            task_id: ID de la tarea
            
        Returns:
            SagaLog: La saga encontrada o None
        """
        # Obtener todas las sagas
        all_sagas = await self.coordinator.get_all_sagas(limit=100)
        
        for saga in all_sagas:
            # Buscar en los pasos por el entity_id que coincida con task_id
            for step in saga.steps:
                if step.entity_id == task_id:
                    return saga
                
                # También buscar en los metadatos
                if step._metadata and step._metadata.get("task_id") == task_id:
                    return saga
        
        return None
    
    async def _find_saga_by_metadata(self, metadata: Dict[str, Any]) -> Optional[Any]:
        """
        Busca una saga que tenga algún paso relacionado con los metadatos dados.
        
        Args:
            metadata: Diccionario de metadatos
            
        Returns:
            SagaLog: La saga encontrada o None
        """
        # Si el metadata tiene task_id, usamos el método existente
        if "task_id" in metadata:
            return await self._find_saga_by_task_id(metadata["task_id"])
        
        # Si no, hacemos una búsqueda más exhaustiva por los campos disponibles
        all_sagas = await self.coordinator.get_all_sagas(limit=100)
        
        for saga in all_sagas:
            for step in saga.steps:
                # Verificar si los metadatos coinciden parcialmente
                if step._metadata:
                    matches = []
                    for key, value in metadata.items():
                        if key in step._metadata and step._metadata[key] == value:
                            matches.append(True)
                    
                    # Si todos los metadatos coinciden
                    if matches and len(matches) == len(metadata):
                        return saga
        
        return None