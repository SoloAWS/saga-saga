import logging
import uuid
from typing import Dict, Any, Optional

from ..core.saga_coordinator import SagaCoordinator
from ..domain.enums import ServiceType, StepType, StepStatus

logger = logging.getLogger(__name__)

class AnonymizationListener:
    """
    Servicio que escucha eventos del servicio de Anonymization
    y los registra en el Saga Log.
    """
    
    def __init__(self, coordinator: SagaCoordinator):
        """
        Inicializa el listener con un coordinador de saga.
        
        Args:
            coordinator: Coordinador de saga para registrar eventos
        """
        self.coordinator = coordinator
    
    async def handle_anonymization_requested(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de solicitud de anonimización.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling AnonymizationRequested event: {event_data}")
        
        image_id = event_data.get("image_id")
        task_id = event_data.get("task_id")
        if not image_id or not task_id:
            logger.error("Missing image_id or task_id in AnonymizationRequested event")
            return
        
        # Buscar una saga existente por task_id
        saga = await self._find_saga_by_task_or_image_id(task_id, image_id)
        
        if not saga:
            logger.warning(f"No saga found for task_id {task_id} or image_id {image_id}")
            return
        
        # Registrar el paso de solicitud de anonimización
        step_data = {
            "step_type": StepType.ANONYMIZATION_REQUEST,
            "service": ServiceType.ANONYMIZATION,
            "status": StepStatus.STARTED,
            "event_id": event_data.get("id"),
            "event_type": "AnonymizationRequested",
            "entity_id": image_id,
            "metadata": {
                "task_id": task_id,
                "image_id": image_id,
                "image_type": event_data.get("image_type"),
                "source": event_data.get("source"),
                "modality": event_data.get("modality"),
                "region": event_data.get("region"),
                "file_path": event_data.get("file_path"),
                "destination_service": event_data.get("destination_service")
            }
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added ANONYMIZATION_REQUEST started step to saga {saga.id}")
    
    async def handle_anonymization_completed(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de finalización de anonimización.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling AnonymizationCompleted event: {event_data}")
        
        image_id = event_data.get("image_id")
        task_id = event_data.get("task_id")
        if not image_id or not task_id:
            logger.error("Missing image_id or task_id in AnonymizationCompleted event")
            return
        
        # Buscar una saga existente por task_id o image_id
        saga = await self._find_saga_by_task_or_image_id(task_id, image_id)
        
        if not saga:
            logger.warning(f"No saga found for task_id {task_id} or image_id {image_id}")
            return
        
        # Registrar el paso de finalización de anonimización
        step_data = {
            "step_type": StepType.ANONYMIZATION_COMPLETE,
            "service": ServiceType.ANONYMIZATION,
            "status": StepStatus.COMPLETED,
            "event_id": event_data.get("id"),
            "event_type": "AnonymizationCompleted",
            "entity_id": image_id,
            "metadata": {
                "task_id": task_id,
                "image_id": image_id,
                "image_type": event_data.get("image_type"),
                "result_file_path": event_data.get("result_file_path"),
                "processing_time_ms": event_data.get("processing_time_ms")
            }
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added ANONYMIZATION_COMPLETE step to saga {saga.id}")
    
    async def handle_anonymization_failed(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de fallo de anonimización.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling AnonymizationFailed event: {event_data}")
        
        image_id = event_data.get("image_id")
        task_id = event_data.get("task_id")
        if not image_id or not task_id:
            logger.error("Missing image_id or task_id in AnonymizationFailed event")
            return
        
        # Buscar una saga existente por task_id o image_id
        saga = await self._find_saga_by_task_or_image_id(task_id, image_id)
        
        if not saga:
            logger.warning(f"No saga found for task_id {task_id} or image_id {image_id}")
            return
        
        # Registrar el paso de fallo de anonimización
        step_data = {
            "step_type": StepType.ANONYMIZATION_COMPLETE,
            "service": ServiceType.ANONYMIZATION,
            "status": StepStatus.FAILED,
            "event_id": event_data.get("id"),
            "event_type": "AnonymizationFailed",
            "entity_id": image_id,
            "error_message": event_data.get("error_message"),
            "metadata": {
                "task_id": task_id,
                "image_id": image_id,
                "image_type": event_data.get("image_type")
            }
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added failed ANONYMIZATION_COMPLETE step to saga {saga.id}")
        
        # No marcamos toda la saga como fallida, ya que podría ser solo una imagen
        # de muchas y el proceso general podría continuar
    
    async def handle_image_ready_for_processing(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de imagen lista para procesamiento.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling ImageReadyForProcessing event: {event_data}")
        
        image_id = event_data.get("image_id")
        task_id = event_data.get("task_id")
        if not image_id or not task_id:
            logger.error("Missing image_id or task_id in ImageReadyForProcessing event")
            return
        
        # Buscar una saga existente por task_id o image_id
        saga = await self._find_saga_by_task_or_image_id(task_id, image_id)
        
        if not saga:
            logger.warning(f"No saga found for task_id {task_id} or image_id {image_id}")
            return
        
        # Registrar el paso de imagen lista para procesamiento
        step_data = {
            "step_type": StepType.PROCESSING_REQUEST,
            "service": ServiceType.ANONYMIZATION,  # Origen del evento
            "status": StepStatus.STARTED,
            "event_id": event_data.get("id"),
            "event_type": "ImageReadyForProcessing",
            "entity_id": image_id,
            "metadata": {
                "task_id": task_id,
                "image_id": image_id,
                "image_type": event_data.get("image_type"),
                "anonymized_file_path": event_data.get("anonymized_file_path"),
                "original_file_path": event_data.get("original_file_path"),
                "source": event_data.get("source"),
                "modality": event_data.get("modality"),
                "region": event_data.get("region")
            }
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added PROCESSING_REQUEST step for image {image_id} to saga {saga.id}")
    
    async def handle_anonymization_rolled_back(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de rollback de anonimización.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling AnonymizationRolledBack event: {event_data}")
        
        task_id = event_data.get("task_id")
        image_id = event_data.get("image_id")
        if not task_id:
            logger.error("Missing task_id in AnonymizationRolledBack event")
            return
        
        # Buscar una saga existente por task_id o image_id
        saga = await self._find_saga_by_task_or_image_id(task_id, image_id)
        
        if not saga:
            logger.warning(f"No saga found for task_id {task_id} or image_id {image_id}")
            return
        
        # Buscar pasos previos de anonimización para esta imagen
        entity_id = image_id if image_id else task_id
        
        # Registrar el paso de rollback de anonimización
        step_data = {
            "step_type": StepType.ANONYMIZATION_COMPLETE,
            "service": ServiceType.ANONYMIZATION,
            "status": StepStatus.COMPENSATED,
            "event_id": event_data.get("id"),
            "event_type": "AnonymizationRolledBack",
            "entity_id": entity_id,
            "metadata": {
                "task_id": task_id,
                "image_id": image_id,
                "reason": event_data.get("reason")
            }
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added compensated ANONYMIZATION_COMPLETE step to saga {saga.id}")
    
    async def _find_saga_by_task_or_image_id(self, task_id: str, image_id: str = None) -> Optional[Any]:
        """
        Busca una saga que tenga algún paso relacionado con el task_id o image_id dado.
        Método auxiliar para evitar duplicación de código.
        
        Args:
            task_id: ID de la tarea
            image_id: ID de la imagen (opcional)
            
        Returns:
            SagaLog: La saga encontrada o None
        """
        # Obtener todas las sagas
        all_sagas = await self.coordinator.get_all_sagas(limit=100)
        
        for saga in all_sagas:
            # Buscar por task_id
            for step in saga.steps:
                if step.entity_id == task_id:
                    return saga
                
                # También buscar en los metadatos
                if step.metadata and step.metadata.get("task_id") == task_id:
                    return saga
            
            # Si tenemos image_id, buscar también por él
            if image_id:
                for step in saga.steps:
                    if step.entity_id == image_id:
                        return saga
                    
                    # También buscar en los metadatos
                    if step.metadata and step.metadata.get("image_id") == image_id:
                        return saga
        
        return None