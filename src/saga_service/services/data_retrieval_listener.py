import logging
import uuid
from typing import Dict, Any, Optional

from ..core.saga_coordinator import SagaCoordinator
from ..domain.enums import ServiceType, StepType, StepStatus

logger = logging.getLogger(__name__)

class DataRetrievalListener:
    """
    Servicio que escucha eventos del servicio de Data Retrieval
    y los registra en el Saga Log.
    """
    
    def __init__(self, coordinator: SagaCoordinator):
        """
        Inicializa el listener con un coordinador de saga.
        
        Args:
            coordinator: Coordinador de saga para registrar eventos
        """
        self.coordinator = coordinator
    
    async def handle_retrieval_started(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de inicio de recuperación de datos.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling RetrievalStarted event: {event_data}")
        
        task_id = event_data.get("task_id")
        if not task_id:
            logger.error("Missing task_id in RetrievalStarted event")
            return
        
        # Buscar una saga existente por correlation_id o crear una nueva
        correlation_id = event_data.get("correlation_id")
        saga = None
        
        if correlation_id:
            saga = await self.coordinator.get_saga_by_correlation_id(correlation_id)
        
        if not saga:
            # Crear una nueva saga
            saga = await self.coordinator.create_saga(correlation_id)
            logger.info(f"Created new saga {saga.id} for task {task_id}")
        
        # Registrar el paso de inicio de recuperación
        step_data = {
            "step_type": StepType.DATA_RETRIEVAL_CREATE,
            "service": ServiceType.DATA_RETRIEVAL,
            "status": StepStatus.STARTED,
            "event_id": event_data.get("id"),
            "event_type": "RetrievalStarted",
            "entity_id": task_id,
            "metadata": event_data
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added DATA_RETRIEVAL_CREATE step to saga {saga.id}")
    
    async def handle_retrieval_completed(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de finalización de recuperación de datos.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling RetrievalCompleted event: {event_data}")
        
        task_id = event_data.get("task_id")
        if not task_id:
            logger.error("Missing task_id in RetrievalCompleted event")
            return
        
        # Buscar la saga relacionada con este task_id
        saga = await self._find_saga_by_task_id(task_id)
        
        if not saga:
            logger.error(f"No saga found for task_id {task_id}")
            return
        
        # Registrar el paso de finalización de recuperación
        step_data = {
            "step_type": StepType.DATA_RETRIEVAL_START,
            "service": ServiceType.DATA_RETRIEVAL,
            "status": StepStatus.COMPLETED,
            "event_id": event_data.get("id"),
            "event_type": "RetrievalCompleted",
            "entity_id": task_id,
            "metadata": event_data
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added DATA_RETRIEVAL_START step to saga {saga.id}")
    
    async def handle_retrieval_failed(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de fallo de recuperación de datos.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling RetrievalFailed event: {event_data}")
        
        task_id = event_data.get("task_id")
        if not task_id:
            logger.error("Missing task_id in RetrievalFailed event")
            return
        
        # Buscar la saga relacionada con este task_id
        saga = await self._find_saga_by_task_id(task_id)
        
        if not saga:
            logger.error(f"No saga found for task_id {task_id}")
            return
        
        # Registrar el paso de fallo de recuperación
        step_data = {
            "step_type": StepType.DATA_RETRIEVAL_START,
            "service": ServiceType.DATA_RETRIEVAL,
            "status": StepStatus.FAILED,
            "event_id": event_data.get("id"),
            "event_type": "RetrievalFailed",
            "entity_id": task_id,
            "error_message": event_data.get("error_message"),
            "metadata": event_data
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added failed DATA_RETRIEVAL_START step to saga {saga.id}")
        
        # Marcar la saga como fallida
        await self.coordinator.mark_saga_failed(
            saga.id, 
            f"Data retrieval failed: {event_data.get('error_message', 'Unknown error')}"
        )
        logger.info(f"Marked saga {saga.id} as failed")
    
    async def handle_image_uploaded(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de imágenes recuperadas (ImagesRetrieved).
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling ImagesRetrieved event: {event_data}")
        
        task_id = event_data.get("task_id")
        if not task_id:
            logger.error("Missing task_id in ImagesRetrieved event")
            return
        
        # Buscar la saga relacionada con este task_id
        saga = await self._find_saga_by_task_id(task_id)
        
        if not saga:
            logger.error(f"No saga found for task_id {task_id}")
            return
        
        # Para cada imagen, registrar un paso de carga
        image_ids = event_data.get("image_ids", [])
        if not image_ids:
            logger.warning(f"No image_ids in ImagesRetrieved event for task {task_id}")
            return
        
        for image_id in image_ids:
            # Registrar el paso de carga de imagen
            step_data = {
                "step_type": StepType.DATA_RETRIEVAL_UPLOAD,
                "service": ServiceType.DATA_RETRIEVAL,
                "status": StepStatus.COMPLETED,
                "event_id": event_data.get("id"),
                "event_type": "ImagesRetrieved",
                "entity_id": task_id,
                "metadata": {
                    "image_id": image_id,
                    "source": event_data.get("source"),
                    "batch_id": event_data.get("batch_id")
                }
            }
            
            await self.coordinator.add_step(saga.id, step_data)
            logger.info(f"Added DATA_RETRIEVAL_UPLOAD step for image {image_id} to saga {saga.id}")
    
    async def handle_image_upload_failed(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de fallo de carga de imagen.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling ImageUploadFailed event: {event_data}")
        
        task_id = event_data.get("task_id")
        if not task_id:
            logger.error("Missing task_id in ImageUploadFailed event")
            return
        
        # Buscar la saga relacionada con este task_id
        saga = await self._find_saga_by_task_id(task_id)
        
        if not saga:
            logger.error(f"No saga found for task_id {task_id}")
            return
        
        # Registrar el paso de fallo de carga de imagen
        step_data = {
            "step_type": StepType.DATA_RETRIEVAL_UPLOAD,
            "service": ServiceType.DATA_RETRIEVAL,
            "status": StepStatus.FAILED,
            "event_id": event_data.get("id"),
            "event_type": "ImageUploadFailed",
            "entity_id": task_id,
            "error_message": event_data.get("error_message"),
            "metadata": {
                "filename": event_data.get("filename"),
                "format": event_data.get("format"),
                "modality": event_data.get("modality"),
                "region": event_data.get("region"),
                "source": event_data.get("source")
            }
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added failed DATA_RETRIEVAL_UPLOAD step to saga {saga.id}")
        
        # No marcamos toda la saga como fallida, ya que podría ser solo una imagen 
        # de muchas y el proceso general podría continuar
    
    async def handle_image_ready_for_anonymization(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de imagen lista para anonimización.
        
        Args:
            event_data: Datos del evento
        """
        logger.info(f"Handling ImageReadyForAnonymization event: {event_data}")
        
        task_id = event_data.get("task_id")
        image_id = event_data.get("image_id")
        if not task_id or not image_id:
            logger.error("Missing task_id or image_id in ImageReadyForAnonymization event")
            return
        
        # Buscar la saga relacionada con este task_id
        saga = await self._find_saga_by_task_id(task_id)
        
        if not saga:
            logger.error(f"No saga found for task_id {task_id}")
            return
        
        # Registrar el paso de imagen lista para anonimización
        step_data = {
            "step_type": StepType.ANONYMIZATION_REQUEST,
            "service": ServiceType.DATA_RETRIEVAL,  # Origen del evento
            "status": StepStatus.COMPLETED,
            "event_id": event_data.get("id"),
            "event_type": "ImageReadyForAnonymization",
            "entity_id": image_id,  # Aquí usamos image_id porque es específico para esta imagen
            "metadata": {
                "task_id": task_id,
                "image_id": image_id,
                "source": event_data.get("source"),
                "modality": event_data.get("modality"),
                "region": event_data.get("region"),
                "file_path": event_data.get("file_path")
            }
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        logger.info(f"Added ANONYMIZATION_REQUEST step for image {image_id} to saga {saga.id}")
    
    async def _find_saga_by_task_id(self, task_id: str) -> Optional[Any]:
        """
        Busca una saga que tenga algún paso relacionado con el task_id dado.
        Método auxiliar para evitar duplicación de código.
        
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
                if step.metadata and step.metadata.get("task_id") == task_id:
                    return saga
        
        return None