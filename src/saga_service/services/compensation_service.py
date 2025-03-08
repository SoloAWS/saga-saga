import logging
import uuid
from typing import Dict, Any, List, Optional

from ..domain.entities import SagaLog, SagaStep
from ..domain.enums import SagaStatus, StepStatus, ServiceType, StepType
from ..core.saga_coordinator import SagaCoordinator
from ..infrastructure.messaging.pulsar_publisher import PulsarPublisher
from ..config.settings import get_settings

logger = logging.getLogger(__name__)

class CompensationService:
    """
    Servicio encargado de coordinar las compensaciones cuando se produce un fallo
    en algún paso de la saga.
    """
    
    def __init__(self, publisher: PulsarPublisher, coordinator: SagaCoordinator):
        self.publisher = publisher
        self.coordinator = coordinator
        self.settings = get_settings()
    
    async def handle_processing_failure(self, event_data: Dict[str, Any]) -> None:
        """
        Maneja un evento de fallo en el procesamiento de una imagen.
        
        Args:
            event_data: Datos del evento de fallo
        """
        logger.info(f"Handling processing failure event: {event_data}")
        
        task_id = event_data.get("task_id")
        error_message = event_data.get("error_message", "Unknown error")
        
        if not task_id:
            logger.error("Missing task_id in processing failure event")
            return
        
        # Buscar la saga relacionada con este task_id
        # En este escenario, puede ser que tengamos múltiples sagas que estén asociadas al task_id
        # La asunción aquí es que buscamos en los metadatos de los pasos para encontrar la saga correcta
        saga = await self._find_saga_by_task_id(task_id)
        
        if not saga:
            logger.error(f"No saga found for task_id {task_id}")
            return
        
        # Iniciar compensación
        logger.info(f"Starting compensation for saga {saga.id}")
        await self.coordinator.start_compensation(saga.id)
        
        # Registrar paso de fallo de procesamiento
        step_data = {
            "step_type": StepType.PROCESSING_REQUEST,
            "service": ServiceType.PROCESSING,
            "status": StepStatus.FAILED,
            "event_type": event_data.get("type", "ProcessingFailed"),
            "entity_id": task_id,
            "error_message": error_message,
            "metadata": event_data
        }
        
        await self.coordinator.add_step(saga.id, step_data)
        
        # Ejecutar compensaciones en orden inverso
        try:
            # Ejecutamos compensaciones en orden inverso: primero anonymization, luego data-retrieval
            await self._compensate_anonymization(saga, task_id, error_message)
            await self._compensate_data_retrieval(saga, task_id, error_message)
            
            # Marcar como compensación exitosa
            await self.coordinator.mark_compensation_succeeded(saga.id)
            logger.info(f"Compensation for saga {saga.id} completed successfully")
            
        except Exception as e:
            # Marcar como compensación fallida
            logger.error(f"Compensation for saga {saga.id} failed: {str(e)}")
            await self.coordinator.mark_compensation_failed(saga.id, str(e))
    
    async def _find_saga_by_task_id(self, task_id: str) -> Optional[SagaLog]:
        """
        Busca una saga que tenga algún paso relacionado con el task_id dado.
        
        Args:
            task_id: ID de la tarea
            
        Returns:
            SagaLog: La saga encontrada o None
        """
        # Obtener todas las sagas no completadas
        sagas = await self.coordinator.get_sagas_by_status(SagaStatus.STARTED, 100)
        sagas.extend(await self.coordinator.get_sagas_by_status(SagaStatus.FAILED, 100))
        
        for saga in sagas:
            # Buscar en los pasos por el entity_id que coincida con task_id
            for step in saga.steps:
                if step.entity_id == task_id:
                    return saga
                
                # También buscar en los metadatos
                if step.metadata and step.metadata.get("task_id") == task_id:
                    return saga
        
        return None
    
    async def _compensate_anonymization(self, saga: SagaLog, task_id: str, reason: str) -> None:
        """
        Ejecuta la compensación del servicio de anonimización.
        
        Args:
            saga: Saga a compensar
            task_id: ID de la tarea
            reason: Razón de la compensación
        """
        # Buscar pasos relacionados con anonimización
        anonymization_steps = [
            step for step in saga.steps 
            if step.service == ServiceType.ANONYMIZATION and 
            step.status in [StepStatus.COMPLETED, StepStatus.STARTED]
        ]
        
        if not anonymization_steps:
            logger.info(f"No anonymization steps found for saga {saga.id}")
            return
        
        logger.info(f"Compensating anonymization steps for saga {saga.id}")
        
        # Por cada paso de anonimización, enviar un comando de compensación
        for step in anonymization_steps:
            try:
                # Registrar inicio de compensación
                await self.coordinator.update_step_status(step.id, StepStatus.COMPENSATING)
                
                # Preparar comando de compensación
                # En este caso, usamos el comando RollbackAnonymization definido en anonymization_service
                command_data = {
                    "task_id": step.entity_id or task_id,
                    "reason": f"Compensación de saga {saga.id}: {reason}"
                }
                
                # Publicar comando
                topic = self.settings.command_topics.get("rollback_anonymization")
                if not topic:
                    raise ValueError("No topic configured for rollback_anonymization command")
                
                await self.publisher.publish_command("RollbackAnonymization", command_data, topic)
                
                # Por ahora asumimos que la compensación fue exitosa
                # En un sistema real, debería haber un mecanismo para confirmar el éxito
                await self.coordinator.update_step_status(step.id, StepStatus.COMPENSATED)
                logger.info(f"Anonymization step {step.id} compensated successfully")
                
            except Exception as e:
                logger.error(f"Error compensating anonymization step {step.id}: {str(e)}")
                await self.coordinator.update_step_status(
                    step.id, StepStatus.COMPENSATION_FAILED, str(e)
                )
                raise
    
    async def _compensate_data_retrieval(self, saga: SagaLog, task_id: str, reason: str) -> None:
        """
        Ejecuta la compensación del servicio de recuperación de datos.
        
        Args:
            saga: Saga a compensar
            task_id: ID de la tarea
            reason: Razón de la compensación
        """
        # Buscar pasos relacionados con data retrieval y las imágenes
        data_retrieval_steps = [
            step for step in saga.steps 
            if step.service == ServiceType.DATA_RETRIEVAL and 
            step.status in [StepStatus.COMPLETED, StepStatus.STARTED]
        ]
        
        if not data_retrieval_steps:
            logger.info(f"No data retrieval steps found for saga {saga.id}")
            return
        
        logger.info(f"Compensating data retrieval steps for saga {saga.id}")
        
        # En este caso, necesitamos identificar las imágenes que deben eliminarse
        # Esto se hace buscando en los metadatos de cada paso
        for step in data_retrieval_steps:
            try:
                # Registrar inicio de compensación
                await self.coordinator.update_step_status(step.id, StepStatus.COMPENSATING)
                
                # Si es un paso de carga de imagen, necesitamos eliminar esa imagen
                if step.step_type == StepType.DATA_RETRIEVAL_UPLOAD:
                    image_id = step.metadata.get("image_id")
                    if image_id:
                        # Preparar comando de compensación
                        command_data = {
                            "image_id": image_id,
                            "task_id": step.entity_id or task_id,
                            "reason": f"Compensación de saga {saga.id}: {reason}"
                        }
                        
                        # Publicar comando
                        topic = self.settings.command_topics.get("delete_retrieved_image")
                        if not topic:
                            raise ValueError("No topic configured for delete_retrieved_image command")
                        
                        await self.publisher.publish_command("DeleteRetrievedImage", command_data, topic)
                        
                        # Por ahora asumimos que la compensación fue exitosa
                        await self.coordinator.update_step_status(step.id, StepStatus.COMPENSATED)
                        logger.info(f"Data retrieval step {step.id} compensated successfully")
                    else:
                        logger.warning(f"No image_id found in metadata for step {step.id}")
                        await self.coordinator.update_step_status(step.id, StepStatus.COMPENSATED)
                else:
                    # No se necesita compensación especial para otros tipos de pasos de data retrieval
                    await self.coordinator.update_step_status(step.id, StepStatus.COMPENSATED)
                    
            except Exception as e:
                logger.error(f"Error compensating data retrieval step {step.id}: {str(e)}")
                await self.coordinator.update_step_status(
                    step.id, StepStatus.COMPENSATION_FAILED, str(e)
                )
                raise