import uuid
import logging
from typing import Dict, Any
import asyncio

from ...config.settings import get_settings
from ..messaging.pulsar_publisher import PulsarPublisher

logger = logging.getLogger(__name__)

class DataRetrievalCommandsSender:
    """
    Clase encargada de enviar comandos de compensación al servicio de Data Retrieval.
    """
    
    def __init__(self, publisher: PulsarPublisher):
        """
        Inicializa el enviador de comandos.
        
        Args:
            publisher: Publicador de Pulsar para enviar comandos
        """
        self.publisher = publisher
        self.settings = get_settings()
    
    async def send_delete_retrieved_image(
        self,
        image_id: str,
        task_id: str,
        reason: str,
        correlation_id: str = None
    ) -> Dict[str, Any]:
        """
        Envía un comando para eliminar una imagen recuperada.
        
        Args:
            image_id: ID de la imagen a eliminar
            task_id: ID de la tarea asociada
            reason: Razón de la eliminación
            correlation_id: ID de correlación (opcional)
            
        Returns:
            Dict: Respuesta de la publicación del comando
        """
        # Preparar los datos del comando
        command_data = {
            "image_id": image_id,
            "task_id": task_id,
            "reason": reason
        }
        
        # Configurar tópico
        topic = self.settings.command_topics.get("delete_retrieved_image")
        if not topic:
            raise ValueError("No topic configured for delete_retrieved_image command")
        
        # Enviar comando
        logger.info(f"Sending DeleteRetrievedImage command for image {image_id}")
        response = await self.publisher.publish_command(
            command_type="DeleteRetrievedImage",
            data=command_data,
            topic=topic,
            correlation_id=correlation_id
        )
        
        logger.info(f"DeleteRetrievedImage command sent successfully: {response}")
        return response
    
    async def send_batch_deletion(
        self,
        task_id: str,
        image_ids: list,
        reason: str,
        correlation_id: str = None
    ) -> Dict[str, Any]:
        """
        Envía comandos para eliminar un lote de imágenes.
        
        Args:
            task_id: ID de la tarea asociada
            image_ids: Lista de IDs de imágenes a eliminar
            reason: Razón de la eliminación
            correlation_id: ID de correlación (opcional)
            
        Returns:
            Dict: Resumen de las respuestas
        """
        results = []
        
        # Enviar comandos de eliminación para cada imagen
        for image_id in image_ids:
            try:
                result = await self.send_delete_retrieved_image(
                    image_id=image_id,
                    task_id=task_id,
                    reason=reason,
                    correlation_id=correlation_id
                )
                results.append(result)
                # Pequeña pausa para evitar sobrecarga
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error sending deletion command for image {image_id}: {str(e)}")
                results.append({
                    "image_id": image_id,
                    "status": "error",
                    "error": str(e)
                })
        
        return {
            "task_id": task_id,
            "deleted_count": len([r for r in results if r.get("status") != "error"]),
            "error_count": len([r for r in results if r.get("status") == "error"]),
            "results": results
        }