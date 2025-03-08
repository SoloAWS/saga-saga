import uuid
import logging
from typing import Dict, Any

from ...config.settings import get_settings
from ..messaging.pulsar_publisher import PulsarPublisher

logger = logging.getLogger(__name__)

class AnonymizationCommandsSender:
    """
    Clase encargada de enviar comandos de compensación al servicio de Anonymization.
    """
    
    def __init__(self, publisher: PulsarPublisher):
        """
        Inicializa el enviador de comandos.
        
        Args:
            publisher: Publicador de Pulsar para enviar comandos
        """
        self.publisher = publisher
        self.settings = get_settings()
    
    async def send_rollback_anonymization(
        self,
        task_id: str,
        reason: str,
        correlation_id: str = None
    ) -> Dict[str, Any]:
        """
        Envía un comando para revertir la anonimización.
        
        Args:
            task_id: ID de la tarea o imagen a revertir
            reason: Razón de la reversión
            correlation_id: ID de correlación (opcional)
            
        Returns:
            Dict: Respuesta de la publicación del comando
        """
        # Preparar los datos del comando
        command_data = {
            "task_id": task_id,
            "reason": reason
        }
        
        # Configurar tópico
        topic = self.settings.command_topics.get("rollback_anonymization")
        if not topic:
            raise ValueError("No topic configured for rollback_anonymization command")
        
        # Enviar comando
        logger.info(f"Sending RollbackAnonymization command for task {task_id}")
        response = await self.publisher.publish_command(
            command_type="RollbackAnonymization",
            data=command_data,
            topic=topic,
            correlation_id=correlation_id
        )
        
        logger.info(f"RollbackAnonymization command sent successfully: {response}")
        return response