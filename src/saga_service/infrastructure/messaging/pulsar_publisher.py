import json
import logging
import uuid
import pulsar
from typing import Dict, Any, Optional
import asyncio

logger = logging.getLogger(__name__)

class PulsarPublisher:
    """
    Publicador de mensajes usando Apache Pulsar.
    Se encarga de publicar comandos de compensación a los servicios correspondientes.
    """

    def __init__(
        self,
        service_url: str,
        token: Optional[str] = None,
        client_config: Dict[str, Any] = None,
    ):
        """
        Inicializa el publicador de mensajes

        Args:
            service_url: URL del servicio Pulsar
            token: Token de autenticación para Pulsar (opcional)
            client_config: Configuración adicional para el cliente Pulsar
        """
        self.service_url = service_url
        self.token = token
        self.client_config = client_config or {}
        self.client = None
        self.producers = {}

    def _initialize(self):
        """Inicializa la conexión a Pulsar si aún no existe"""
        if not self.client:
            try:
                # Configurar autenticación si hay token
                auth = None
                if self.token:
                    auth = pulsar.AuthenticationToken(self.token)

                self.client = pulsar.Client(
                    service_url=self.service_url,
                    authentication=auth,
                    **self.client_config
                )

                logger.info("Pulsar client initialized successfully")
            except Exception as e:
                logger.error(f"Error initializing Pulsar client: {str(e)}")
                raise

    def _get_producer(self, topic: str):
        """Obtiene o crea un productor para un tópico específico"""
        if topic not in self.producers:
            try:
                self.producers[topic] = self.client.create_producer(topic)
                logger.info(f"Created producer for topic: {topic}")
            except Exception as e:
                logger.error(f"Error creating producer for topic {topic}: {str(e)}")
                raise

        return self.producers[topic]

    async def publish_command(self, command_type: str, data: Dict[str, Any], topic: str, correlation_id: str = None):
        """
        Publica un comando en Pulsar

        Args:
            command_type: Tipo de comando a publicar
            data: Datos del comando
            topic: Tópico donde publicar
            correlation_id: ID de correlación para seguimiento (opcional)
            
        Returns:
            dict: Información sobre el mensaje publicado
        """
        try:
            # Asegurarse de que el cliente está inicializado
            self._initialize()

            # Añadir metadatos al comando
            command = {
                "type": command_type,
                "id": str(uuid.uuid4()),
                "correlation_id": correlation_id or str(uuid.uuid4()),
                "timestamp": str(asyncio.get_event_loop().time()),
                "data": data
            }

            # Serializar el comando a JSON
            command_json = json.dumps(command)

            # Obtener un productor para el tópico
            producer = self._get_producer(topic)

            # Enviar el mensaje de forma asíncrona
            loop = asyncio.get_event_loop()
            message_id = await loop.run_in_executor(
                None, lambda: producer.send(command_json.encode("utf-8"))
            )

            logger.info(f"Command {command_type} published to topic {topic} with ID {message_id}")
            
            return {
                "command_id": command["id"],
                "correlation_id": command["correlation_id"],
                "topic": topic,
                "message_id": str(message_id),
                "status": "published"
            }
        except Exception as e:
            logger.error(f"Error publishing command {command_type}: {str(e)}")
            raise

    def close(self):
        """Cierra las conexiones con Pulsar"""
        if self.client:
            for topic, producer in self.producers.items():
                try:
                    producer.close()
                except Exception as e:
                    logger.warning(
                        f"Error closing producer for topic {topic}: {str(e)}"
                    )

            try:
                self.client.close()
                self.client = None
                self.producers = {}
                logger.info("Pulsar client closed successfully")
            except Exception as e:
                logger.warning(f"Error closing Pulsar client: {str(e)}")