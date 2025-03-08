import json
import logging
import asyncio
import pulsar
import traceback
from typing import Dict, Any, List, Callable, Awaitable, Optional
from concurrent.futures import ThreadPoolExecutor  # Importación corregida

logger = logging.getLogger(__name__)

class PulsarConsumer:
    """
    Consumidor de mensajes usando Apache Pulsar.
    Se encarga de recibir eventos de los microservicios para construir el saga log.
    """

    def __init__(
        self,
        service_url: str,
        subscription_name: str,
        topics: List[str],
        event_handlers: Dict[str, Callable[[Dict[str, Any]], Awaitable[None]]],
        token: Optional[str] = None,
        consumer_config: Dict[str, Any] = None,
        max_workers: int = 5
    ):
        """
        Inicializa el consumidor de Pulsar

        Args:
            service_url: URL del servicio Pulsar
            subscription_name: Nombre de la suscripción
            topics: Lista de tópicos a los que suscribirse
            event_handlers: Diccionario que mapea tipos de eventos a sus manejadores
            token: Token de autenticación opcional
            consumer_config: Configuración adicional para el consumidor
            max_workers: Número máximo de workers para procesamiento en paralelo
        """
        self.service_url = service_url
        self.subscription_name = subscription_name
        self.topics = topics
        self.token = token
        self.consumer_config = consumer_config or {}
        self.event_handlers = event_handlers
        self.max_workers = max_workers
        
        # Componentes inicializados bajo demanda
        self.client = None
        self.consumers = {}
        self._is_running = False
        self._consumer_tasks = []
        self._executor = None
        
        # Contador para registro de eventos periódico
        self._timeout_counter = 0
        self._log_interval = 100  # Registrar cada 100 timeouts

    async def start(self):
        """Inicia el consumidor de Pulsar"""
        if self._is_running:
            logger.warning("Consumer is already running")
            return

        try:
            # Inicializar cliente Pulsar
            auth = None
            if self.token:
                auth = pulsar.AuthenticationToken(self.token)
            
            self.client = pulsar.Client(
                service_url=self.service_url,
                authentication=auth
            )
            
            # Crear executor de threads (corregido)
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
            
            # Marcar como en ejecución
            self._is_running = True
            
            # Para cada tópico, crear un consumidor y una tarea de consumo
            for topic in self.topics:
                # Crear un ID único para la suscripción a cada tópico
                # Esto permite tener múltiples consumidores para el mismo tópico
                subscription = f"{self.subscription_name}-{topic.split('/')[-1]}"
                
                # Crear consumidor
                try:
                    consumer = self.client.subscribe(
                        topic=topic,
                        subscription_name=subscription,
                        consumer_type=pulsar.ConsumerType.Shared,
                        **self.consumer_config
                    )
                    
                    self.consumers[topic] = consumer
                    logger.info(f"Subscribed to topic: {topic}")
                    
                    # Iniciar tarea de consumo
                    task = asyncio.create_task(self._consume_messages(consumer, topic))
                    self._consumer_tasks.append(task)
                    
                except Exception as e:
                    logger.error(f"Error subscribing to topic {topic}: {str(e)}")
            
            logger.info("Pulsar consumer started")
        except Exception as e:
            logger.error(f"Error starting Pulsar consumer: {str(e)}")
            await self.close()  # Usar método close en lugar de cerrar directamente
            raise

    async def stop(self):
        """Detiene el consumidor de Pulsar"""
        if not self._is_running:
            return

        logger.info("Stopping Pulsar consumer...")
        self._is_running = False
        
        # Cancelar tareas de consumo
        for task in self._consumer_tasks:
            task.cancel()
        
        # Esperar a que terminen las tareas
        if self._consumer_tasks:
            try:
                await asyncio.gather(*self._consumer_tasks, return_exceptions=True)
            except asyncio.CancelledError:
                pass
        
        self._consumer_tasks = []
        
        await self.close()  # Cerrar recursos
        
        logger.info("Pulsar consumer stopped")

    async def close(self):
        """Cierra consumidores, cliente y executor"""
        # Cerrar consumidores
        for topic, consumer in self.consumers.items():
            try:
                consumer.close()
            except Exception as e:
                logger.warning(f"Error closing consumer for topic {topic}: {str(e)}")
        
        self.consumers = {}
        
        # Cerrar executor
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None
        
        # Cerrar cliente
        if self.client:
            try:
                self.client.close()
                self.client = None
                logger.info("Pulsar client closed successfully")
            except Exception as e:
                logger.warning(f"Error closing Pulsar client: {str(e)}")

    async def _consume_messages(self, consumer, topic):
        """
        Tarea principal para consumir mensajes de un tópico específico.
        Se ejecuta continuamente hasta que se detiene el consumidor.
        
        Args:
            consumer: Consumidor de Pulsar
            topic: Tópico al que está suscrito el consumidor
        """
        logger.info(f"Started consuming messages from topic: {topic}")
        
        while self._is_running:
            try:
                # Recibir mensaje (operación bloqueante) con un timeout
                loop = asyncio.get_event_loop()
                msg_future = loop.run_in_executor(
                    self._executor, 
                    lambda: consumer.receive(timeout_millis=1000)
                )
                
                try:
                    # Esperar mensaje con timeout
                    msg = await asyncio.wait_for(msg_future, timeout=2.0)
                    
                    # Reiniciar contador de timeouts
                    self._timeout_counter = 0
                    
                    # Procesar mensaje en tarea separada
                    try:
                        await self._process_message(consumer, msg, topic)
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                        consumer.negative_acknowledge(msg)
                    
                except asyncio.TimeoutError:
                    # Timeout normal, solo incrementar contador
                    self._timeout_counter += 1
                    
                    # Registrar solo periódicamente para evitar inundar el log
                    if self._timeout_counter % self._log_interval == 0:
                        logger.debug(f"No new messages in {self._timeout_counter} polls from topic {topic}")
                    
                    # Verificar si debemos seguir ejecutando
                    continue
                except pulsar._pulsar.Timeout:
                    # Timeout de Pulsar, comportamiento similar
                    self._timeout_counter += 1
                    continue
                
            except asyncio.CancelledError:
                # La tarea fue cancelada, salir del bucle
                logger.info(f"Consumer task for topic {topic} cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in consumer loop for topic {topic}: {str(e)}")
                await asyncio.sleep(1.0)  # Pausa para evitar CPU 100%
        
        logger.info(f"Stopped consuming messages from topic: {topic}")

    async def _process_message(self, consumer, msg, topic):
        """
        Procesa un mensaje recibido de Pulsar.
        
        Args:
            consumer: Consumidor que recibió el mensaje
            msg: Mensaje de Pulsar
            topic: Tópico del que se recibió el mensaje
        """
        try:
            # Decodificar el mensaje
            payload = msg.data().decode('utf-8')
            data = json.loads(payload)
            
            # Extraer información del evento
            event_type = data.get('type')
            
            if not event_type:
                logger.warning(f"Received message without type from topic {topic}")
                consumer.acknowledge(msg)
                return
            
            logger.info(f"Received event {event_type} from topic {topic}")
            
            # Buscar un manejador para este tipo de evento
            if event_type in self.event_handlers:
                handler = self.event_handlers[event_type]
                await handler(data)
                logger.info(f"Event {event_type} processed successfully")
            else:
                # Si no hay un manejador específico, buscar un manejador genérico por prefijo
                # Esto es útil para eventos como "processing.*.failed"
                handled = False
                for handler_key, handler in self.event_handlers.items():
                    if handler_key.endswith('*') and event_type.startswith(handler_key[:-1]):
                        await handler(data)
                        logger.info(f"Event {event_type} processed by wildcard handler {handler_key}")
                        handled = True
                        break
                
                if not handled:
                    logger.warning(f"No handler found for event type: {event_type}")
            
            # Acknowledgment del mensaje procesado
            consumer.acknowledge(msg)
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding event message: {str(e)}")
            # Mensaje malformado, no intentar de nuevo
            consumer.acknowledge(msg)
        except Exception as e:
            logger.error(f"Error processing event {event_type if 'event_type' in locals() else 'unknown'}: {str(e)}")
            logger.error(traceback.format_exc())
            # Negative acknowledgment para que se reintente
            consumer.negative_acknowledge(msg)

    def register_event_handler(self, event_type: str, handler: Callable[[Dict[str, Any]], Awaitable[None]]):
        """
        Registra un manejador para un tipo de evento.
        
        Args:
            event_type: Tipo de evento
            handler: Función asíncrona para manejar el evento
        """
        self.event_handlers[event_type] = handler
        logger.info(f"Registered handler for event type: {event_type}")