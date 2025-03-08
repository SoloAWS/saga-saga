import json
import logging
import asyncio
import pulsar
import traceback
import re
from typing import Dict, Any, List, Callable, Awaitable, Optional
from concurrent.futures import ThreadPoolExecutor

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
        max_workers: int = 5,
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
                service_url=self.service_url, authentication=auth
            )

            # Crear executor de threads
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers)

            # Marcar como en ejecución
            self._is_running = True

            # SAFETY FIRST APPROACH: Individual subscriptions
            # Esto asegura que no perdemos ningún tópico

            # Opción configurable para deshabilitar la optimización de patrones
            # Si tienes dudas sobre la suscripción por patrones, configura esta variable como True
            disable_pattern_subscription = False

            if disable_pattern_subscription:
                # Usar suscripción individual para cada tópico
                logger.info(
                    "Using individual topic subscriptions for maximum reliability"
                )
                for topic in self.topics:
                    await self._subscribe_to_individual_topic(topic, "individual")
            else:
                # Agrupar tópicos por servicio para reducir el número de consumidores
                topic_groups = self._group_topics_by_service()

                # Para cada grupo de servicio, crear un consumidor con patrón si es posible
                for service_name, topics_in_group in topic_groups.items():
                    # Verificar si podemos usar un patrón para este grupo
                    pattern_info = self._create_pattern_for_topics(topics_in_group)

                    if (
                        pattern_info and len(topics_in_group) > 2
                    ):  # Solo usar patrón si hay múltiples tópicos similares
                        pattern, matched_topics = pattern_info

                        # Verificación de seguridad: asegurarse que el patrón cubre todos los tópicos del grupo
                        if len(matched_topics) == len(topics_in_group):
                            # Usar suscripción basada en patrón
                            try:
                                subscription = (
                                    f"{self.subscription_name}-{service_name}"
                                )
                                consumer = self.client.subscribe(
                                    topic_pattern=pattern,
                                    subscription_name=subscription,
                                    consumer_type=pulsar.ConsumerType.Shared,
                                    **self.consumer_config,
                                )

                                self.consumers[service_name] = consumer
                                logger.info(
                                    f"Subscribed to topic pattern: {pattern} for service {service_name}"
                                )
                                logger.info(
                                    f"Pattern covers these topics: {', '.join(matched_topics)}"
                                )

                                # Iniciar tarea de consumo
                                task = asyncio.create_task(
                                    self._consume_messages(
                                        consumer, f"{service_name}-pattern"
                                    )
                                )
                                self._consumer_tasks.append(task)

                            except Exception as e:
                                logger.error(
                                    f"Error subscribing to pattern {pattern} for {service_name}: {str(e)}"
                                )
                                # Fallar a suscripciones individuales
                                for topic in topics_in_group:
                                    await self._subscribe_to_individual_topic(
                                        topic, service_name
                                    )
                        else:
                            # El patrón no cubre todos los tópicos, usar suscripciones individuales
                            logger.warning(
                                f"Pattern {pattern} doesn't cover all topics in group {service_name}. Using individual subscriptions instead."
                            )
                            for topic in topics_in_group:
                                await self._subscribe_to_individual_topic(
                                    topic, service_name
                                )
                    else:
                        # Usar suscripciones individuales con nombre de suscripción compartido
                        for topic in topics_in_group:
                            await self._subscribe_to_individual_topic(
                                topic, service_name
                            )

            # Verificar que todos los tópicos están cubiertos
            subscribed_topics = set()
            for consumer_id, consumer in self.consumers.items():
                if "-pattern" in consumer_id:
                    # Este es un consumidor de patrón, no podemos determinar exactamente qué tópicos cubre
                    # pero ya lo registramos en logs más arriba
                    pass
                else:
                    subscribed_topics.add(consumer.topic())

            # Comparar con la lista original de tópicos
            original_topics_set = set(self.topics)
            missing_topics = original_topics_set - subscribed_topics

            if missing_topics and not any(
                "-pattern" in c for c in self.consumers.keys()
            ):
                logger.error(
                    f"WARNING: Some topics are not subscribed: {missing_topics}"
                )

            logger.info(
                f"Pulsar consumer started with {len(self.consumers)} consumers for {len(self.topics)} topics"
            )
        except Exception as e:
            logger.error(f"Error starting Pulsar consumer: {str(e)}")
            await self.close()
            raise

    async def _subscribe_to_individual_topic(self, topic, service_name):
        """Suscribe a un tópico individual con nombre de suscripción por servicio"""
        try:
            # Usar el mismo nombre de suscripción para todos los tópicos del mismo servicio
            subscription = f"{self.subscription_name}-{service_name}"

            consumer = self.client.subscribe(
                topic=topic,
                subscription_name=subscription,
                consumer_type=pulsar.ConsumerType.Shared,
                **self.consumer_config,
            )

            self.consumers[topic] = consumer
            logger.info(f"Subscribed to topic: {topic} (subscription: {subscription})")

            # Iniciar tarea de consumo
            task = asyncio.create_task(self._consume_messages(consumer, topic))
            self._consumer_tasks.append(task)

        except Exception as e:
            logger.error(f"Error subscribing to topic {topic}: {str(e)}")

    def _group_topics_by_service(self):
        """Agrupa tópicos por servicio para optimizar las suscripciones"""
        groups = {
            "data-retrieval": [],
            "anonymization": [],
            "processing": [],
            "misc": [],
        }

        for topic in self.topics:
            topic_name = topic.split("/")[-1]

            if "retrieval" in topic_name:
                groups["data-retrieval"].append(topic)
            elif "anonymization" in topic_name:
                groups["anonymization"].append(topic)
            elif "processing" in topic_name or "processed" in topic_name:
                groups["processing"].append(topic)
            else:
                groups["misc"].append(topic)

        # Eliminar grupos vacíos
        return {k: v for k, v in groups.items() if v}

    def _create_pattern_for_topics(self, topics):
        """
        Crea un patrón regex para un grupo de tópicos si es posible y verifica
        que el patrón realmente cubra todos los tópicos del grupo.

        Returns:
            tuple: (pattern, matched_topics) si se puede crear un patrón, None en caso contrario
        """
        if not topics or len(topics) < 2:
            return None

        # Extraer los nombres de tópico sin el prefijo común
        topic_names = [t.split("/")[-1] for t in topics]

        # Buscar prefijo común
        prefix = self._find_common_prefix(topic_names)
        if prefix and len(prefix) >= 3:  # Prefijo significativo
            # Crear un patrón basado en el prefijo común
            namespace_prefix = topics[0].rsplit("/", 1)[0]
            pattern = f"{namespace_prefix}/{prefix}.*"

            # IMPORTANTE: Verificar que el patrón realmente coincida con todos los tópicos
            regex = re.compile(pattern.replace(".", "\\.").replace("*", ".*"))
            matched_topics = [t for t in topics if regex.match(t)]

            # Solo devolver el patrón si cubre todos los tópicos del grupo
            if len(matched_topics) == len(topics):
                return (pattern, matched_topics)
            else:
                logger.warning(
                    f"Pattern {pattern} only matches {len(matched_topics)}/{len(topics)} topics. "
                    f"Unmatched: {set(topics) - set(matched_topics)}"
                )

        return None

    def _find_common_prefix(self, strings):
        """Encuentra el prefijo común más largo en una lista de strings"""
        if not strings:
            return ""

        prefix = strings[0]
        for s in strings[1:]:
            while not s.startswith(prefix) and prefix:
                prefix = prefix[:-1]
            if not prefix:
                break

        return prefix

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

    async def _consume_messages(self, consumer, topic_identifier):
        """
        Tarea principal para consumir mensajes de un tópico o patrón específico.
        Se ejecuta continuamente hasta que se detiene el consumidor.

        Args:
            consumer: Consumidor de Pulsar
            topic_identifier: Identificador del tópico o patrón
        """
        logger.info(f"Started consuming messages from: {topic_identifier}")

        while self._is_running:
            try:
                # Recibir mensaje (operación bloqueante) con un timeout
                loop = asyncio.get_event_loop()
                msg_future = loop.run_in_executor(
                    self._executor, lambda: consumer.receive(timeout_millis=1000)
                )

                try:
                    # Esperar mensaje con timeout
                    msg = await asyncio.wait_for(msg_future, timeout=2.0)

                    # Reiniciar contador de timeouts
                    self._timeout_counter = 0

                    # Procesar mensaje en tarea separada
                    asyncio.create_task(self._process_message(consumer, msg))

                except asyncio.TimeoutError:
                    # Timeout normal, solo incrementar contador
                    self._timeout_counter += 1

                    # Registrar solo periódicamente para evitar inundar el log
                    if self._timeout_counter % self._log_interval == 0:
                        logger.debug(
                            f"No new messages in {self._timeout_counter} polls from {topic_identifier}"
                        )

                    # Verificar si debemos seguir ejecutando
                    continue
                except pulsar._pulsar.Timeout:
                    # Timeout de Pulsar, comportamiento similar
                    self._timeout_counter += 1
                    continue

            except asyncio.CancelledError:
                # La tarea fue cancelada, salir del bucle
                logger.info(f"Consumer task for {topic_identifier} cancelled")
                break
            except Exception as e:
                logger.error(
                    f"Unexpected error in consumer loop for {topic_identifier}: {str(e)}"
                )
                await asyncio.sleep(1.0)  # Pausa para evitar CPU 100%

        logger.info(f"Stopped consuming messages from: {topic_identifier}")

    async def _process_message(self, consumer, msg):
        """
        Procesa un mensaje recibido de Pulsar.

        Args:
            consumer: Consumidor que recibió el mensaje
            msg: Mensaje de Pulsar
        """
        try:
            # Decodificar el mensaje
            payload = msg.data().decode("utf-8")
            data = json.loads(payload)

            # Extraer información del evento
            event_type = data.get("type")

            if not event_type:
                logger.warning(
                    f"Received message without type from topic {consumer.topic()}"
                )
                consumer.acknowledge(msg)
                return

            logger.info(f"Received event {event_type} from topic {consumer.topic()}")

            # Buscar un manejador para este tipo de evento
            if event_type in self.event_handlers:
                handler = self.event_handlers[event_type]

                # Ejecutar el handler dentro de un contexto que maneje correctamente
                # la transacción de base de datos
                try:
                    await handler(data)
                    logger.info(f"Event {event_type} processed successfully")
                except Exception as handler_error:
                    logger.error(
                        f"Error in handler for event {event_type}: {str(handler_error)}"
                    )
                    logger.error(traceback.format_exc())
                    # Negative acknowledgment para que se reintente
                    consumer.negative_acknowledge(msg)
                    return
            else:
                # Si no hay un manejador específico, buscar un manejador genérico por prefijo
                # Esto es útil para eventos como "processing.*.failed"
                handled = False
                for handler_key, handler in self.event_handlers.items():
                    if handler_key.endswith("*") and event_type.startswith(
                        handler_key[:-1]
                    ):
                        try:
                            await handler(data)
                            logger.info(
                                f"Event {event_type} processed by wildcard handler {handler_key}"
                            )
                            handled = True
                            break
                        except Exception as handler_error:
                            logger.error(
                                f"Error in wildcard handler {handler_key} for event {event_type}: {str(handler_error)}"
                            )
                            logger.error(traceback.format_exc())
                            # Negative acknowledgment para que se reintente
                            consumer.negative_acknowledge(msg)
                            return

                if not handled:
                    logger.warning(f"No handler found for event type: {event_type}")

            # Acknowledgment del mensaje procesado
            consumer.acknowledge(msg)

        except json.JSONDecodeError as e:
            logger.error(f"Error decoding event message: {str(e)}")
            # Mensaje malformado, no intentar de nuevo
            consumer.acknowledge(msg)
        except Exception as e:
            logger.error(
                f"Error processing event {event_type if 'event_type' in locals() else 'unknown'}: {str(e)}"
            )
            logger.error(traceback.format_exc())
            # Negative acknowledgment para que se reintente
            consumer.negative_acknowledge(msg)

    def register_event_handler(
        self, event_type: str, handler: Callable[[Dict[str, Any]], Awaitable[None]]
    ):
        """
        Registra un manejador para un tipo de evento.

        Args:
            event_type: Tipo de evento
            handler: Función asíncrona para manejar el evento
        """
        self.event_handlers[event_type] = handler
        logger.info(f"Registered handler for event type: {event_type}")
