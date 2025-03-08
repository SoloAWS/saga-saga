import logging
from typing import Dict, Any, Optional
from fastapi import Depends

from .database import get_db, create_session
from .settings import get_settings
from ..infrastructure.messaging.pulsar_publisher import PulsarPublisher
from ..infrastructure.messaging.pulsar_consumer import PulsarConsumer
from ..core.saga_coordinator import SagaCoordinator
from ..services.compensation_service import CompensationService
from ..services.data_retrieval_listener import DataRetrievalListener
from ..services.anonymization_listener import AnonymizationListener
from ..services.processing_listener import ProcessingListener
from ..infrastructure.commands.data_retrieval_commands import DataRetrievalCommandsSender
from ..infrastructure.commands.anonymization_commands import AnonymizationCommandsSender

logger = logging.getLogger(__name__)

# Variables globales para almacenar instancias singleton
_publisher_instance = None
_consumer_instance = None
_coordinator_instance = None
_compensation_service_instance = None
_data_retrieval_listener = None
_anonymization_listener = None
_processing_listener = None
_data_retrieval_commands = None
_anonymization_commands = None

def initialize_publisher() -> Optional[PulsarPublisher]:
    """
    Inicializa el publicador de Pulsar.
    
    Returns:
        PulsarPublisher: Instancia del publicador o None si hay error
    """
    global _publisher_instance
    
    if _publisher_instance is not None:
        return _publisher_instance
    
    settings = get_settings()
    
    try:
        if settings.pulsar_service_url:
            _publisher_instance = PulsarPublisher(
                service_url=settings.pulsar_service_url,
                token=settings.pulsar_token
            )
            logger.info("Pulsar publisher initialized successfully")
        else:
            logger.warning("Pulsar service URL not configured, messaging disabled")
    except Exception as e:
        logger.error(f"Error initializing Pulsar publisher: {str(e)}")
        logger.warning("Continuing without Pulsar messaging")
    
    return _publisher_instance

def get_publisher() -> Optional[PulsarPublisher]:
    """
    Obtiene la instancia del publicador de Pulsar.
    
    Returns:
        PulsarPublisher: Instancia del publicador o None si no está inicializado
    """
    return _publisher_instance

def get_coordinator() -> SagaCoordinator:
    """
    Obtiene la instancia del coordinador de Saga.
    
    Returns:
        SagaCoordinator: Instancia del coordinador
    """
    global _coordinator_instance
    
    if _coordinator_instance is None:
        _coordinator_instance = SagaCoordinator()
        logger.info("SagaCoordinator instance created")
    
    return _coordinator_instance

def get_data_retrieval_commands() -> DataRetrievalCommandsSender:
    """
    Obtiene la instancia del enviador de comandos para Data Retrieval.
    
    Returns:
        DataRetrievalCommandsSender: Instancia del enviador de comandos
    """
    global _data_retrieval_commands
    
    if _data_retrieval_commands is None:
        publisher = get_publisher()
        _data_retrieval_commands = DataRetrievalCommandsSender(publisher)
        logger.info("DataRetrievalCommandsSender instance created")
    
    return _data_retrieval_commands

def get_anonymization_commands() -> AnonymizationCommandsSender:
    """
    Obtiene la instancia del enviador de comandos para Anonymization.
    
    Returns:
        AnonymizationCommandsSender: Instancia del enviador de comandos
    """
    global _anonymization_commands
    
    if _anonymization_commands is None:
        publisher = get_publisher()
        _anonymization_commands = AnonymizationCommandsSender(publisher)
        logger.info("AnonymizationCommandsSender instance created")
    
    return _anonymization_commands

def get_compensation_service() -> CompensationService:
    """
    Obtiene la instancia del servicio de compensación.
    
    Returns:
        CompensationService: Instancia del servicio de compensación
    """
    global _compensation_service_instance
    
    if _compensation_service_instance is None:
        publisher = get_publisher()
        coordinator = get_coordinator()
        _compensation_service_instance = CompensationService(publisher, coordinator)
        logger.info("CompensationService instance created")
    
    return _compensation_service_instance

def get_data_retrieval_listener() -> DataRetrievalListener:
    """
    Obtiene la instancia del listener para Data Retrieval.
    
    Returns:
        DataRetrievalListener: Instancia del listener
    """
    global _data_retrieval_listener
    
    if _data_retrieval_listener is None:
        coordinator = get_coordinator()
        _data_retrieval_listener = DataRetrievalListener(coordinator)
        logger.info("DataRetrievalListener instance created")
    
    return _data_retrieval_listener

def get_anonymization_listener() -> AnonymizationListener:
    """
    Obtiene la instancia del listener para Anonymization.
    
    Returns:
        AnonymizationListener: Instancia del listener
    """
    global _anonymization_listener
    
    if _anonymization_listener is None:
        coordinator = get_coordinator()
        _anonymization_listener = AnonymizationListener(coordinator)
        logger.info("AnonymizationListener instance created")
    
    return _anonymization_listener

def get_processing_listener() -> ProcessingListener:
    """
    Obtiene la instancia del listener para Processing.
    
    Returns:
        ProcessingListener: Instancia del listener
    """
    global _processing_listener
    
    if _processing_listener is None:
        coordinator = get_coordinator()
        compensation_service = get_compensation_service()
        _processing_listener = ProcessingListener(coordinator, compensation_service)
        logger.info("ProcessingListener instance created")
    
    return _processing_listener

async def initialize_consumer() -> Optional[PulsarConsumer]:
    """
    Inicializa el consumidor de Pulsar.
    
    Returns:
        PulsarConsumer: Instancia del consumidor o None si hay error
    """
    global _consumer_instance
    
    if _consumer_instance is not None:
        return _consumer_instance
    
    settings = get_settings()
    
    try:
        if settings.pulsar_service_url and settings.topics_to_listen:
            # Inicializar manejadores de eventos
            data_retrieval_listener = get_data_retrieval_listener()
            anonymization_listener = get_anonymization_listener()
            processing_listener = get_processing_listener()
            
            # Definir los manejadores de eventos
            event_handlers = {
                # Data Retrieval handlers
                "RetrievalStarted": data_retrieval_listener.handle_retrieval_started,
                "RetrievalCompleted": data_retrieval_listener.handle_retrieval_completed,
                "RetrievalFailed": data_retrieval_listener.handle_retrieval_failed,
                "ImagesRetrieved": data_retrieval_listener.handle_image_uploaded,
                "ImageUploadFailed": data_retrieval_listener.handle_image_upload_failed,
                "ImageReadyForAnonymization": data_retrieval_listener.handle_image_ready_for_anonymization,
                
                # Anonymization handlers
                "AnonymizationRequested": anonymization_listener.handle_anonymization_requested,
                "AnonymizationCompleted": anonymization_listener.handle_anonymization_completed,
                "AnonymizationFailed": anonymization_listener.handle_anonymization_failed,
                "ImageReadyForProcessing": anonymization_listener.handle_image_ready_for_processing,
                "AnonymizationRolledBack": anonymization_listener.handle_anonymization_rolled_back,
                
                # Processing handlers
                "ProcessingStarted": processing_listener.handle_processing_started,
                "ProcessingCompleted": processing_listener.handle_processing_completed,
                "ProcessingFailed": processing_listener.handle_processing_failed,
                # Wildcard para manejar múltiples tópicos regionales
                "processing.*.started": processing_listener.handle_processing_started,
                "processing.*.completed": processing_listener.handle_processing_completed,
                "processing.*.failed": processing_listener.handle_processing_failed,
            }
            
            # Inicializar consumidor
            _consumer_instance = PulsarConsumer(
                service_url=settings.pulsar_service_url,
                subscription_name="saga-service",
                topics=list(settings.topics_to_listen.values()),
                event_handlers=event_handlers,
                token=settings.pulsar_token
            )
            
            try:
                # Iniciar el consumidor
                await _consumer_instance.start()
                logger.info("Pulsar consumer initialized and started successfully")
            except Exception as e:
                # Si hay un error al iniciar, asegurarnos de cerrar el consumidor
                logger.error(f"Error starting Pulsar consumer: {str(e)}")
                if _consumer_instance:
                    await _consumer_instance.close()
                    _consumer_instance = None
                logger.warning("Continuing without Pulsar consumer")
        else:
            logger.warning("Pulsar service URL or topics not configured, consumer disabled")
    except Exception as e:
        logger.error(f"Error initializing Pulsar consumer: {str(e)}")
        logger.warning("Continuing without Pulsar consumer")
        _consumer_instance = None
    
    return _consumer_instance

def get_consumer() -> Optional[PulsarConsumer]:
    """
    Obtiene la instancia del consumidor de Pulsar.
    
    Returns:
        PulsarConsumer: Instancia del consumidor o None si no está inicializado
    """
    return _consumer_instance

async def shutdown_services():
    """Cierra todos los servicios."""
    global _consumer_instance, _publisher_instance
    
    # Detener consumidor
    if _consumer_instance:
        try:
            await _consumer_instance.stop()
            logger.info("Pulsar consumer stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping Pulsar consumer: {str(e)}")
        _consumer_instance = None
    
    # Cerrar publicador
    if _publisher_instance:
        try:
            _publisher_instance.close()
            logger.info("Pulsar publisher closed successfully")
        except Exception as e:
            logger.error(f"Error closing Pulsar publisher: {str(e)}")
        _publisher_instance = None