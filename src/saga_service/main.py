import logging
import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config.settings import get_settings
from .config.database import init_db
from .config.dependencies import (
    initialize_publisher,
    initialize_consumer,
    shutdown_services
)
from .api.v1.routes import router as v1_router

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Obtener configuración
settings = get_settings()

# Crear la aplicación FastAPI
app = FastAPI(
    title="Saga Service",
    description="Servicio para gestión de sagas en el procesamiento de imágenes médicas",
    version="1.0.0"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configurar rutas
app.include_router(v1_router, prefix="/api/v1/saga")

# Inicialización de la aplicación
@app.on_event("startup")
async def startup_event():
    logger.info("Iniciando servicio de saga")
    
    # Inicializar la base de datos
    try:
        await init_db()
        logger.info("Base de datos inicializada correctamente")
    except Exception as e:
        logger.error(f"Error al inicializar la base de datos: {str(e)}")
        raise
    
    # Inicializar el publicador de Pulsar
    try:
        publisher = initialize_publisher()
        if publisher:
            logger.info("Publicador de Pulsar inicializado correctamente")
        else:
            logger.warning("Publicador de Pulsar no disponible")
    except Exception as e:
        logger.error(f"Error al inicializar el publicador de Pulsar: {str(e)}")
    
    # Inicializar el consumidor de Pulsar
    try:
        consumer = await initialize_consumer()
        if consumer:
            logger.info("Consumidor de Pulsar inicializado correctamente")
        else:
            logger.warning("Consumidor de Pulsar no disponible")
    except Exception as e:
        logger.error(f"Error al inicializar el consumidor de Pulsar: {str(e)}")
    
    logger.info("Servicio de saga iniciado correctamente")

# Evento de cierre de la aplicación
@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Cerrando servicio de saga")
    
    # Cerrar servicios de mensajería
    try:
        await shutdown_services()
        logger.info("Servicios de mensajería cerrados correctamente")
    except Exception as e:
        logger.error(f"Error al cerrar servicios de mensajería: {str(e)}")
    
    logger.info("Servicio de saga cerrado correctamente")

# Endpoint raíz
@app.get("/")
def root():
    return {"message": "Bienvenido al Servicio de Saga"}

# Health check endpoint
@app.get("/health")
def health_check():
    return {"status": "ok"}

# Ejecutar la aplicación si se llama directamente
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_reload,
        log_level=settings.log_level.lower()
    )