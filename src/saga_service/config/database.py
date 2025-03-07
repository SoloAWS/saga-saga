
# src/saga_service/config/database.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from contextlib import asynccontextmanager
import logging

from .settings import get_settings

# Obtener configuración
settings = get_settings()

# Configurar logging
logger = logging.getLogger(__name__)

# Crear el motor de base de datos
engine = create_async_engine(
    settings.db_url,
    echo=settings.environment == "dev",
    future=True
)

# Crear la sesión asíncrona
async_session_factory = sessionmaker(
    engine, 
    class_=AsyncSession, 
    expire_on_commit=False
)

Base = declarative_base()

async def init_db():
    """Inicializa la base de datos"""    
    
    async with engine.begin() as conn:
        logger.info("Creating all tables if they don't exist")
        await conn.run_sync(Base.metadata.create_all)
        
    logger.info("Database initialized successfully")

# Función para obtener una sesión de base de datos
@asynccontextmanager
async def get_db():
    """Provides an async database session"""
    async with async_session_factory() as session:
        try:
            yield session
        except Exception as e:
            await session.rollback()
            logger.error(f"Error with database session: {e}")
            raise

# Función para crear una nueva sesión de base de datos
def create_session() -> AsyncSession:
    """Creates a new database session"""
    return async_session_factory()
