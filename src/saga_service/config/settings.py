import os
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache
from typing import Dict

class Settings(BaseSettings):
    """Configuración global de la aplicación"""

    # Configuración general
    environment: str = Field(default="dev")
    log_level: str = Field(default="INFO")

    # Configuración de la base de datos
    db_host: str = Field(default="postgres")
    db_port: int = Field(default=5432)
    db_user: str = Field(default="user")
    db_password: str = Field(default="password")
    db_name: str = Field(default="saga_service_db")

    @property
    def db_url(self) -> str:
        """URL de conexión a la base de datos"""
        return os.getenv(
            "DATABASE_URL",
            f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}",
        )

    # Configuración de Pulsar
    pulsar_service_url: str = Field(default="pulsar://pulsar-broker:6650")
    pulsar_token: str = Field(default="")
    
    # Mapeo de eventos a tópicos de Pulsar para escuchar
    topics_to_listen: Dict[str, str] = Field(default={
        # Data Retrieval topics
        "data_retrieval_started": "persistent://public/default/retrieval-started",
        "data_retrieval_completed": "persistent://public/default/retrieval-completed",
        "data_retrieval_failed": "persistent://public/default/retrieval-failed",
        "images_retrieved": "persistent://public/default/images-retrieved",
        "image_ready_for_anonymization": "persistent://public/default/image-anonymization",
        "image_upload_failed": "persistent://public/default/image-upload-failed",
        
        # Anonymization topics
        "anonymization_requested": "persistent://public/default/anonymization-requests",
        "anonymization_completed": "persistent://public/default/anonymization-completed",
        "anonymization_failed": "persistent://public/default/anonymization-failed",
        "image_ready_for_processing": "persistent://public/default/image-processing",
        
        # Processing topics - Legacy
        "processing_started": "persistent://public/default/processing.*.started",
        "processing_completed": "persistent://public/default/processing.*.completed",
        "processing_failed": "persistent://public/default/processing.*.failed",
        
        # Processing topics - New Node.js implementation
        "node_processing_started": "persistent://public/default/processing-started",
        "node_processing_completed": "persistent://public/default/image-processed",
        "node_processing_failed": "persistent://public/default/processing-failed",
        
        # USA Processing Topics
        "usa_processing_started": "persistent://public/default/usa-processing-started",
        "usa_processing_completed": "persistent://public/default/usa-processing-completed",
        "usa_processing_failed": "persistent://public/default/usa-processing-failed",
        
        # LATAM Processing Topics
        "latam_processing_started": "persistent://public/default/latam-processing-started",
        "latam_processing_completed": "persistent://public/default/latam-processing-completed",
        "latam_processing_failed": "persistent://public/default/latam-processing-failed",
    })
    
    # Comandos para compensación
    command_topics: Dict[str, str] = Field(default={
        "delete_retrieved_image": "persistent://public/default/data-retrieval-commands",
        "rollback_anonymization": "persistent://public/default/anonymization-commands",
    })
    
    # Configuración del API
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8085)
    api_reload: bool = Field(default=True)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

@lru_cache()
def get_settings() -> Settings:
    """Retorna una instancia de la configuración cacheada"""
    return Settings()