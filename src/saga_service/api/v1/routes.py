import logging
from fastapi import APIRouter, HTTPException, Query, Path, Depends
from typing import List, Optional
import uuid
from datetime import datetime

from ...core.saga_coordinator import SagaCoordinator
from ...config.dependencies import get_coordinator, get_consumer, get_publisher
from ...domain.enums import SagaStatus
from .schemas import (
    SagaLogResponse,
    SagaLogSummaryResponse,
    PaginatedSagaLogsResponse,
    SagaStatusEnum,
    HealthCheckResponse
)

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """Verifica el estado del servicio."""
    # Verificar estado del publicador de Pulsar
    publisher = get_publisher()
    publisher_status = "ready" if publisher else "disabled"
    
    # Verificar estado del consumidor de Pulsar
    consumer = get_consumer()
    consumer_status = "ready" if consumer and consumer._is_running else "disabled"
    
    # Verificar estado de la base de datos
    # Aquí solo verificamos si podemos obtener el coordinador
    db_status = "ready"
    try:
        coordinator = get_coordinator()
        # Si llegamos hasta aquí, asumimos que la base de datos está disponible
    except Exception as e:
        logger.error(f"Error checking database status: {str(e)}")
        db_status = "error"
    
    return {
        "status": "ok",
        "version": "1.0.0",
        "database_status": db_status,
        "pulsar_publisher_status": publisher_status,
        "pulsar_consumer_status": consumer_status
    }

@router.get("/sagas", response_model=PaginatedSagaLogsResponse)
async def get_sagas(
    status: Optional[SagaStatusEnum] = None,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    task_id: Optional[str] = None,
    image_id: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    """
    Obtiene una lista paginada de sagas con filtros opcionales.
    
    Args:
        status: Estado de la saga
        from_date: Fecha de inicio desde
        to_date: Fecha de inicio hasta
        task_id: ID de tarea
        image_id: ID de imagen
        limit: Límite de resultados
        offset: Desplazamiento para paginación
    """
    coordinator = get_coordinator()
    
    # Si se proporciona un estado, filtramos por él
    if status:
        sagas = await coordinator.get_sagas_by_status(SagaStatus(status), limit=100)
    else:
        sagas = await coordinator.get_all_sagas(limit=100, offset=0)
    
    # Filtros adicionales (implementación básica)
    filtered_sagas = []
    for saga in sagas:
        # Filtrar por fecha
        if from_date and saga.start_time < from_date:
            continue
        if to_date and saga.start_time > to_date:
            continue
        
        # Filtrar por task_id o image_id
        if task_id or image_id:
            has_match = False
            for step in saga.steps:
                if task_id and step.entity_id == task_id:
                    has_match = True
                    break
                if task_id and step._metadata and step._metadata.get("task_id") == task_id:
                    has_match = True
                    break
                if image_id and step.entity_id == image_id:
                    has_match = True
                    break
                if image_id and step._metadata and step._metadata.get("image_id") == image_id:
                    has_match = True
                    break
            if not has_match:
                continue
        
        filtered_sagas.append(saga)
    
    # Aplicar paginación
    total = len(filtered_sagas)
    paginated_sagas = filtered_sagas[offset:offset + limit]
    
    # Convertir a response model
    items = []
    for saga in paginated_sagas:
        failed_steps = [step for step in saga.steps if step.status.value.endswith("FAILED")]
        items.append(SagaLogSummaryResponse(
            id=str(saga.id),
            correlation_id=saga.correlation_id,
            saga_type=saga.saga_type,
            status=SagaStatusEnum(saga.status.value),
            steps_count=len(saga.steps),
            failed_steps_count=len(failed_steps),
            start_time=saga.start_time,
            end_time=saga.end_time,
            error_message=saga.error_message
        ))
    
    return PaginatedSagaLogsResponse(
        total=total,
        offset=offset,
        limit=limit,
        items=items
    )

@router.get("/sagas/{saga_id}", response_model=SagaLogResponse)
async def get_saga_by_id(
    saga_id: str = Path(..., description="ID de la saga a consultar")
):
    """
    Obtiene los detalles de una saga específica.
    
    Args:
        saga_id: ID de la saga
    """
    try:
        saga_uuid = uuid.UUID(saga_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid saga ID format")
    
    coordinator = get_coordinator()
    saga = await coordinator.get_saga_by_id(saga_uuid)
    
    if not saga:
        raise HTTPException(status_code=404, detail=f"Saga with ID {saga_id} not found")
    
    # Convertir a response model
    steps = []
    for step in saga.steps:
        steps.append({
            "id": str(step.id),
            "saga_id": str(step.saga_id),
            "step_type": step.step_type.value,
            "service": step.service.value,
            "status": step.status.value,
            "event_id": step.event_id,
            "event_type": step.event_type,
            "entity_id": step.entity_id,
            "metadata": step._metadata,
            "error_message": step.error_message,
            "timestamp": step.timestamp,
            "compensation_timestamp": step.compensation_timestamp
        })
    
    return {
        "id": str(saga.id),
        "correlation_id": saga.correlation_id,
        "saga_type": saga.saga_type,
        "status": saga.status.value,
        "steps": steps,
        "start_time": saga.start_time,
        "end_time": saga.end_time,
        "error_message": saga.error_message,
        "metadata": saga._metadata
    }

@router.get("/sagas/by-correlation/{correlation_id}", response_model=SagaLogResponse)
async def get_saga_by_correlation_id(
    correlation_id: str = Path(..., description="ID de correlación de la saga")
):
    """
    Obtiene los detalles de una saga por su ID de correlación.
    
    Args:
        correlation_id: ID de correlación
    """
    coordinator = get_coordinator()
    saga = await coordinator.get_saga_by_correlation_id(correlation_id)
    
    if not saga:
        raise HTTPException(status_code=404, detail=f"Saga with correlation ID {correlation_id} not found")
    
    # Convertir a response model
    steps = []
    for step in saga.steps:
        steps.append({
            "id": str(step.id),
            "saga_id": str(step.saga_id),
            "step_type": step.step_type.value,
            "service": step.service.value,
            "status": step.status.value,
            "event_id": step.event_id,
            "event_type": step.event_type,
            "entity_id": step.entity_id,
            "metadata": step._metadata,
            "error_message": step.error_message,
            "timestamp": step.timestamp,
            "compensation_timestamp": step.compensation_timestamp
        })
    
    return {
        "id": str(saga.id),
        "correlation_id": saga.correlation_id,
        "saga_type": saga.saga_type,
        "status": saga.status.value,
        "steps": steps,
        "start_time": saga.start_time,
        "end_time": saga.end_time,
        "error_message": saga.error_message,
        "metadata": saga._metadata
    }

@router.get("/sagas/by-task/{task_id}", response_model=List[SagaLogSummaryResponse])
async def get_sagas_by_task_id(
    task_id: str = Path(..., description="ID de la tarea"),
    limit: int = Query(20, ge=1, le=100)
):
    """
    Obtiene las sagas relacionadas con una tarea específica.
    
    Args:
        task_id: ID de la tarea
        limit: Límite de resultados
    """
    coordinator = get_coordinator()
    sagas = await coordinator.get_all_sagas(limit=100)
    
    # Filtrar por task_id
    filtered_sagas = []
    for saga in sagas:
        for step in saga.steps:
            if step.entity_id == task_id or (step._metadata and step._metadata.get("task_id") == task_id):
                filtered_sagas.append(saga)
                break
    
    # Limitar resultados
    limited_sagas = filtered_sagas[:limit]
    
    # Convertir a response model
    result = []
    for saga in limited_sagas:
        failed_steps = [step for step in saga.steps if step.status.value.endswith("FAILED")]
        result.append(SagaLogSummaryResponse(
            id=str(saga.id),
            correlation_id=saga.correlation_id,
            saga_type=saga.saga_type,
            status=SagaStatusEnum(saga.status.value),
            steps_count=len(saga.steps),
            failed_steps_count=len(failed_steps),
            start_time=saga.start_time,
            end_time=saga.end_time,
            error_message=saga.error_message
        ))
    
    return result