# Saga Service

Servicio de monitoreo y gestión del flujo de procesamiento de imágenes médicas a través del patrón Saga. Este servicio registra todos los eventos importantes del proceso de carga, anonimización y procesamiento de imágenes, permitiendo el seguimiento de transacciones de larga duración y manejando compensaciones cuando algo falla.

## Características principales

- Registro de Saga Logs completos en base de datos PostgreSQL
- Detección de fallos en cualquier etapa del procesamiento
- Mecanismo de compensación automática para deshacer cambios cuando algo falla
- API REST para consulta de logs y estado
- Comunicación asíncrona con Apache Pulsar

## Estructura del proyecto

```
src/saga_service/
│
├── api/                                  # APIs REST para consulta de logs y estado
│   ├── __init__.py
│   └── v1/
│       ├── __init__.py
│       ├── routes.py                     # Endpoints para consultas
│       └── schemas.py                    # Esquemas Pydantic
│
├── config/                               # Configuración
│   ├── __init__.py
│   ├── database.py                       # Configuración de la BD
│   ├── dependencies.py                   # Inyección de dependencias
│   └── settings.py                       # Variables de entorno
│
├── core/                                 # Utilidades compartidas
│   ├── __init__.py
│   └── saga_coordinator.py               # Coordinador principal de sagas
│
├── domain/                               # Dominio y objetos de valor
│   ├── __init__.py
│   ├── entities.py                       # Entidades como SagaLog
│   └── enums.py                          # Enumeraciones: SagaStatus, StepStatus
│
├── infrastructure/                       # Infraestructura
│   ├── __init__.py
│   ├── commands/                         # Comandos para compensación
│   │   ├── __init__.py
│   │   ├── anonymization_commands.py     # Comandos para Anonymization
│   │   └── data_retrieval_commands.py    # Comandos para Data Retrieval
│   │
│   ├── messaging/                        # Mensajería Pulsar
│   │   ├── __init__.py
│   │   ├── pulsar_consumer.py            # Consumidor de eventos
│   │   └── pulsar_publisher.py           # Publicador de comandos
│   │
│   └── repositories/                     # Repositorios
│       ├── __init__.py
│       ├── saga_log_repository.py        # Repositorio para SagaLog
│       └── dto.py                        # DTOs para ORM
│
├── services/                             # Servicios
│   ├── __init__.py
│   ├── data_retrieval_listener.py        # Escucha eventos Data Retrieval
│   ├── anonymization_listener.py         # Escucha eventos Anonymization
│   ├── processing_listener.py            # Escucha eventos Processing
│   └── compensation_service.py           # Coordina compensaciones
│
└── main.py                               # Punto de entrada
```

## Funcionamiento del patrón Saga

El servicio implementa el patrón Saga en formato de coreografía para gestionar transacciones distribuidas. Funciona de la siguiente manera:

1. **Escucha de eventos**: El servicio escucha eventos de todos los microservicios a través de Apache Pulsar.
2. **Registro de saga logs**: Se mantiene un registro completo de cada transacción, incluidos todos sus pasos.
3. **Detección de fallos**: Cuando se detecta un evento de fallo, se identifica la saga correspondiente.
4. **Compensación**: En caso de fallo, se inicia un proceso de compensación para revertir los cambios realizados en pasos anteriores.
5. **Monitoreo**: A través de la API REST, se puede consultar el estado de las sagas y obtener detalles de los pasos.

## Eventos monitoreados

El servicio escucha los siguientes eventos de los microservicios:

### Data Retrieval
- RetrievalStarted
- RetrievalCompleted
- RetrievalFailed
- ImagesRetrieved
- ImageUploadFailed
- ImageReadyForAnonymization

### Anonymization
- AnonymizationRequested
- AnonymizationCompleted
- AnonymizationFailed
- ImageReadyForProcessing
- AnonymizationRolledBack

### Processing
- ProcessingStarted
- ProcessingCompleted
- ProcessingFailed

## Comandos de compensación

El servicio envía los siguientes comandos de compensación cuando es necesario:

- **DeleteRetrievedImage**: Elimina una imagen recuperada en caso de fallo.
- **RollbackAnonymization**: Revierte el proceso de anonimización.

## Configuración del entorno

Variables de entorno necesarias:

- `DATABASE_URL`: URL de conexión a la base de datos PostgreSQL
- `PULSAR_SERVICE_URL`: URL del servicio Pulsar
- `PULSAR_TOKEN`: Token de autenticación para Pulsar (opcional)
- `ENVIRONMENT`: Entorno de ejecución (dev, prod)
- `LOG_LEVEL`: Nivel de logging (INFO, DEBUG, etc.)
- `API_HOST`: Host para la API
- `API_PORT`: Puerto para la API
- `API_RELOAD`: Habilitar recarga automática en desarrollo

## Endpoints de la API

El servicio expone los siguientes endpoints:

- `GET /api/v1/saga/health`: Verificación de estado
- `GET /api/v1/saga/sagas`: Consulta paginada de sagas con filtros
- `GET /api/v1/saga/sagas/{saga_id}`: Detalles de una saga específica
- `GET /api/v1/saga/sagas/by-correlation/{correlation_id}`: Busca saga por ID de correlación
- `GET /api/v1/saga/sagas/by-task/{task_id}`: Busca sagas relacionadas con una tarea

## Ejecución

### Con Docker

```bash
docker build -t saga-service .
docker run -p 8085:8085 --env-file .env saga-service
```

### Local

```bash
# Instalar dependencias
pip install -r requirements.txt

# Ejecutar el servicio
python -m src.saga_service.main
```

## Integración con otros servicios

Este servicio se integra con:
- **Data Retrieval Service**: Para monitorear la recuperación de imágenes.
- **Anonymization Service**: Para monitorear el proceso de anonimización.
- **Processing Service**: Para monitorear el procesamiento de imágenes.

## Licencia

Confidencial © SaludTech de los Alpes 2025