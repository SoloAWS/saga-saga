FROM python:3.10-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copiar los requerimientos e instalarlos
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY . .

# Exponer el puerto para la API
EXPOSE 8085

# Comando para ejecutar la aplicación
CMD ["python", "-m", "src.saga_service.main"]