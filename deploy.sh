#!/bin/bash
# Configure environment variables
export PROJECT_ID="sologcp"
export APP_NAME="saga-saga-app"
export PORT=8080
export REGION="us-central1"
export IMAGE_TAG="gcr.io/$PROJECT_ID/$APP_NAME"

# Read environment variables from .env file
if [ -f .env ]; then
  source .env
else
  echo "Error: .env file not found"
  exit 1
fi

# Set default project
gcloud config set project $PROJECT_ID

# Enable necessary services
gcloud services enable cloudbuild.googleapis.com \
    containerregistry.googleapis.com \
    run.googleapis.com

# Build Docker image and push to Google Container Registry
gcloud builds submit --tag $IMAGE_TAG

# Deploy application to Google Cloud Run with environment variables
gcloud run deploy $APP_NAME \
    --image $IMAGE_TAG \
    --platform managed \
    --region $REGION \
    --port $PORT \
    --set-env-vars="ENVIRONMENT=${ENVIRONMENT},LOG_LEVEL=${LOG_LEVEL},DB_HOST=${DB_HOST},DB_PORT=${DB_PORT},DB_USER=${DB_USER},DB_PASSWORD=${DB_PASSWORD},DB_NAME=${DB_NAME},API_HOST=${API_HOST},API_PORT=${API_PORT},API_RELOAD=${API_RELOAD},PULSAR_SERVICE_URL=${PULSAR_SERVICE_URL},PULSAR_TOKEN=${PULSAR_TOKEN}" \
    --allow-unauthenticated

echo "Deployment complete!" 