from fastapi import APIRouter

# Import versioned routers
from .v1.routes import router as v1_router

# Create the main API router
api_router = APIRouter()

# Include versioned routers
api_router.include_router(v1_router, prefix="/v1")