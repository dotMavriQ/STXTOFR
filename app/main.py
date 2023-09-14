from fastapi import FastAPI

from app.api.routes import api_router
from app.core.logging import configure_logging


configure_logging()

app = FastAPI(title="STXTOFR", version="0.1.0")
app.include_router(api_router)

