from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.routes import api_router, public_router
from app.core.logging import configure_logging
from app.core.config import get_settings
from app.storage.db import init_db


configure_logging()

app = FastAPI(title="STXTOFR", version="0.1.0")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(public_router)
app.include_router(api_router)


@app.on_event("startup")
def on_startup() -> None:
    if get_settings().repository_backend != "memory":
        init_db()
