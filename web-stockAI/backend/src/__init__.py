import logging
import os

from fastapi import FastAPI
from .api.router import router
from .db import db_instance
from fastapi.middleware.cors import CORSMiddleware

log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.basicConfig(
    level=log_level,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

app = FastAPI()

# --- Thêm CORS middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="")

@app.on_event("startup")
async def startup_event():
    from .api.routers.stocks import manager
    db_instance.connect()
    manager.start_background_cdc()


@app.on_event("shutdown")
async def shutdown_event():
    from .api.routers.stocks import manager
    await manager.shutdown_background_cdc()
    db_instance.close()