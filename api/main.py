from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import events, head_to_head, results, session, standings, telemetry

app = FastAPI(title="F1 Telemetry API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(standings.router, prefix="/api")
app.include_router(events.router, prefix="/api")
app.include_router(results.router, prefix="/api")
app.include_router(head_to_head.router, prefix="/api")
app.include_router(session.router, prefix="/api")
app.include_router(telemetry.router, prefix="/api")
