import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routes import events, head_to_head, results, session, standings, telemetry

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="F1 Telemetry API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.error("Unhandled error on %s: %s", request.url.path, exc, exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error"},
    )


app.include_router(standings.router, prefix="/api")
app.include_router(events.router, prefix="/api")
app.include_router(results.router, prefix="/api")
app.include_router(head_to_head.router, prefix="/api")
app.include_router(session.router, prefix="/api")
app.include_router(telemetry.router, prefix="/api")
