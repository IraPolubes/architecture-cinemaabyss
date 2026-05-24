import logging
import os
import random

import httpx
from fastapi import FastAPI, Request, Response


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("proxy-service")

app = FastAPI(title="CinemaAbyss Proxy Service")

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")

GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"

try:
    MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))
except ValueError:
    MOVIES_MIGRATION_PERCENT = 0

MOVIES_MIGRATION_PERCENT = max(0, min(100, MOVIES_MIGRATION_PERCENT))


@app.get("/health")
async def health():
    return {
        "status": True,
        "service": "proxy-service",
        "gradual_migration": GRADUAL_MIGRATION,
        "movies_migration_percent": MOVIES_MIGRATION_PERCENT,
    }


def choose_movies_target() -> str:
    """
    Applies Strangler Fig feature-flag routing for /api/movies.

    GRADUAL_MIGRATION=false:
        100% of /api/movies traffic goes to the monolith.

    GRADUAL_MIGRATION=true:
        MOVIES_MIGRATION_PERCENT controls how much /api/movies traffic
        goes to the Movies Service.
    """
    if not GRADUAL_MIGRATION:
        return MONOLITH_URL

    if MOVIES_MIGRATION_PERCENT <= 0:
        return MONOLITH_URL

    if MOVIES_MIGRATION_PERCENT >= 100:
        return MOVIES_SERVICE_URL

    if random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
        return MOVIES_SERVICE_URL

    return MONOLITH_URL


def choose_target(path: str) -> str:
    """
    Selects the upstream service for the request.

    Feature-flag routing is applied only to /api/movies routes.
    All other routes stay on the monolith.
    """
    if path.startswith("/api/movies"):
        return choose_movies_target()

    return MONOLITH_URL


async def proxy_request(request: Request, target_base_url: str) -> Response:
    target_url = f"{target_base_url}{request.url.path}"

    if request.url.query:
        target_url += f"?{request.url.query}"

    body = await request.body()

    headers = dict(request.headers)
    headers.pop("host", None)

    async with httpx.AsyncClient(timeout=30.0) as client:
        proxied_response = await client.request(
            method=request.method,
            url=target_url,
            headers=headers,
            content=body,
        )

    excluded_headers = {
        "content-encoding",
        "transfer-encoding",
        "connection",
    }

    response_headers = {
        key: value
        for key, value in proxied_response.headers.items()
        if key.lower() not in excluded_headers
    }

    response_headers["X-Proxied-To"] = target_base_url

    return Response(
        content=proxied_response.content,
        status_code=proxied_response.status_code,
        headers=response_headers,
        media_type=proxied_response.headers.get("content-type"),
    )

# в задании проверка в тестах на health, movies поэтому делаем общий вид эндпойнта
@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def catch_all(request: Request, full_path: str):
    path = "/" + full_path
    target = choose_target(path)

    logger.info("%s %s -> %s", request.method, path, target)

    return await proxy_request(request, target)