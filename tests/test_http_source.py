import asyncio
from collections.abc import Awaitable, Callable

import pytest
from aiohttp import web

from datapipe.core.exceptions import SourceError
from datapipe.sources.http import HTTPSource


async def with_server(
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    check: Callable[[str], Awaitable[None]],
) -> None:
    app = web.Application()
    app.router.add_get("/data", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]
    try:
        await check(f"http://127.0.0.1:{port}/data")
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_pagination_and_environment_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATAPIPE_TEST_TOKEN", "not-for-logs")
    seen: list[int] = []

    async def handler(request: web.Request) -> web.StreamResponse:
        assert request.headers["Authorization"] == "Bearer not-for-logs"
        page = int(request.query["page"])
        seen.append(page)
        return web.json_response([{"id": str(page), "date": "2026-01-01"}] if page <= 2 else [])

    async def check(url: str) -> None:
        source = HTTPSource("api", url, auth_env="DATAPIPE_TEST_TOKEN", page_param="page")
        result = [row async for row in source.read()]
        assert [row["id"] for row in result] == ["1", "2"]
        assert source.cursor == 3

    await with_server(handler, check)
    assert seen == [1, 2, 3]


@pytest.mark.asyncio
async def test_429_retry_after_is_honored() -> None:
    attempts = 0

    async def handler(_request: web.Request) -> web.StreamResponse:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return web.Response(status=429, headers={"Retry-After": "0"})
        return web.json_response([{"id": "ok", "date": "2026-01-01"}])

    async def check(url: str) -> None:
        source = HTTPSource("api", url, retries=1)
        assert len([row async for row in source.read()]) == 1

    await with_server(handler, check)
    assert attempts == 2


@pytest.mark.asyncio
async def test_permanent_http_error_is_not_retried() -> None:
    attempts = 0

    async def handler(_request: web.Request) -> web.StreamResponse:
        nonlocal attempts
        attempts += 1
        return web.Response(status=404)

    async def check(url: str) -> None:
        with pytest.raises(SourceError, match="permanent HTTP status 404"):
            _ = [row async for row in HTTPSource("api", url, retries=4).read()]

    await with_server(handler, check)
    assert attempts == 1


@pytest.mark.asyncio
async def test_timeout_is_reported() -> None:
    async def handler(_request: web.Request) -> web.StreamResponse:
        await asyncio.sleep(0.05)
        return web.json_response([])

    async def check(url: str) -> None:
        with pytest.raises(SourceError):
            _ = [row async for row in HTTPSource("slow", url, timeout=0.005, retries=0).read()]

    await with_server(handler, check)
