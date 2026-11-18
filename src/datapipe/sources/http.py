"""HTTP JSON source with bounded retries and pagination."""

import asyncio
import os
import random
from collections.abc import AsyncIterator, Mapping
from typing import Any

import aiohttp

from datapipe.core.exceptions import SourceError


class HTTPSource:
    def __init__(
        self,
        name: str,
        url: str,
        timeout: float = 20,
        retries: int = 3,
        auth_env: str | None = None,
        page_param: str | None = None,
        start_page: int = 1,
        items_key: str | None = None,
    ) -> None:
        (
            self.name,
            self.url,
            self.timeout,
            self.retries,
            self.auth_env,
            self.page_param,
            self.start_page,
            self.items_key,
        ) = name, url, timeout, retries, auth_env, page_param, start_page, items_key
        self.cursor = start_page

    async def read(
        self, session: aiohttp.ClientSession | None = None
    ) -> AsyncIterator[Mapping[str, Any]]:
        own = session is None
        client = session or aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout))
        page = self.start_page
        try:
            while True:
                params = {self.page_param: page} if self.page_param else None
                headers = {}
                if self.auth_env:
                    token = os.getenv(self.auth_env)
                    if token:
                        headers["Authorization"] = f"Bearer {token}"
                for attempt in range(self.retries + 1):
                    try:
                        async with client.get(
                            self.url,
                            params=params,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=self.timeout),
                        ) as response:
                            if response.status == 429 or response.status >= 500:
                                if attempt >= self.retries:
                                    response.raise_for_status()
                                delay = float(
                                    response.headers.get(
                                        "Retry-After", min(30, 2**attempt + random.random())
                                    )
                                )
                                await asyncio.sleep(max(0, min(delay, 60)))
                                continue
                            response.raise_for_status()
                            payload = await response.json()
                        break
                    except aiohttp.ClientResponseError as exc:
                        if 400 <= exc.status < 500 and exc.status != 429:
                            raise SourceError(
                                f"{self.name}: permanent HTTP status {exc.status}"
                            ) from exc
                        if attempt >= self.retries:
                            raise
                        await asyncio.sleep(min(30, 2**attempt + random.random()))
                    except (TimeoutError, aiohttp.ClientError):
                        if attempt >= self.retries:
                            raise
                        await asyncio.sleep(min(30, 2**attempt + random.random()))
                if isinstance(payload, list):
                    items = payload
                elif isinstance(payload, dict):
                    items = payload.get(self.items_key, []) if self.items_key else [payload]
                else:
                    raise SourceError(f"{self.name}: expected JSON object or array")
                if not items:
                    break
                for item in items:
                    if isinstance(item, dict):
                        yield item
                    else:
                        yield {
                            "__datapipe_error__": {
                                "input": item,
                                "reason": "HTTP item must be a JSON object",
                            }
                        }
                if not self.page_param or len(items) == 0:
                    break
                page += 1
                self.cursor = page
        except (TimeoutError, aiohttp.ClientError, ValueError) as exc:
            raise SourceError(f"{self.name}: {type(exc).__name__}: {exc}") from exc
        finally:
            if own:
                await client.close()
