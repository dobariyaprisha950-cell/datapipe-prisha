"""HTTP JSON source with bounded retries and pagination."""

import asyncio
import json
import os
import random
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp

from datapipe.core.exceptions import SourceError


def _retry_after_seconds(value: str | None, attempt: int) -> float:
    """Parse both delta-seconds and HTTP-date Retry-After values."""
    fallback: float = min(30.0, 2**attempt + random.random())
    if value is None:
        return fallback
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return fallback


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
        start_offset: int = 0,
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
        self.start_offset = start_offset
        self.cursor: dict[str, int] = {"page": start_page, "offset": start_offset}

    async def read(
        self, session: aiohttp.ClientSession | None = None
    ) -> AsyncIterator[Mapping[str, Any]]:
        own = session is None
        client = session or aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout))
        page = self.start_page
        offset = self.start_offset
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
                                delay = _retry_after_seconds(
                                    response.headers.get("Retry-After"), attempt
                                )
                                await asyncio.sleep(max(0, min(delay, 60)))
                                continue
                            response.raise_for_status()
                            body = (await response.read()).decode("utf-8", errors="replace")
                            try:
                                payload = json.loads(body)
                            except json.JSONDecodeError as exc:
                                payload = {
                                    "__datapipe_error__": {
                                        "input": body,
                                        "reason": f"malformed HTTP JSON: {exc}",
                                    }
                                }
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
                if isinstance(payload, dict) and set(payload) == {"__datapipe_error__"}:
                    items = [payload]
                elif isinstance(payload, list):
                    items = payload
                elif isinstance(payload, dict):
                    items = payload.get(self.items_key, []) if self.items_key else [payload]
                else:
                    raise SourceError(f"{self.name}: expected JSON object or array")
                if not items:
                    self.cursor = {"page": page, "offset": 0}
                    break
                for index, item in enumerate(items):
                    if index < offset:
                        continue
                    self.cursor = {"page": page, "offset": index + 1}
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
                offset = 0
                self.cursor = {"page": page, "offset": 0}
        except (TimeoutError, aiohttp.ClientError, ValueError) as exc:
            raise SourceError(f"{self.name}: {type(exc).__name__}: {exc}") from exc
        finally:
            if own:
                await client.close()
