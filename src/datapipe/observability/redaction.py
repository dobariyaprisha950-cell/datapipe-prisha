"""Logging filter that redacts common credentials."""

import logging
import re

PATTERNS = (
    re.compile(
        r"(?i)(authorization|token|api[_-]?key|password|secret)(\s*[:=]\s*)(?:bearer\s+)?([^\s,;]+)"
    ),
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+"),
    re.compile(r"(?i)([?&](?:api[_-]?key|access[_-]?token|token|password)=)[^&#\s]+"),
    re.compile(r"(?i)(https?://)[^/@\s]+:[^/@\s]+@"),
)


def redact(value: str) -> str:
    value = PATTERNS[0].sub(lambda match: match.group(1) + match.group(2) + "[REDACTED]", value)
    value = PATTERNS[1].sub(lambda match: match.group(1) + "[REDACTED]", value)
    value = PATTERNS[2].sub(lambda match: match.group(1) + "[REDACTED]", value)
    value = PATTERNS[3].sub(lambda match: match.group(1) + "[REDACTED]", value)
    return value


class RedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage())
        record.args = ()
        if record.exc_info:
            record.exc_text = redact(logging.Formatter().formatException(record.exc_info))
            record.exc_info = None
        return True
