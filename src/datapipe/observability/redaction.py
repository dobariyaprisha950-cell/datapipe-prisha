"""Logging filter that redacts common credentials."""
import logging,re
PATTERNS=(
    re.compile(r"(?i)(authorization|token|api[_-]?key|password|secret)(\s*[:=]\s*)([^\s,;]+)"),
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+"),
    re.compile(r"(?i)([?&](?:api[_-]?key|access[_-]?token|token|password)=)[^&#\s]+"),
    re.compile(r"(?i)(https?://)[^/@\s]+:[^/@\s]+@"),
)
def redact(value:str)->str:
    for pattern in PATTERNS:
        value=pattern.sub(lambda match: (match.group(1) if match.lastindex else "")+"[REDACTED]",value)
    return value
class RedactionFilter(logging.Filter):
    def filter(self,record:logging.LogRecord)->bool:
        record.msg=redact(str(record.msg))
        if record.args:
            record.args=(tuple(redact(x) if isinstance(x,str) else x for x in record.args)
                         if isinstance(record.args,tuple)
                         else redact(record.args) if isinstance(record.args,str) else record.args)
        if record.exc_info:
            record.exc_text=redact(logging.Formatter().formatException(record.exc_info))
            record.exc_info=None
        return True
