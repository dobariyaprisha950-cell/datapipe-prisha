import logging
from datapipe.observability.redaction import RedactionFilter
def test_secret_redacted(caplog):
 logger=logging.getLogger("redaction-test");logger.addFilter(RedactionFilter())
 with caplog.at_level(logging.INFO):logger.info("token=supersecret")
 assert "supersecret" not in caplog.text and "[REDACTED]" in caplog.text
