"""Domain exceptions."""
class DatapipeError(Exception):
    """Base operational exception."""
class ConfigurationError(DatapipeError):
    """Invalid configuration."""
class SourceError(DatapipeError):
    """Source could not produce records."""
class SinkError(DatapipeError):
    """Output could not be committed."""
