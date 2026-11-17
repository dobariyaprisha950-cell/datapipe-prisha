"""Discover third-party source providers."""
from importlib.metadata import entry_points
from datapipe.plugins.registry import SourceRegistry
def discover()->list[str]:
    for entry in entry_points(group="datapipe.sources"):
        if entry.name not in SourceRegistry.names():
            provider=entry.load(); SourceRegistry.register(entry.name,provider)
    return SourceRegistry.names()
