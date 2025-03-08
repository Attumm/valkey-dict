"""__init__ module for valkey dict."""
from importlib.metadata import version, PackageNotFoundError

from .core import ValkeyDict
from .python_dict import PythonValkeyDict
from .type_management import decoding_registry, encoding_registry, ValkeyDictJSONEncoder, ValkeyDictJSONDecoder

__all__ = [
    'ValkeyDict',
    'PythonValkeyDict',
    'decoding_registry',
    'encoding_registry',
    'ValkeyDictJSONEncoder',
    'ValkeyDictJSONDecoder',
]
try:
    __version__ = version("valkey-dict")
except PackageNotFoundError:
    __version__ = "0.0.0"
