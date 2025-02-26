from dataclasses import dataclass
from typing import Callable, Any
from progressivis.core.api import Module
import ipywidgets as ipw

@dataclass
class SnippetResult:
    output_module: Module
    output_slot: str
    widget: ipw.DOMWidget | None = None

CUSTOMER_FNC: dict[str, Callable[..., Any]] = {}
CUSTOMER_SNIPPET: dict[str, Callable[..., SnippetResult]] = {}


def register_function(fnc: Callable[..., Any]) -> Callable[..., Any]:
    CUSTOMER_FNC[fnc.__name__] = fnc
    return fnc

def register_snippet(fnc: Callable[..., SnippetResult]) -> Callable[..., SnippetResult]:
    CUSTOMER_SNIPPET[fnc.__name__] = fnc
    return fnc

