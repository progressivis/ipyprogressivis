from typing import Callable, Any


CUSTOMER_FNC = {}


def custom_function(fnc: Callable[..., Any]) -> Callable[..., Any]:
    CUSTOMER_FNC[fnc.__name__] = fnc
    return fnc

