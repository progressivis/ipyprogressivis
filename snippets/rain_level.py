# progressivis-snippets
import numpy as np
@register_function
@np.vectorize
def rain_level(val: float) -> str:
    if np.isnan(val) or val < 0.07:
        return "No"
    if val < 0.19:
        return "Light"
    return "Rain"
