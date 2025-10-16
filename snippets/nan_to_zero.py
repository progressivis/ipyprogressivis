# progressivis-snippet
import numpy as np

@register_function
def nan_to_zero(x: float) -> float:
    return 0. if np.isnan(x) else x

