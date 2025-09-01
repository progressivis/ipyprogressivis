from typing import List

from progressivis.core.api import Module
from ipyprogressivis.widgets.quality_visualization import QualityVisualization


def display_quality(
    mods: Module | List[Module], period: float = 3
) -> QualityVisualization:
    qv = QualityVisualization()
    last: float = 0  # show immediately
    if isinstance(mods, Module):
        mods = [mods]

    async def _after_run(m: Module, run_number: int) -> None:
        nonlocal last
        now = m.timer()
        if (now - last) < period:
            return
        last = now
        for m in mods:
            measures = m.get_quality()
            if measures is None:
                continue
            qv.update(measures, m.timer())

    for mod in mods:
        mod.on_after_run(_after_run)
    return qv
