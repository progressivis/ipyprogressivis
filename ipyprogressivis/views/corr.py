from ..widgets.vega import VegaWidget
from ..widgets._corr_schema import corr_spec_no_data
from .util import PView
from progressivis.stats.api import Corr
from itertools import product
from progressivis.core.api import Module
from typing import (
    Any as AnyType,
    Sequence,
    cast,
)
WidgetType = AnyType

def corr_as_vega_dataset(
    mod: Corr, columns: Sequence[str] | None = None
) -> Sequence[dict[str, AnyType]]:
    """ """
    if columns is None:
        columns = mod.columns
        assert columns

    def _c(kx: str, ky: str) -> float:
        assert mod.result is not None
        res: dict[AnyType, float] = cast(dict[AnyType, float], mod.result)
        try:
            return res[(kx, ky)]
        except KeyError:
            return res[(ky, kx)]
    return [
        dict(corr=_c(kx, ky),
             corr_label=f"{_c(kx, ky):.2f}",
             var=kx,
             var2=ky)
        for (kx, ky) in product(columns, columns)
    ]

class CorrView(PView):
    async def action(self, m: Module, run_number: int) -> None:
        assert isinstance(m, Corr)
        cols = m.columns
        dataset = corr_as_vega_dataset(m, cols)
        self._widget.update("data", remove="true", insert=dataset)  # type: ignore
        m.updated_once = True  # type: ignore

_ = CorrView("Corr", VegaWidget(spec=corr_spec_no_data))  # type: ignore
