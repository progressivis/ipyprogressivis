from ..widgets.vega import VegaWidget
from ..widgets._hist2d_schema import hist2d_spec_no_data
import numpy as np
from .util import PView
from progressivis.stats.api import Histogram2D

from progressivis.core.api import Module
from typing import Any as AnyType

WidgetType = AnyType



class VegaMapView(PView):
    async def action(self, m: Module, run_number: int) -> None:
        if not m:
            return
        assert isinstance(m, Histogram2D)
        assert m.result is not None
        last = m.result.last()
        assert last
        res = last.to_dict()
        arr = res["array"]
        maxa = arr.max()
        if maxa != 0:
            hist = np.cbrt(arr/maxa)
        else:
            hist = maxa
        self._widget.update("data", remove="true", insert=hist)  # type: ignore


_ = VegaMapView("HeatmapVega", VegaWidget(spec=hist2d_spec_no_data))  # type: ignore
