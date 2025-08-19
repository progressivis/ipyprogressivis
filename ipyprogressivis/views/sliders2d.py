import ipywidgets as ipw
from .util import PView
import progressivis.core.aio as aio
from progressivis.core.api import Module
from progressivis.io.api import Variable
from progressivis.table.api import RangeQuery2D, BinningIndexND
from typing import Any as AnyType, cast

WidgetType = AnyType

wg = ipw.VBox([ipw.Label(), ipw.Label()])

def make_slider(label: str, min_: float, max_: float) -> ipw.FloatRangeSlider:
    slider = ipw.FloatRangeSlider(
        value=[min_, max_],
        min=min_,
        max=max_,
        step=(max_-min_)/10,
        description=label,
        disabled=False,
        continuous_update=False,
        orientation='horizontal',
        readout=True,
        readout_format='.1f',
        layout={"width": "initial"},
    )
    return slider


class SlidersView(PView):
    def connect_module(self, m: Module) -> None:
        assert isinstance(m, RangeQuery2D)
        var_min = cast(Variable, m.get_input_slot("lower").output_module)
        var_max = cast(Variable, m.get_input_slot("upper").output_module)
        bindexnd = cast(BinningIndexND, m.get_input_slot("index").output_module)
        col_x = m._column_x
        col_y = m._column_y
        perc_x = bindexnd.compute_percentiles(col_x, dict(min=3.0, max=97.0), 0.5)
        minv_x = perc_x["min"]
        maxv_x = perc_x["max"]
        perc_y = bindexnd.compute_percentiles(col_y, dict(min=3.0, max=97.0), 0.5)
        minv_y = perc_y["min"]
        maxv_y = perc_y["max"]
        x_slider = make_slider("X:", minv_x, maxv_x)
        y_slider = make_slider("Y:", minv_y, maxv_y)
        assert isinstance(self._widget, ipw.Box)
        self._widget.children = (x_slider, y_slider)
        #bnds_min = PDict({col_x: bounds.left, col_y: bounds.bottom})
        #bnds_max = PDict({col_x: bounds.right, col_y: bounds.top})

        def observer(_: AnyType) -> None:
            async def _coro() -> None:
                x_min, x_max = x_slider.value
                y_min, y_max = y_slider.value
                await var_min.from_input({col_x: x_min, col_y: y_min})
                await var_max.from_input({col_x: x_max, col_y: y_max})
            aio.create_task(_coro())
        x_slider.observe(observer, "value")
        y_slider.observe(observer, "value")
        aio.create_task(var_min.from_input({col_x: minv_x, col_y: minv_y}))
        aio.create_task(var_max.from_input({col_x: maxv_x, col_y: maxv_y}))


_ = SlidersView("Sliders", wg)
