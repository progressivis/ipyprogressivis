import ipywidgets
from traitlets import Unicode

from typing import Any
import vega.widget as vw

@ipywidgets.register
class VegaWidget(vw.VegaWidget):
    _view_module = Unicode("jupyter-progressivis").tag(sync=True)
    _model_module = Unicode("jupyter-progressivis").tag(sync=True)
    _view_module_version = Unicode("^0.1.0").tag(sync=True)
    _model_module_version = Unicode("^0.1.0").tag(sync=True)

    def __init__(self, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)