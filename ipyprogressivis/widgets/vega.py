import ipywidgets
from traitlets import Unicode

from typing import Any
import vega.widget as vw
from .. _frontend import NPM_PACKAGE, NPM_PACKAGE_RANGE


@ipywidgets.register
class VegaWidget(vw.VegaWidget):
    _view_module = Unicode(NPM_PACKAGE).tag(sync=True)
    _model_module = Unicode(NPM_PACKAGE).tag(sync=True)
    _view_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)
    _model_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)

    def __init__(self, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
