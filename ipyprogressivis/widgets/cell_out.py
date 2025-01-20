import ipywidgets as widgets
from traitlets import Unicode
from .. _frontend import NPM_PACKAGE, NPM_PACKAGE_RANGE
from typing import Any as AnyType
# See js/lib/widgets.js for the frontend counterpart to this file.


@widgets.register
class CellOut(widgets.DOMWidget):
    """
    Progressivis CellOut widget.
    It displays a png image previously saved
    in progressivis_outs list from notebook metadata
    """
    # Name of the widget view class in front-end
    _view_name = Unicode("CellOutView").tag(sync=True)

    # Name of the widget model class in front-end
    _model_name = Unicode("CellOutModel").tag(sync=True)

    # Name of the front-end module containing widget view
    _view_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Name of the front-end module containing widget model
    _model_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Version of the front-end module containing widget view
    _view_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)
    # Version of the front-end module containing widget model
    _model_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)

    tag = Unicode("").tag(sync=True)
    def __init__(self, tag: Unicode, *args: AnyType, **kw: AnyType) -> None:
        widgets.DOMWidget.__init__(self, *args, **kw)
        self.tag = tag
