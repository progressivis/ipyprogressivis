import ipywidgets as widgets
from traitlets import Unicode
from .. _frontend import NPM_PACKAGE, NPM_PACKAGE_RANGE

from typing import Any


@widgets.register
class Talker(widgets.DOMWidget):
    """A backup widget."""

    # Name of the widget view class in front-end
    _view_name = Unicode('TalkerView').tag(sync=True)

    # Name of the widget model class in front-end
    _model_name = Unicode('TalkerModel').tag(sync=True)

    # Name of the front-end module containing widget view
    _view_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Name of the front-end module containing widget model
    _model_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Version of the front-end module containing widget view
    _view_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)
    # Version of the front-end module containing widget model
    _model_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)
    # Widget specific property.
    # Widget properties are defined as traitlets. Any property tagged with `sync=True`
    # is automatically synced to the frontend *any* time it changes in Python.
    # It is synced back to Python from the frontend *any* time the model is touched.

    def labcommand(self, cmd: str, kw: dict[str, Any] = {}) -> None:
        self.send(dict(cmd=cmd, **kw.copy()))
