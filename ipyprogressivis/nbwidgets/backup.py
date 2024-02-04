import ipywidgets as widgets
from traitlets import Unicode
from .. _frontend import NPM_PACKAGE, NPM_PACKAGE_RANGE
import json
from typing import Any


@widgets.register
class BackupWidget(widgets.DOMWidget):
    """A backup widget."""

    # Name of the widget view class in front-end
    _view_name = Unicode('BackupWidgetView').tag(sync=True)

    # Name of the widget model class in front-end
    _model_name = Unicode('BackupWidgetModel').tag(sync=True)

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
    _value = Unicode("").tag(sync=True)
    _previous = Unicode("").tag(sync=True)

    def save_context(self, ctx: str, val: str) -> None:
        tmp = json.loads(self._value)  # type: ignore
        tmp[ctx] = val
        self._value = json.dumps(tmp)  # type: ignore

    def get_saved_context(self, ctx: str, default: dict[str, Any] = {}) -> None:
        return json.loads(self._previous).get(ctx, default)  # type: ignore
