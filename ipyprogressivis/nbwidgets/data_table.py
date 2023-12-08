import ipywidgets as widgets
from traitlets import Unicode, Any
from .._version import NPM_PACKAGE, NPM_PACKAGE_RANGE

# See js/lib/widgets.js for the frontend counterpart to this file.


@widgets.register
class DataPTable(widgets.DOMWidget):
    """Progressivis DataPTable widget."""

    # Name of the widget view class in front-end
    _view_name = Unicode("DataTableView").tag(sync=True)

    # Name of the widget model class in front-end
    _model_name = Unicode("DataTableModel").tag(sync=True)

    # Name of the front-end module containing widget view
    _view_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Name of the front-end module containing widget model
    _model_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Version of the front-end module containing widget view
    _view_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)
    # Version of the front-end module containing widget model
    _model_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)
    data = Unicode("").tag(sync=True)
    columns = Unicode("").tag(sync=True)
    page = Any({}).tag(sync=True)
    dt_id = Unicode("aDT").tag(sync=True)
