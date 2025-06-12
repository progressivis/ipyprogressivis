import ipywidgets as widgets
from traitlets import Unicode, Any
from .. _frontend import NPM_PACKAGE, NPM_PACKAGE_RANGE


@widgets.register
class KNNKernelEstimator(widgets.DOMWidget):
    """KNN kernel estimator"""

    # Name of the widget view class in front-end
    _view_name = Unicode("KNNKernelView").tag(sync=True)

    # Name of the widget model class in front-end
    _model_name = Unicode("KNNKernelModel").tag(sync=True)

    # Name of the front-end module containing widget view
    _view_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Name of the front-end module containing widget model
    _model_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Version of the front-end module containing widget view
    _view_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)
    # Version of the front-end module containing widget model
    _model_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)

    data = Any("{}").tag(sync=True)

