import time
import math
from typing import Dict, Any as AnyType

import ipywidgets as widgets
from traitlets import Unicode, Any
from .. _frontend import NPM_PACKAGE, NPM_PACKAGE_RANGE

def sanitize(x: AnyType) -> AnyType:
    if isinstance(x, dict):
        return {k: sanitize(v) for (k, v) in x.items()}
    if isinstance(x, list):
        return [sanitize(elt) for elt in x]
    if isinstance(x, float) and math.isnan(x):
        return None
    return x

# See js/src/quality.js for the frontend counterpart to this file.


@widgets.register
class QualityVisualization(widgets.DOMWidget):
    """Quality visualization widget.

    """

    # Name of the widget view class in front-end
    _view_name = Unicode("QualityVisualizationView").tag(sync=True)

    # Name of the widget model class in front-end
    _model_name = Unicode("QualityVisualizationModel").tag(sync=True)

    # Name of the front-end module containing widget view
    _view_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Name of the front-end module containing widget model
    _model_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Version of the front-end module containing widget view
    _view_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)
    # Version of the front-end module containing widget model
    _model_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)
    # width = CFloat(300).tag(sync=True)
    width = Any(300).tag(sync=True)
    # height = CFloat(50).tag(sync=True)
    height = Any(50).tag(sync=True)
    # hack to save image in notebooks
    _img_url = Unicode('null').tag(sync=True)

    def update(self, measures: Dict[str, float], ts: float | None = None) -> None:
        """Update the visualization data.

        Add a list of measures, associating a string name to a float value
        """
        if ts is None:
            ts = time.perf_counter()
        self.send(dict(type="update", timestamp=ts, measures=sanitize(measures)))
