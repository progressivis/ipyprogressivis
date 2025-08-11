# type: ignore

import ipywidgets as widgets
from traitlets import Unicode
from .. _frontend import NPM_PACKAGE, NPM_PACKAGE_RANGE

from ipytablewidgets import (serialization,
                             SourceAdapter,
                             PandasAdapter,
                             TableType)
from progressivis.table.api import BasePTable

class ProgressiVisAdapter(SourceAdapter):
    """
    Actually this adapter requires a dict of ndarrays
    """

    def __init__(self, source, *args, **kw):
        from progressivis.table.api import BasePTable
        assert source is None or isinstance(
            source, BasePTable
        )
        super().__init__(source, *args, **kw)

    @property
    def columns(self):
        return self._columns or self._source.columns

    def to_array(self, col):
        return self._source[col].values

    def equals(self, other):
        from progressivis.table.api import BasePTable
        if isinstance(other, SourceAdapter):
            other = other._source
        assert isinstance(other, BasePTable)
        return self._source.equals(other)


@widgets.register
class ContourDensity(widgets.DOMWidget):
    """Contour density"""

    # Name of the widget view class in front-end
    _view_name = Unicode("ContourDensityView").tag(sync=True)

    # Name of the widget model class in front-end
    _model_name = Unicode("ContourDensityModel").tag(sync=True)

    # Name of the front-end module containing widget view
    _view_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Name of the front-end module containing widget model
    _model_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Version of the front-end module containing widget view
    _view_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)
    # Version of the front-end module containing widget model
    _model_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)

    #data = Any("{}").tag(sync=True)
    compression = None
    _df = TableType(None).tag(sync=True, **serialization)

    def update(self, df, remove=None, resize=True):
        if isinstance(df, BasePTable):
            self._df = ProgressiVisAdapter(df, touch_mode=True)
        else:
            assert isinstance(df, PandasAdapter, touch_mode=True)
            self._df = df
        self._df.touch()

