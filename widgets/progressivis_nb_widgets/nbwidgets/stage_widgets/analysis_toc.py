import ipywidgets as ipw
import pandas as pd
from progressivis.io.csv_sniffer import CSVSniffer
from progressivis import Scheduler
from progressivis.io import SimpleCSVLoader
from progressivis.table import Table
from progressivis.table.constant import Constant
from progressivis.core import Sink
from .utils import (make_button, make_chaining_box,
                    set_child, dongle_widget, get_schema, ChainingWidget,
                    get_widget_by_id)

from typing import (
    Any as AnyType,
    Optional,
    Dict,
    List,
    Callable,
)


def reset_all(urls: List[str], **sniffer_kwds) -> SimpleCSVLoader:
    s = Scheduler.default = Scheduler()
    with s:
        filenames = pd.DataFrame({'filename': urls})
        cst = Constant(Table('filenames', data=filenames), scheduler=s)
        csv = SimpleCSVLoader(scheduler=s, throttle=100, **sniffer_kwds)
        csv.input.filenames = cst.output[0]
        sink = Sink(scheduler=s)
        sink.input.inp = csv.output.result
        s.task_start()
        return csv


def make_start_loader(obj: "AnalysisToC") -> Callable:
    def _cbk(btn: ipw.Button) -> None:
        params = obj.sniffer.params
        csv_module = reset_all(obj._urls, **params)
        obj._output_module = csv_module
        obj._output_slot = "result"
        set_child(obj, 2, make_chaining_box(obj))
        btn.disabled = True
    return _cbk


class AnalysisToC(ipw.VBox, ChainingWidget):
    last_created = None

    def __init__(self, urls: List[str], *,
                 to_sniff: Optional[str] = None) -> None:
        super().__init__(frame=1,
                         dtypes=None,
                         input_module=None,
                         input_slot=None)
        assert urls
        assert isinstance(urls, list)
        if to_sniff is None:
            to_sniff = urls[0]
        self._urls = urls
        self._to_sniff = to_sniff
        assert isinstance(to_sniff, str)
        self.sniffer = CSVSniffer(to_sniff)
        start_btn = make_button("Start loading csv ...",
                                cb=make_start_loader(self))
        self.children = [
            self.sniffer.box,
            start_btn,
            dongle_widget("")
        ]

    @property
    def _output_dtypes(self) -> Dict[str, AnyType]:
        return get_schema(self.sniffer)

    @staticmethod
    def widget_by_id(key):
        return get_widget_by_id(key)