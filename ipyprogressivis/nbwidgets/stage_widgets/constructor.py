import ipywidgets as ipw
from progressivis import Scheduler
from progressivis.io import Variable
from progressivis.core import Sink, aio
from ..utils import get_backup_content
from .utils import (
    make_button,
    set_dag,
    _Dag,
    DAGWidget,
    RootVBox,
    TypedBox,
    NodeVBox,
    TypedBase,
    get_widget_by_id,
    get_widget_by_key,
    backup_widget,
    bunpack,
    replay_list,
    replay_next,
    b642json,
    PARAMS,
    reset_backup

)

from typing import (
    Any as AnyType,
    Optional,
    List,
)


async def _wake_up(sc: Scheduler, sec: float) -> None:
    while True:
        if sc._stopped:
            return
        await aio.sleep(sec)
        await sc.wake_up()


def init_dataflow(s: Scheduler) -> AnyType:
    with s:
        dyn = Variable(scheduler=s)
        sink = Sink(scheduler=s)
        sink.input.inp = dyn.output.result
    s.task_start()
    aio.create_task(_wake_up(s, 3))
    return sink


class Constructor(RootVBox, TypedBox):
    class Typed(TypedBase):
        h2: ipw.HTML
        start_btn: ipw.Button
        recording_ck: ipw.Checkbox
        csv: Optional[ipw.HBox]
        parquet: Optional[ipw.HBox]
        replay: ipw.Button

    last_created = None
    archive = get_backup_content()

    def __init__(
            self,
            dag: DAGWidget,
            urls: List[str] = [],
            *,
            backup: AnyType = None,
            name: str = "root",
            to_sniff: Optional[str] = None,
    ) -> None:
        set_dag(dag)
        ctx = dict(
            parent=None,
            dtypes=None,
            input_module=None,
            input_slot=None,
            dag=_Dag(label=name, number=0, dag=dag),
        )
        RootVBox.__init__(self, ctx)
        TypedBox.__init__(self)
        self.child.start_btn = make_button(
            "Start scheduler ...", cb=self._start_scheduler_cb
        )
        self.child.h2 = ipw.HTML(f"<h2 id='{self.dom_id}'>{name}</h2>")
        assert not backup_widget  # == []
        backup_widget.append(backup)
        s = Scheduler.default = Scheduler()
        self.scheduler = s
        PARAMS["constructor"] = self

    def _start_scheduler_cb(self, btn: ipw.Button) -> None:
        init_module = init_dataflow(self.scheduler)
        self._output_module = init_module
        self._output_slot = "result"
        self._output_dtypes = {}
        self.child.recording_ck = ipw.Checkbox(description="Recording ...")
        self.child.recording_ck.observe(self._recording_cb, names="value")
        self.child.csv = self.make_loader_box(ftype="csv")
        self.child.parquet = self.make_loader_box(ftype="parquet")
        self._arch_list = [b642json(elt)
                           for elt in bunpack(Constructor.archive)
                           ] if Constructor.archive else []
        self.child.replay = make_button(
            "Replay ...", cb=self._replay_cb, disabled=(not self._arch_list)
        )
        btn.disabled = True
        self.dag.register_widget(self, "root", "root", self.dom_id, [])

    def _recording_cb(self, change: dict[str, AnyType]) -> None:
        if change["new"]:
            PARAMS["recording"] = True
            self.child.recording_ck.disabled = True
            self.child.replay.disabled = True
            reset_backup()
        else:
            raise ValueError("Recording cannot be disabled")

    def _replay_cb(self, btn: ipw.Button) -> None:
        btn.disabled = True
        replay_list.clear()
        replay_list.extend(self._arch_list)
        replay_list.append({})  # end of tape marker
        replay_next(self)

    @staticmethod
    def widget_by_id(key: int) -> NodeVBox:
        return get_widget_by_id(key)

    @staticmethod
    def widget(key: str, num: int = 0) -> NodeVBox:
        return get_widget_by_key(key, num)

    @property
    def dom_id(self) -> str:
        return f"prog_{id(self)}"

    @property
    def _frame(self) -> int:
        return 1

    def dag_register(self) -> None:
        pass
