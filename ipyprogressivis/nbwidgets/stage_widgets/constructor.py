import ipywidgets as ipw
from progressivis import Scheduler
from progressivis.io import Variable
from progressivis.core import Sink, aio
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
    bunpack,
    replay_list,
    replay_next,
    b642json,
    PARAMS,
    reset_recorder,
    restore_recorder
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
        play_mode_radio: ipw.RadioButtons
        record_ck: ipw.Checkbox
        csv: Optional[ipw.HBox]
        parquet: Optional[ipw.HBox]
        replay: ipw.Button

    last_created = None

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
        s = Scheduler.default = Scheduler()
        self.scheduler = s
        self._backup = backup
        PARAMS["constructor"] = self

    def _start_scheduler_cb(self, btn: ipw.Button) -> None:
        init_module = init_dataflow(self.scheduler)
        self._output_module = init_module
        self._output_slot = "result"
        self._output_dtypes = {}
        if self._backup.value:
            self.child.play_mode_radio = ipw.RadioButtons(
                options=[('Start a new scenario', 'play'),
                         ('Replay from recording', 'replay')
                         ],
                value='play',
                #    layout={'width': 'max-content'}, # If the items' names are long
                # description='Mode:',
                disabled=False
            )
            self.child.play_mode_radio.observe(self._play_mode_cb, names="value")
        self.child.record_ck = ipw.Checkbox(description="Record this scenario")
        if self._backup.value:
            self.child.record_ck.description += "(previous record will be lost)"
        self.child.record_ck.observe(self._record_cb, names="value")

        self.child.csv = self.make_loader_box(ftype="csv")
        self.child.parquet = self.make_loader_box(ftype="parquet")
        self._arch_list = [b642json(elt)
                           for elt in bunpack(self._backup.value)
                           ] if self._backup.value else []
        self.child.replay = make_button(
            "Replay ...", cb=self._replay_cb, disabled=True
        )
        btn.disabled = True
        self.dag.register_widget(self, "root", "root", self.dom_id, [])

    def _play_mode_cb(self, change: dict[str, AnyType]) -> None:
        PARAMS["play_mode"] = change["new"]
        is_replay = (change["new"] == 'replay')
        self.child.replay.disabled = not is_replay
        self.child.csv.children[-1].disabled = is_replay
        self.child.parquet.children[-1].disabled = is_replay
        if is_replay:
            self.child.record_ck.value = False
        self.child.record_ck.disabled = is_replay

    def _record_cb(self, change: dict[str, AnyType]) -> None:
        if change["new"]:
            reset_recorder(self._backup.value)
        else:
            restore_recorder()

    def _replay_cb(self, btn: ipw.Button) -> None:
        btn.disabled = True
        self.child.csv.children[-1].disabled = True
        self.child.parquet.children[-1].disabled = True
        self.child.record_ck.value = False
        self.child.record_ck.disabled = True
        self.child.play_mode_radio.disabled = True
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
