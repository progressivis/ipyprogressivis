import ipywidgets as ipw
from progressivis.io.api import Variable
from progressivis.core.api import Sink, Scheduler
import progressivis.core.aio as aio
import asyncio
from .utils import (
    make_button,
    set_dag,
    _Dag,
    Proxy,
    DAGWidget,
    RootVBox,
    TypedBox,
    NodeCarrier,
    TypedBase,
    get_widget_by_id,
    get_widget_by_key,
    bunpack,
    replay_list,
    replay_next,
    replay_sequence,
    b642json,
    PARAMS,
    reset_recorder,
    set_recording_state,
    IpyHBoxTyped,
    IpyVBoxTyped
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


class BtnBar(IpyHBoxTyped):
    class Typed(TypedBase):
        replay: ipw.Button
        resume: ipw.Button
        sbs: ipw.Button


class LoadBlock(IpyVBoxTyped):
    class Typed(TypedBase):
        csv: ipw.HBox
        parquet: ipw.HBox
        custom: ipw.HBox


START_DELAY: int = 2


class Constructor(RootVBox, TypedBox):
    class Typed(TypedBase):
        start_msg: ipw.IntProgress
        allow_overwrite: ipw.Checkbox
        record_ck: ipw.Checkbox
        loader: LoadBlock
        btnbar: BtnBar

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
        self.child.start_msg = ipw.IntProgress(
            description="Starting ProgressiVis ...",
            min=0,
            max=START_DELAY,
            value=0,
            style={"description_width": "initial"}
        )
        self.child.btnbar = BtnBar()
        self.child.loader = LoadBlock()
        s = Scheduler.default = Scheduler()
        self.scheduler = s
        self._backup = backup
        PARAMS["constructor"] = self
        PARAMS["is_replay"] = False
        PARAMS["replay_before_resume"] = False
        PARAMS["step_by_step"] = False
        PARAMS["deleted_stages"] = set()
        PARAMS["command_list"] = []
        set_recording_state(False)
        self._do_record = not self._backup.value

    def _allow_overwrite_cb(self, change: dict[str, AnyType]) -> None:
        if change["new"]:
            self.make_loaders()
        else:
            self.c_.loader = LoadBlock()
        self._do_record = change["new"]
        self.child.btnbar.child.resume.disabled = not change["new"]

    def _start_scheduler_cb(self, btn: ipw.Button | None = None) -> None:
        init_module = init_dataflow(self.scheduler)
        self._output_module = init_module
        self._output_slot = "result"
        self._output_dtypes = {}
        if self._backup.value:
            self.child.allow_overwrite = ipw.Checkbox(description="Allow overwriting"
                                                      " record")
            self.child.allow_overwrite.observe(self._allow_overwrite_cb, names="value")
            self.child.btnbar.child.replay = make_button(
                "Replay ...", cb=self._replay_cb, disabled=False
            )
            self.child.btnbar.child.resume = make_button(
                "Resume ...", cb=self._resume_cb, disabled=not (
                    self.child.allow_overwrite.value
                )
            )
            self.child.btnbar.child.sbs = make_button(
                "Step by step", cb=self._step_by_step_cb, disabled=False
            )
        else:  # no backup => new scenario
            self.child.record_ck = ipw.Checkbox(description="Record this scenario",
                                                value=True,
                                                disabled=False)
            self.child.record_ck.observe(self._record_cb, names="value")
            self.make_loaders()
        self._arch_list = [b642json(elt)
                           for elt in bunpack(self._backup.value)
                           ] if self._backup.value else []
        self.dag.register_widget(self, "root", "root", self.dom_id, [])
        if btn is not None:
            btn.disabled = True

    def _record_cb(self, change: dict[str, AnyType]) -> None:
        self._do_record = change["new"]

    def make_loaders(self) -> None:
        self.c_.loader.c_.csv = self.make_loader_box(ftype="csv", disabled=False)
        self.c_.loader.c_.parquet = self.make_loader_box(ftype="parquet",
                                                         disabled=False)
        self.c_.loader.c_.custom = self.make_loader_box(ftype="custom",
                                                        disabled=False)

    def start_scheduler(self, n: int = 3) -> None:
        async def _func() -> None:
            for i in range(START_DELAY):
                await aio.sleep(0.1)
                self.child.start_msg.value = i + 1
            self._start_scheduler_cb()
            self.child.start_msg.description = "ProgressiVis started"
        loop = asyncio.get_event_loop()
        loop.create_task(_func())

    def do_replay(self, batch: bool = False) -> None:
        PARAMS["is_replay"] = True
        replay_list.clear()
        replay_list.extend(self._arch_list)
        replay_list.append({})  # end of tape marker
        # disable_all(self)
        if batch:
            replay_sequence(self)
        else:
            replay_next(self)

    def disable_all_changes(self) -> None:
        self.child.btnbar.child.replay.disabled = True
        self.child.btnbar.child.resume.disabled = True
        self.child.btnbar.child.sbs.disabled = True
        self.child.allow_overwrite.disabled = True

    def _replay_cb(self, btn: ipw.Button) -> None:
        self.disable_all_changes()
        self.do_replay(batch=True)

    def _resume_cb(self, btn: ipw.Button) -> None:
        self.disable_all_changes()
        PARAMS["replay_before_resume"] = True
        reset_recorder()
        set_recording_state(True)
        self.do_replay(batch=True)

    def _step_by_step_cb(self, btn: ipw.Button) -> None:
        PARAMS["step_by_step"] = True
        self.disable_all_changes()
        if self.c_.allow_overwrite.value:
            PARAMS["replay_before_resume"] = True
            reset_recorder()
            set_recording_state(True)
        self.do_replay(batch=False)

    @staticmethod
    def widget_by_id(key: int) -> NodeCarrier:
        return get_widget_by_id(key)

    @staticmethod
    def widget(key: str, num: int = 0) -> NodeCarrier:
        return get_widget_by_key(key, num)

    @staticmethod
    def proxy(key: str, num: int = 0) -> Proxy:
        widget = get_widget_by_key(key, num)
        return Proxy(widget)

    @property
    def dom_id(self) -> str:
        return "root"

    @property
    def _frame(self) -> int:
        return 1

    def dag_register(self) -> None:
        pass
