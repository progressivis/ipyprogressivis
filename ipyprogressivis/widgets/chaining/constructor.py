import ipywidgets as ipw
from progressivis.io.api import Variable
from progressivis.core.api import Sink, Scheduler
import progressivis.core.aio as aio
from ipyprogressivis.hook_tools import make_css_marker
import asyncio


from .utils import (
    make_button,
    set_dag,
    _Dag,
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
    LOADERS,
    add_new_loader,
    disable_all,
    set_parent_widget
)

from typing import (
    Any as AnyType,
    Callable,
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
        sbs: ipw.Button


class LoadBlock(IpyHBoxTyped):
    class Typed(TypedBase):
        choice: ipw.Dropdown
        alias_inp: ipw.Text
        create_btn: ipw.Button


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
            urls: list[str] = [],
            *,
            backup: AnyType = None,
            name: str = "root",
            to_sniff: str | None = None,
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
        self.add_class("progressivis_guest_widget")
        self.add_class(make_css_marker("root"))

    def _allow_overwrite_cb(self, change: dict[str, AnyType]) -> None:
        self._do_record = change["new"]
        self.child.btnbar.child.sbs.disabled = not change["new"]

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
                "Replay all", cb=self._replay_cb, disabled=False
            )
            self.child.btnbar.child.sbs = make_button(
                "Step by step", cb=self._step_by_step_cb, disabled=True
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
        choice = self.c_.loader.c_.choice = ipw.Dropdown(
                options=[("", "")] + list(LOADERS.items()),
                value="",
                description="Loader",
                disabled=False,
                style={"description_width": "initial"},
            )
        choice.observe(self._loader_choice_cb, names="value")
        self.c_.loader.c_.alias_inp = ipw.Text(
            value="",
            placeholder="optional alias",
            description="",
            disabled=False,
            style={"description_width": "initial"},
        )
        self.c_.loader.c_.create_btn = make_button(
            "Create",
            disabled=True,
            cb=self._btn_start_loader_cb
        )
    def _loader_choice_cb(self,  val: AnyType) -> None:
        self.c_.loader.c_.create_btn.disabled = not val["new"]

    def _btn_start_loader_cb(self, btn: ipw.Button) -> None:
        set_parent_widget(self)
        if self._do_record:
            reset_recorder()
            set_recording_state(True)
        add_new_loader(self, self.c_.loader.c_.choice.value, self.c_.loader.c_.alias_inp.value)
        disable_all(self)
        self.dag_running()

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
        self.dag_running()
        if batch:
            replay_sequence(self)
        else:
            replay_next(self)

    def disable_all_changes(self) -> None:
        self.child.btnbar.child.replay.disabled = True
        self.child.btnbar.child.sbs.disabled = True
        self.child.allow_overwrite.disabled = True

    def _replay_cb(self, btn: ipw.Button) -> None:
        if self.c_.allow_overwrite.value:
            self._resume_cb(btn)
        else:
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

    @property
    def dom_id(self) -> str:
        return "root"

    @property
    def _frame(self) -> int:
        return 1

    def dag_register(self) -> None:
        pass

    @staticmethod
    def custom_function(fnc: Callable[..., AnyType]) -> Callable[..., AnyType]:
        PARAMS["customer_functions"][fnc.__name__] = fnc
        return fnc
