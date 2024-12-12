import ipywidgets as ipw
from progressivis.io.api import Variable
from progressivis.core.api import Sink, Scheduler
import progressivis.core.aio as aio
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
    restore_recorder,
    set_recording_state,
    disable_all
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
        allow_overwrite: ipw.Checkbox
        play_mode_radio: ipw.RadioButtons
        record_ck: ipw.Checkbox
        csv: Optional[ipw.HBox]
        parquet: Optional[ipw.HBox]
        custom: Optional[ipw.HBox]
        replay: ipw.Button
        resume: ipw.Button
        sbs: ipw.Button

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
        PARAMS["is_replay"] = False
        PARAMS["replay_before_resume"] = False
        PARAMS["step_by_step"] = False
        PARAMS["deleted_stages"] = set()
        PARAMS["command_list"] = []
        set_recording_state(False)

    def _locked(self) -> bool:
        return bool((self._backup.value and not self.c_.allow_overwrite.value))
    # or self.c_.play_mode_radio.value == "replay")

    def _allow_overwrite_cb(self, change: dict[str, AnyType]) -> None:
        self.child.play_mode_radio.disabled = not change["new"]
        self.child.record_ck.disabled = not change["new"]
        self.child.resume.disabled = not (change["new"] and self._backup.value)
        self.child.sbs.disabled = not (change["new"] and self._backup.value)

    def _start_scheduler_cb(self, btn: ipw.Button) -> None:
        init_module = init_dataflow(self.scheduler)
        self._output_module = init_module
        self._output_slot = "result"
        self._output_dtypes = {}
        if self._backup.value:
            self.child.allow_overwrite = ipw.Checkbox(description="Allow overwriting"
                                                      " of pre-existing recordings")
            self.child.allow_overwrite.observe(self._allow_overwrite_cb, names="value")
            self.child.play_mode_radio = ipw.RadioButtons(
                options=[('Start a new scenario', 'play'),
                         ('Replay/Resume/Edit', 'replay')
                         ],
                value='replay',
                #    layout={'width': 'max-content'}, # If the items' names are long
                # description='Mode:',
                disabled=True
            )
            self.child.play_mode_radio.observe(self._play_mode_cb, names="value")
        self.child.record_ck = ipw.Checkbox(description="Record this scenario",
                                            disabled=self._locked())
        if self._backup.value:
            self.child.record_ck.description += "(previous record will be lost)"
        self.child.record_ck.observe(self._record_cb, names="value")

        self.child.csv = self.make_loader_box(ftype="csv", disabled=self._locked())
        self.child.parquet = self.make_loader_box(ftype="parquet",
                                                  disabled=self._locked())
        self.child.custom = self.make_loader_box(ftype="custom",
                                                 disabled=self._locked())
        self._arch_list = [b642json(elt)
                           for elt in bunpack(self._backup.value)
                           ] if self._backup.value else []
        self.child.replay = make_button(
            "Replay ...", cb=self._replay_cb, disabled=(not self._backup.value)
        )
        btn.disabled = True
        self.child.resume = make_button(
            "Resume ...", cb=self._resume_cb, disabled=not (
                self._backup.value and self.child.allow_overwrite.value
            )
        )
        self.child.sbs = make_button(
            "Step by step", cb=self._step_by_step_cb, disabled=not (
                self._backup.value
            )
        )
        self.dag.register_widget(self, "root", "root", self.dom_id, [])

    def _play_mode_cb(self, change: dict[str, AnyType]) -> None:
        PARAMS["play_mode"] = change["new"]
        is_replay = (change["new"] == 'replay')
        self.child.replay.disabled = not is_replay
        for wg in self.child.csv.children:
            wg.disabled = is_replay
        for wg in self.child.parquet.children:
            wg.disabled = is_replay
        for wg in self.child.custom.children:
            wg.disabled = is_replay

        if is_replay:
            self.child.record_ck.value = False
        self.child.record_ck.disabled = is_replay

    def _record_cb(self, change: dict[str, AnyType]) -> None:
        if change["new"]:
            reset_recorder()
            set_recording_state(True)
        else:
            restore_recorder()
            set_recording_state(False)

    def do_replay(self, batch: bool = False) -> None:
        PARAMS["is_replay"] = True
        self.child.csv.children[-1].disabled = True
        self.child.parquet.children[-1].disabled = True
        self.child.custom.children[-1].disabled = True
        self.child.record_ck.value = False
        self.child.record_ck.disabled = True
        self.child.play_mode_radio.disabled = True
        replay_list.clear()
        replay_list.extend(self._arch_list)
        replay_list.append({})  # end of tape marker
        # chaining_boxes_to_make.clear()
        disable_all(self)
        if batch:
            replay_sequence(self)
        else:
            replay_next(self)

    def disable_all_btn(self) -> None:
        self.child.replay.disabled = True
        self.child.resume.disabled = True
        self.child.sbs.disabled = True

    def _replay_cb(self, btn: ipw.Button) -> None:
        self.disable_all_btn()
        self.do_replay(batch=True)

    def _resume_cb(self, btn: ipw.Button) -> None:
        self.disable_all_btn()
        PARAMS["replay_before_resume"] = True
        reset_recorder()
        set_recording_state(True)
        self.do_replay(batch=True)

    def _step_by_step_cb(self, btn: ipw.Button) -> None:
        PARAMS["step_by_step"] = True
        self.disable_all_btn()
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
        return f"prog_{id(self)}"

    @property
    def _frame(self) -> int:
        return 1

    def dag_register(self) -> None:
        pass
