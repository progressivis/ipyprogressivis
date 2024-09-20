from weakref import ref, ReferenceType
import numpy as np
import os
import json
import base64
import logging
import ipywidgets as ipw
from progressivis.table.dshape import dataframe_dshape
from progressivis.vis import DataShape
from progressivis.core import Sink, Scheduler
from progressivis.core import Module
from progressivis.table.table_facade import TableFacade
from progressivis.core.utils import normalize_columns
from ._js import (jslab_func_remove, jslab_func_cleanup,
                  jslab_func_cell_index)
from ..csv_sniffer import CSVSniffer
from collections import defaultdict
from .. import DagWidgetController  # type: ignore
from .. import PsBoard
import copy
from typing import (
    Any,
    Tuple,
    Union,
    Any as AnyType,
    Optional,
    Dict,
    Set,
    List,
    Callable,
    Iterable,
    Sequence,
    cast,
    Protocol,
    TypeAlias,
    TYPE_CHECKING
)
from dataclasses import dataclass, KW_ONLY
from ..backup import BackupWidget
from ..talker import Talker
from sidecar import Sidecar  # type: ignore
if TYPE_CHECKING:
    from ipyprogressivis.widgets import Constructor

Sniffer = CSVSniffer
DAGWidget: TypeAlias = DagWidgetController
ModuleOrFacade: TypeAlias = Module | TableFacade

logger = logging.getLogger(__name__)

PARAMS: Dict[str, AnyType] = {}

HOME = os.getenv("HOME")
assert HOME is not None

replay_list: List[Dict[str, AnyType]] = []
# chaining_boxes_to_make = []


def runner(func: Callable[..., AnyType]) -> Callable[..., AnyType]:
    def wrapper(*args: Any, **kwargs: Any) -> "NodeCarrier":
        self_ = args[0]
        assert isinstance(self_, GuestWidget)
        if PARAMS["step_by_step"]:
            wg_copy = copy.copy(self_.carrier.children)

            def _continue_cb(b: Any) -> "NodeCarrier":
                assert isinstance(self_, GuestWidget)
                self_.carrier.children = wg_copy
                func(*args, **kwargs)
                content = copy.copy(self_.frozen_kw)
                amend_last_record({'frozen': content})
                return self_.post_run()

            def _edit_cb(b: Any) -> "NodeCarrier":
                assert isinstance(self_, GuestWidget)
                self_.carrier.children = wg_copy
                self_._do_replay_next = True
                return self_.carrier

            def _delete_cb(b: Any) -> "NodeCarrier":
                assert isinstance(self_, GuestWidget)
                self_.carrier.children = wg_copy
                amend_last_record({'deleted': True})
                return self_.post_run()
            btn_c = make_button("Continue", cb=_continue_cb)
            btn_e = make_button("Edit", cb=_edit_cb)
            # btn_d = make_button("Delete", cb=_delete_cb)
            box = ipw.HBox([btn_c, btn_e])
            self_.carrier.children = (box,)
            return self_.carrier
        else:
            func(*args, **kwargs)
            content = copy.copy(self_.frozen_kw)
            amend_last_record({'frozen': content})
            return self_.post_run()
    return wrapper


@dataclass
class Header:
    _: KW_ONLY
    start: bool
    talker: Talker
    backup: BackupWidget
    manager: DagWidgetController
    constructor: "Constructor"
    board: PsBoard
    widgets_out: Sidecar
    modules_out: Sidecar


class Proxy:
    def __init__(self, carrier: "NodeCarrier") -> None:
        self.__carrier = carrier
        self.output_module: ModuleOrFacade | None = None
        self.output_slot: str = 'result'
        self.output_dtypes: dict[str, str] | None = None
        self.freeze = False
        self.cell_content: str = "no code"

    @property
    def input_module(self) -> ModuleOrFacade | None:
        if self.__carrier is PARAMS["constructor"]:
            return None
        return self.__carrier._input_module

    @property
    def input_slot(self) -> str | None:
        if self.__carrier is PARAMS["constructor"]:
            return None
        return self.__carrier._input_slot

    @property
    def input_dtypes(self) -> dict[str, str] | None:
        if self.__carrier is PARAMS["constructor"]:
            return None
        return self.__carrier._dtypes

    @property
    def scheduler(self) -> Scheduler:
        if self.__carrier is PARAMS["constructor"]:
            assert PARAMS["constructor"] is not None
            return cast(Scheduler, PARAMS["constructor"].scheduler)
        assert self.input_module is not None
        return self.input_module.scheduler()

    def resume(self) -> "NodeCarrier":
        self.__carrier._output_module = self.output_module  # type: ignore
        self.__carrier._output_slot = self.output_slot
        self.__carrier._output_dtypes = self.output_dtypes
        if self.freeze and not is_replay():
            amend_last_record({'frozen': dict(cell=self.cell_content)})
        self.__carrier.dag_running()
        if is_replay():
            replay_next()
            return self.__carrier
        self.__carrier.make_chaining_box()
        return self.__carrier


def get_header() -> Header:
    """
    NB: call this function ONLY from the first cell of the notebook!!
    """
    from ipyprogressivis.widgets import Constructor

    if "header" in PARAMS:
        hdr = cast(Header, PARAMS["header"])
        hdr.start = False
        try:
            hdr.widgets_out.close()
            hdr.modules_out.close()
        except Exception:
            pass
        hdr.modules_out = Sidecar(title="Modules Output")
        hdr.widgets_out = Sidecar(title="Widgets Output")
        return hdr
    manager = DagWidgetController()
    backup = BackupWidget()
    constructor = Constructor(manager, backup=backup)
    PARAMS["header"] = Header(
        start=True,
        talker=Talker(),
        backup=backup,
        manager=manager,
        constructor=constructor,
        board=PsBoard(constructor.scheduler),
        modules_out=Sidecar(title="Modules Output"),
        widgets_out=Sidecar(title="Widgets Output"),
    )
    return cast(Header, PARAMS["header"])


def get_backup_widget() -> BackupWidget:
    return cast(BackupWidget, PARAMS["header"].backup)


def labcommand(cmd: str, **kw: AnyType) -> None:
    hdr = PARAMS["header"]
    hdr.talker.labcommand(cmd, kw)


def json2b64(json_: AnyType) -> str:
    return base64.b64encode(json.dumps(json_).encode()).decode()


def b642json(b64str: str) -> AnyType:
    return json.loads(base64.b64decode(b64str.encode()).decode())


def bpack(bak: List[AnyType]) -> str:
    return ";".join([json2b64(elt) for elt in bak])


def bunpack(bstr: str) -> List[AnyType]:
    return bstr.split(";")


def backup_to_json() -> AnyType:
    return [b642json(step) for step in PARAMS["header"].backup.value.split(";")]


def dump_backup(s: str) -> AnyType:
    return [b642json(step) for step in s.split(";")]


class Recorder:
    def __init__(self, value: str = "") -> None:
        self.tape = value

    def is_empty(self) -> bool:
        return not self.tape

    def add_to_record(self, content: Dict[str, AnyType]) -> None:
        self.tape = (self.tape + ";" + json2b64(content)
                     if self.tape
                     else json2b64(content))
        labcommand("progressivis:set_backup", backup=self.tape)

    def amend_last_record(self, content: Dict[str, AnyType]) -> None:
        unpacked = bunpack(self.tape)
        current = b642json(unpacked[-1])
        current.update(content)
        unpacked[-1] = json2b64(current)
        self.tape = ";".join(unpacked)
        labcommand("progressivis:set_backup", backup=self.tape)


def get_recorder() -> Recorder:
    return cast(Recorder, PARAMS.get("recorder"))


def add_to_record(content: Dict[str, AnyType]) -> None:
    rec = get_recorder()
    if rec is None:
        return
    rec.add_to_record(content)


def amend_last_record(content: Dict[str, AnyType]) -> None:
    rec = get_recorder()
    if rec is None:
        return
    rec.amend_last_record(content)


def reset_recorder(previous: str = "", init_val: str = "") -> None:
    if previous:
        PARAMS["previous_recorder"] = Recorder(previous)
    PARAMS["recorder"] = Recorder(value=init_val)
    labcommand("progressivis:set_backup", backup=init_val)


def restore_recorder() -> None:
    PARAMS["recorder"] = PARAMS["previous_recorder"]
    labcommand("progressivis:set_backup", backup=PARAMS["recorder"].tape)


def replay_next(obj: Optional[Union["Constructor", "NodeVBox"]] = None) -> None:
    if not is_replay():
        return
    assert replay_list
    stage = replay_list.pop(0)
    parent = stage.pop("parent", None)
    if obj is None and stage and "ftype" not in stage:
        assert parent is not None
        t0, t1 = parent
        obj = widget_by_key[(cast(str, t0), cast(int, t1))]
    if not stage:  # i.e. stage == {}, end of tape
        if PARAMS["replay_before_resume"]:
            PARAMS["is_replay"] = False
            PARAMS["replay_before_resume"] = False
        return
    if "ftype" in stage:  # i.e. is a loader
        replay_start_loader(PARAMS["constructor"], **stage)
    else:
        assert obj is not None
        replay_new_stage(obj, **stage)  # type: ignore


def get_dag() -> DAGWidget:
    assert "dag_widget" in PARAMS
    return PARAMS["dag_widget"]


def set_dag(dag: DAGWidget) -> None:
    assert "dag_widget" not in PARAMS
    PARAMS["dag_widget"] = dag


WidgetType = AnyType


def get_param(d: Dict[str, List[str]], key: str, default: List[str]) -> List[str]:
    if key not in d:
        return default
    val = d[key]
    if not val:
        return default
    return val


def set_child(wg: ipw.Tab, i: int, child: ipw.DOMWidget, title: str = "") -> None:
    children = list(wg.children)
    children[i] = child
    wg.children = tuple(children)
    if title:
        wg.set_title(i, title)


def append_child(wg: ipw.Tab, child: ipw.DOMWidget, title: str = "") -> None:
    children = list(wg.children)
    last = len(children)
    children.append(child)
    wg.children = tuple(children)
    if title:
        wg.set_title(last, title)


def dongle_widget(v: str = "dongle") -> ipw.Label:
    wg = ipw.Label(v)
    wg.layout.visibility = "hidden"
    return wg


class HandyTab(ipw.Tab):
    def set_next_title(self, name: str) -> None:
        pos = len(self.children) - 1
        self.set_title(pos, name)

    def get_titles(self) -> List[str]:
        return [self.get_title(pos) for pos in range(len(self.children))]

    def set_tab(self, title: str, wg: WidgetType, overwrite: bool = True) -> None:
        all_titles = self.get_titles()
        if title in all_titles:
            if not overwrite:
                return
            pos = all_titles.index(title)
            children_ = list(self.children)
            children_[pos] = wg
            self.children = tuple(children_)
        else:
            self.children += (wg,)  # type: ignore
            self.set_next_title(title)

    def remove_tab(self, title: str) -> None:
        all_titles = self.get_titles()
        if title not in all_titles:
            return
        pos = all_titles.index(title)
        children_ = list(self.children)
        children_ = children_[:pos] + children_[pos + 1:]
        titles_ = all_titles[:pos] + all_titles[pos + 1:]
        self.children = tuple(children_)
        for i, t in enumerate(titles_):
            self.set_title(i, t)

    def get_selected_title(self) -> Optional[str]:
        if self.selected_index is None or self.selected_index >= len(self.titles):
            # logger.warning("no selected title")
            return None
        # logger.warning(f"selected title {self.selected_index} {self.titles}")
        return self.get_title(self.selected_index)

    def get_selected_child(self) -> ipw.DOMWidget:
        if self.selected_index is None:
            return None
        return self.children[self.selected_index]


class TreeTab(HandyTab):
    def __init__(
        self, upper: Optional["TreeTab"], known_as: str, *args: AnyType, **kw: AnyType
    ) -> None:
        super().__init__(*args, **kw)
        self.upper = upper
        self.known_as = known_as
        self.mod_dict: Dict[str, Set[str]] = {}

    def is_visible(self, sel: str) -> bool:
        if self.get_selected_title() != sel:
            return False
        if self.upper is None:
            return True
        return self.upper.is_visible(self.known_as)


def get_schema(sniffer: Sniffer) -> AnyType:
    params = sniffer.params
    usecols = params.get("usecols")
    parse_dates = get_param(params, "parse_dates", [])

    def _ds(col: str, dt: str) -> str:
        if col in parse_dates:
            return "datetime64"
        return dataframe_dshape(np.dtype(dt))

    assert hasattr(sniffer, "_df")
    assert sniffer._df is not None
    norm_cols = dict(zip(sniffer._df.columns, normalize_columns(sniffer._df.columns)))
    dtypes = {col: _ds(col, dt) for (col, dt) in sniffer._df.dtypes.to_dict().items()}
    if usecols is not None:
        dtypes = {norm_cols[col]: dtypes[col] for col in usecols}
    else:
        dtypes = {norm_cols[col]: t for (col, t) in dtypes.items()}
    return dtypes


def disable_all(wg: Any, exceptions: frozenset[Any] = frozenset()) -> None:
    if hasattr(wg, "disabled") and wg not in exceptions:
        wg.disabled = True
    if hasattr(wg, "children") and wg not in exceptions:
        for ch in wg.children:
            disable_all(ch, exceptions)


def make_button(
    label: str,
    disabled: bool = False,
    cb: Optional[Callable[..., AnyType]] = None,
    **kw: Any,
) -> ipw.Button:
    btn = ipw.Button(
        description=label,
        disabled=disabled,
        button_style="",
        tooltip=label,
        icon="check",
        **kw,
    )
    if cb is not None:
        btn.on_click(cb)
    return btn


stage_register: Dict[str, AnyType] = {}
parent_widget: Optional["NodeCarrier"] = None
parent_dtypes: Optional[Dict[str, str]] = None
key_by_id: Dict[int, Tuple[str, int]] = {}
widget_by_id: Dict[int, "NodeCarrier"] = {}
widget_by_key: Dict[Tuple[str, int], "NodeCarrier"] = {}
widget_numbers: Dict[str, int] = defaultdict(int)
recording_state: bool = False


class _Dag:
    def __init__(
        self, label: str, number: int, dag: DAGWidget, alias: str = ""
    ) -> None:
        self._label = label
        if alias:
            self._number = 0
        else:
            self._number = number
        self._dag = dag
        self._alias = alias


def create_stage_widget(key: str, frozen: AnyType = None) -> "NodeCarrier":
    obj = parent_widget
    assert obj is not None
    dtypes = obj._output_dtypes
    if dtypes is None:
        dtypes = parent_dtypes
    dag = _Dag(label=key, number=widget_numbers[key], dag=get_dag())
    ctx = dict(parent=obj, dtypes=dtypes, input_module=obj._output_module, dag=dag)
    guest = stage_register[key]()
    if frozen is not None:
        guest.frozen_kw = frozen
    stage = NodeCarrier(ctx, guest)
    guest.initialize()
    widget_numbers[key] += 1
    assert obj not in obj.subwidgets
    obj.subwidgets.append(stage)
    widget_by_key[(key, stage.number)] = stage
    widget_by_id[id(stage)] = stage
    key_by_id[id(stage)] = (key, stage.number)
    return stage


def create_loader_widget(
    key: str, ftype: str, alias: str, frozen: AnyType = None
) -> "NodeCarrier":
    obj = parent_widget
    dtypes = None
    assert obj is not None
    assert obj not in obj.subwidgets
    dag = _Dag(label=key, number=widget_numbers[key], dag=get_dag(), alias=alias)
    ctx = dict(parent=obj, dtypes=dtypes, input_module=obj._output_module, dag=dag)
    from .csv_loader import CsvLoaderW
    from .parquet_loader import ParquetLoaderW
    from .custom_loader import CustomLoaderW

    loader: CsvLoaderW | ParquetLoaderW | CustomLoaderW
    if ftype == "csv":
        loader = CsvLoaderW()
    elif ftype == "parquet":
        loader = ParquetLoaderW()
    else:
        assert ftype == "custom"
        loader = CustomLoaderW()
    if frozen is not None:
        loader.frozen_kw = frozen
    stage = NodeCarrier(ctx, loader)
    loader.initialize()
    widget_numbers[key] += 1
    obj.subwidgets.append(stage)
    widget_by_id[id(stage)] = stage
    if alias:
        widget_by_key[(alias, 0)] = stage
        key_by_id[id(stage)] = (alias, 0)
    else:
        widget_by_key[(key, stage.number)] = stage
        key_by_id[id(stage)] = (key, stage.number)
    return stage


def get_widget_by_id(key: int) -> "NodeCarrier":
    return widget_by_id[key]


def get_widget_by_key(key: str, num: int) -> "NodeCarrier":
    return widget_by_key[(key, num)]


def get_recording_state() -> bool:
    return recording_state


def set_recording_state(val: bool) -> None:
    global recording_state
    recording_state = val


def _make_btn_start_loader(
        obj: "NodeCarrier", ftype: str, alias: WidgetType, frozen: AnyType = None
) -> Callable[..., None]:
    def _cbk(btn: ipw.Button) -> None:
        global parent_widget
        parent_widget = obj
        assert parent_widget
        add_new_loader(obj, ftype, alias.value, frozen)
        alias.value = ""
        disable_all(obj,
                    exceptions=frozenset(
                        [obj.child.csv, obj.child.parquet]))  # type: ignore
    return _cbk


def replay_start_loader(
    obj: "NodeCarrier", ftype: str, alias: str, frozen: AnyType | None = None
) -> None:
    global parent_widget
    parent_widget = obj
    assert parent_widget
    add_new_loader(obj, ftype, alias, frozen=frozen)


def replay_new_stage(
    obj: "NodeCarrier", title: str, frozen: AnyType | None = None
) -> None:
    class _FakeSel:
        value: str

    sel = _FakeSel()
    sel.value = title
    global parent_widget
    parent_widget = obj
    if obj._output_dtypes is None:
        s = obj._output_module.scheduler()
        with s:
            ds = DataShape(scheduler=s)
            ds.input.table = obj._output_module.output.result
            ds.on_after_run(obj.make_guess_types_toc2(sel, frozen))  # type: ignore
            sink = Sink(scheduler=s)
            sink.input.inp = ds.output.result
    else:
        add_new_stage(obj, title, frozen=frozen)


def remove_tagged_cells(tag: int) -> None:
    s = jslab_func_remove.format(tag=tag)
    get_dag().exec_js(s)


def _remove_subtree(obj: "ChainingWidget") -> None:
    for sw in obj.subwidgets:
        _remove_subtree(sw)
    tag = id(obj)
    remove_tagged_cells(tag)
    if obj.parent is not None:
        obj.parent.subwidgets.remove(obj)
    if tag in widget_by_id:
        del widget_by_id[tag]
        del widget_by_key[(obj.label, obj.number)]
    obj.delete_underlying_modules()


def make_remove(obj: "NodeVBox") -> Callable[..., None]:
    def _cbk(btn: ipw.Button) -> None:
        _remove_subtree(obj)

    return _cbk


class ChainingProtocol(Protocol):
    _output_dtypes: Optional[Dict[str, str]]
    _output_module: ModuleOrFacade

    def make_guess_types_toc2(self, sel: ipw.Select,
                              frozen: AnyType | None = None) -> Callable[..., AnyType]:
        ...

    def _make_btn_chain_it_cb(self, sel: AnyType,
                              frozen: AnyType | None = None) -> Callable[..., None]:
        ...


class ChainingMixin:
    _output_module: ModuleOrFacade

    def make_guess_types_toc2(
        self, sel: ipw.Select, frozen: AnyType | None = None
    ) -> Callable[..., AnyType]:

        def _guess(m: Module, run_number: int) -> None:
            global parent_dtypes
            assert hasattr(m, "result")
            if m.result is None:
                return
            parent_dtypes = {
                k: "datetime64" if str(v)[0] == "6" else v
                for (k, v) in m.result.items()
            }
            self.output_dtypes = parent_dtypes
            add_new_stage(self, sel.value, frozen)  # type: ignore
            with m.scheduler() as dataflow:
                deps = dataflow.collateral_damage(m.name)
                dataflow.delete_modules(*deps)
        return _guess

    def _make_btn_chain_it_cb(
            self: ChainingProtocol, sel: AnyType, frozen: AnyType = None
    ) -> Callable[..., None]:
        def _cbk(btn: ipw.Button) -> None:
            global parent_widget
            parent_widget = self  # type: ignore
            if self._output_dtypes is None:
                s = self._output_module.scheduler()
                with s:
                    ds = DataShape(scheduler=s)
                    ds.input.table = self._output_module.output.result
                    ds.on_after_run(self.make_guess_types_toc2(sel, frozen=frozen))
                    sink = Sink(scheduler=s)
                    sink.input.inp = ds.output.result
            else:
                add_new_stage(self, sel.value, frozen=frozen)  # type: ignore

        return _cbk

    def _progress_bar(self) -> ipw.FloatProgress:
        prog_wg = ipw.FloatProgress(description="Progress",
                                    min=0.0, max=1.0, layout={'width': '100%'})
        mod_ = self._output_module
        assert isinstance(mod_, Module)

        def _proc(m: Module, r: int) -> None:
            n, d = m.get_progress()
            prog_wg.value = n/d
        mod_.on_after_run(_proc)
        return prog_wg

    def _make_progress_bar(self) -> ipw.VBox:
        prog_wg = self._progress_bar()
        return ipw.VBox([prog_wg])

    def _make_chaining_box(self: ChainingProtocol) -> ipw.Box:
        sel = ipw.Dropdown(
            options=[""] + list(stage_register.keys()),
            value="",
            description="Next stage",
            disabled=False,
        )
        btn = make_button("Chain it", disabled=True, cb=self._make_btn_chain_it_cb(sel))
        del_btn = make_button(
            "Remove subtree", disabled=False, cb=make_remove(self)  # type: ignore
        )

        def _on_sel_change(change: Any) -> None:
            if change["new"]:
                btn.disabled = False
            else:
                btn.disabled = True
        prog_wg = self._progress_bar()  # type: ignore
        sel.observe(_on_sel_change, names="value")
        chaining = ipw.HBox([sel, btn, del_btn])
        return ipw.VBox([prog_wg, chaining])

    def _make_replay_chaining_box(self: ChainingProtocol) -> ipw.Box:
        next_stage = replay_list.pop(0)
        frozen = next_stage.get("frozen")
        if "ftype" in next_stage:
            title = next_stage["alias"]
        else:
            title = next_stage["title"]
        sel = ipw.Dropdown(
            options=[title],
            value=title,
            description="Next stage",
            disabled=True,
        )

        class _FakeSel:
            value: str

        fake_sel = _FakeSel()
        fake_sel.value = title

        if "ftype" in next_stage:
            cons = PARAMS["constructor"]
            btn = make_button(
                "Create loader",
                disabled=False,
                cb=_make_btn_start_loader(cons, next_stage["ftype"], fake_sel, frozen),
            )
        else:
            btn = make_button(
                "Chain it", disabled=False, cb=self._make_btn_chain_it_cb(sel, frozen)
            )
        del_btn = make_button("Remove subtree", disabled=True)

        def _on_sel_change(change: Any) -> None:
            if change["new"]:
                btn.disabled = False
            else:
                btn.disabled = True

        sel.observe(_on_sel_change, names="value")
        prog_wg = self._progress_bar()  # type: ignore
        chaining = ipw.HBox([sel, btn, del_btn])
        return ipw.VBox([prog_wg, chaining])


class LoaderMixin:
    def make_loader_box(self, ftype: str = "csv", disabled: bool = False) -> ipw.HBox:
        alias_inp = ipw.Text(
            value="",
            placeholder="optional alias",
            description=f"{ftype.upper()} loader:",
            disabled=disabled,
            style={'description_width': 'initial'}
        )
        btn = make_button(
            "Create",
            disabled=disabled,
            cb=_make_btn_start_loader(self, ftype, alias_inp),  # type:ignore
        )
        return ipw.HBox([alias_inp, btn])


def cleanup_cells() -> None:
    manager = PARAMS["header"].manager
    manager.exec_js(jslab_func_cleanup)


def insert_cell_at_index(kind: str, text: str, index: int, tag: str) -> None:
    get_dag().exec_js(
        jslab_func_cell_index.format(kind=kind, text=text, index=index, tag=tag)
    )


def get_previous(obj: "ChainingWidget") -> "ChainingWidget":
    if not obj.subwidgets:
        return obj
    return get_previous(obj.subwidgets[-1])


new_stage_cell_0 = "Constructor.widget('{key}'){end}"
new_stage_cell = "Constructor.widget('{key}', {num}){end}"
new_stage_cell_code = ("%%pv_run_cell -p {key},{num}\n"
                       "# The 'proxy' name is present in this context"
                       " and you can reference it.\n"
                       "# it provides the following attributes:\n"
                       "#  - proxy.input_module: Module | TableFacade \n"
                       "#  - proxy.input_slot: str \n"
                       "#  - proxy.input_dtypes: dict[str, str] | None\n"
                       "#  - proxy.scheduler: Scheduler\n"
                       "# Put your own imports here\n"
                       "...\n"
                       "...\n"
                       "with scheduler:\n"
                       "    # Put your own code here\n"
                       "    ...\n"
                       "    ...\n"
                       "    # fill in the following proxy attributes:\n"
                       "    proxy.output_module = ...  # Module | TableFacade\n"
                       "    proxy.output_slot = 'result'  # str\n"
                       "    proxy.freeze = True  # bool\n"
                       "    # Warning: keep the code below unchanged\n"
                       "    display(proxy.resume())"
                       "{end}"
                       )

new_loader_cell_code = ("%%pv_run_cell -p {key},{num}\n"
                        "scheduler = proxy.scheduler\n"
                        "# Warning: keep the code above unchanged\n"
                        "# Put your own imports here\n"
                        "... \n"
                        "... \n"
                        "with scheduler:\n"
                        "    # Put your own code here\n"
                        "    ...\n"
                        "    ...\n"
                        "    # fill in the following proxy attributes:\n"
                        "    proxy.output_module = ...  # Module | TableFacade\n"
                        "    proxy.output_slot = 'result'  # str\n"
                        "    proxy.freeze = True  # bool\n"
                        "    # Warning: keep the code below unchanged\n"
                        "    display(proxy.resume())"
                        "{end}"
                        )


def is_replay() -> bool:
    return cast(bool, PARAMS.get("is_replay", False))


def get_stage_cell(key: str, num: int, end: str,
                   frozen: AnyType = None) -> tuple[str, bool, bool]:
    if key == "Python":
        if is_replay() and frozen:
            assert "cell" in frozen
            return frozen["cell"], False, True
        return new_stage_cell_code.format(key=key, num=num, end=end), True, False
    return new_stage_cell.format(key=key, num=num, end=end), False, True


def get_loader_cell(key: str, ftype: str, num: int,
                    end: str, frozen: AnyType = None) -> tuple[str, bool, bool]:
    if ftype == "custom":
        if is_replay() and frozen:
            assert "cell" in frozen
            return frozen["cell"], False, True
        return new_loader_cell_code.format(key=key, num=num, end=end), True, False
    return new_stage_cell.format(key=key, num=num, end=end), False, True


def add_new_stage(parent: "ChainingWidget", title: str, frozen: AnyType = None,
                  no_record: bool = False) -> None:
    stage = create_stage_widget(title, frozen)
    parent_key = key_by_id[id(parent)]
    tag = id(stage)
    n = stage.number
    end = ""
    if frozen is not None and is_replay():
        end = ".run()"
    md = "## " + title + (f"[{n}]" if n else "")
    code, rw, run = get_stage_cell(key=title, num=n, end=end, frozen=frozen)
    labcommand("progressivis:create_stage_cells", tag=tag,
               md=md, code=code, rw=rw, run=run)
    if not no_record:
        add_to_record(dict(title=title, parent=parent_key))


def add_new_loader(
    parent: "ChainingWidget", ftype: str, alias: str, frozen: AnyType = None
) -> None:
    title = f"{ftype.upper()} loader"
    stage = create_loader_widget(title, ftype, alias, frozen)
    tag = id(stage)
    n = stage.number
    end = ""
    if frozen is not None and is_replay():
        end = ".run()"
    if alias:
        md = f"## {alias}"
    else:
        md = "## " + title + (f"[{n}]" if n else "")
    code, rw, run = get_loader_cell(key=alias or title,
                                    ftype=ftype, num=n, end=end, frozen=frozen)
    labcommand("progressivis:create_stage_cells", tag=tag, md=md,
               code=code, rw=rw, run=run)
    add_to_record(dict(ftype=ftype, alias=alias))


class ChainingWidget:
    def __init__(self, kw: Any) -> None:
        assert "parent" in kw
        self.parent: Optional["NodeVBox"] = kw["parent"]
        assert "dtypes" in kw
        self._dtypes: Dict[str, str] = kw["dtypes"]
        assert "input_module" in kw
        self._input_module: ModuleOrFacade = cast(ModuleOrFacade, kw["input_module"])
        self._input_slot: str = kw.get("input_slot", "result")
        self._output_module: ModuleOrFacade = self._input_module
        self._output_slot = self._input_slot
        self._output_dtypes: Optional[Dict[str, str]]
        if self._dtypes is not None:  # i.e. not a loader
            self._output_dtypes = None
        self._dag = kw["dag"]
        self.subwidgets: List[ChainingWidget] = []
        self.managed_modules: List[ModuleOrFacade] = []

    def get_underlying_modules(self) -> List[str]:
        raise NotImplementedError()

    def delete_underlying_modules(self) -> None:
        managed_modules = self.get_underlying_modules()
        if not managed_modules:
            return
        with self._input_module.scheduler() as dataflow:
            # for m in obj.managed_modules:
            deps = dataflow.collateral_damage(*managed_modules)
            dataflow.delete_modules(*deps)

    def dag_register(self) -> None:
        assert self.parent is not None
        self.dag.register_widget(
            self, self.title, self.title, self.dom_id, [self.parent.title]
        )

    def dag_running(self, progress: int = 0) -> None:
        self.dag.update_summary(self.title, {"progress": progress, "status": "RUNNING"})

    @property
    def dag(self) -> DAGWidget:
        return self._dag._dag

    @property
    def dom_id(self) -> str:
        return self.title.replace(" ", "-")

    @property
    def label(self) -> str:
        return cast(str, self._dag._label)

    @property
    def number(self) -> int:
        return cast(int, self._dag._number)

    @property
    def title(self) -> str:
        if self._dag._alias:
            return cast(str, self._dag._alias)
        return f"{self.label}[{self.number}]" if self.number else self.label


class GuestWidget:
    def __init__(self) -> None:
        self.__carrier: Union[int, ReferenceType["NodeCarrier"]] = 0
        self.frozen_kw: Dict[str, Any]
        self._do_replay_next: bool = False

    def initialize(self) -> None:
        pass

    @property
    def carrier(self) -> "NodeCarrier":
        assert not isinstance(self.__carrier, int)
        return cast("NodeCarrier", self.__carrier())

    @property
    def dtypes(self) -> Dict[str, str]:
        return self.carrier._dtypes

    @property
    def input_dtypes(self) -> Dict[str, str]:
        return self.carrier._dtypes

    @property
    def input_module(self) -> ModuleOrFacade:
        return self.carrier._input_module

    @property
    def input_slot(self) -> str:
        return self.carrier._input_slot

    @property
    def output_module(self) -> ModuleOrFacade:
        return self.carrier._output_module

    @output_module.setter
    def output_module(self, value: ModuleOrFacade) -> None:
        self.carrier._output_module = value

    @property
    def output_slot(self) -> str:
        return self.carrier._output_slot

    @output_slot.setter
    def output_slot(self, value: str) -> None:
        self.carrier._output_slot = value

    @property
    def output_dtypes(self) -> Optional[Dict[str, str]]:
        return self.carrier._output_dtypes

    @output_dtypes.setter
    def output_dtypes(self, value: Dict[str, str]) -> None:
        self.carrier._output_dtypes = value

    @property
    def parent(self) -> "VBox":
        assert isinstance(self.carrier, NodeCarrier)
        assert self.carrier.parent is not None
        assert len(self.carrier.parent.children)
        return cast("VBox", self.carrier.parent.children[0])

    @property
    def title(self) -> str:
        return self.carrier.title

    @property
    def current_widget_keys(self) -> Iterable[Tuple[str, int]]:
        return widget_by_key.keys()

    @property
    def dag(self) -> DAGWidget:
        return self.carrier.dag

    def get_widget_by_key(self, key: Tuple[str, int]) -> "VBox":
        key = tuple(key)  # type: ignore
        return cast("VBox", widget_by_key[key].children[0])

    def dag_running(self) -> None:
        self.carrier.dag_running()

    def make_chaining_box(self) -> None:
        self.carrier.make_chaining_box()

    def _make_guess_types(
        self, fun: Callable[..., None], args: Iterable[Any], kw: Dict[str, Any]
    ) -> Callable[[Module, int], None]:
        def _guess(m: Module, run_number: int) -> None:
            assert hasattr(m, "result")
            if m.result is None:
                return
            self.output_dtypes = {
                k: "datetime64" if str(v)[0] == "6" else v
                for (k, v) in m.result.items()
            }
            fun(*args, **kw)
            with m.scheduler() as dataflow:
                deps = dataflow.collateral_damage(m.name)
                dataflow.delete_modules(*deps)

        return _guess

    def compute_dtypes_then_call(
        self,
        func: Callable[..., None],
        args: Iterable[Any] = (),
        kw: Dict[str, Any] = {},
    ) -> None:
        s = self.output_module.scheduler()
        with s:
            ds = DataShape(scheduler=s)
            ds.input.table = self.output_module.output.result
            ds.on_after_run(self._make_guess_types(func, args, kw))
            sink = Sink(scheduler=s)
            sink.input.inp = ds.output.result

    @property
    def dot_progressivis(self) -> str:
        home = HOME
        pv_dir = f"{home}/.progressivis/"
        if os.path.isdir(pv_dir):
            return pv_dir
        return ""

    @property
    def widget_dir(self) -> str:
        pv_dir = self.dot_progressivis
        if not pv_dir:
            return ""
        settings_dir = f"{pv_dir}/widget_settings/"
        if not os.path.isdir(settings_dir):
            os.mkdir(settings_dir)
        widget_dir = f"{settings_dir}/{type(self).__name__}/"
        if not os.path.isdir(widget_dir):
            os.mkdir(widget_dir)
        return widget_dir

    def post_run(self) -> "NodeCarrier":
        self.dag_running()
        self.carrier.children = (ipw.Label("..."),)
        if PARAMS["replay_before_resume"]:
            self.make_chaining_box()
            # chaining_boxes_to_make.append(self)
        else:
            self.carrier.make_progress_bar()
        replay_next()
        return self.carrier

    def manage_replay(self) -> None:
        if self._do_replay_next:
            replay_next()


class VBox(ipw.VBox, GuestWidget):
    def __init__(self, *args: Any, **kw: Any) -> None:
        ipw.VBox.__init__(self, *args, **kw)
        GuestWidget.__init__(self)


class LeafVBox(ipw.VBox, ChainingWidget):
    def __init__(
        self, ctx: Dict[str, Any], children: Sequence[GuestWidget] = ()
    ) -> None:
        ipw.VBox.__init__(self, children)
        ChainingWidget.__init__(self, ctx)
        self.dag_register()


class NodeVBox(LeafVBox, ChainingMixin):
    def __init__(
        self, ctx: Dict[str, Any], children: Sequence[GuestWidget] = ()
    ) -> None:
        super().__init__(ctx, children)
        self.dag_register()


class RootVBox(LeafVBox, LoaderMixin):
    def __init__(
        self, ctx: Dict[str, Any], children: Sequence[GuestWidget] = ()
    ) -> None:
        super().__init__(ctx, children)
        self.dag_register()


class NodeCarrier(NodeVBox):
    def __init__(self, ctx: Dict[str, Any], guest: GuestWidget) -> None:
        super().__init__(ctx, (guest,))
        guest._GuestWidget__carrier = ref(self)  # type: ignore
        self.dag_register()

    def run(self) -> None:
        assert self.children[0].frozen_kw is not None  # type: ignore
        return self.children[0].run()  # type: ignore

    def make_chaining_box(self) -> None:
        if len(self.children) > 1:
            raise ValueError("The chaining box already exists")
        if replay_list and not PARAMS["replay_before_resume"]:
            box = self._make_replay_chaining_box()
        else:
            box = self._make_chaining_box()
        if not box:
            return
        self.children = (self.children[0], box)

    def make_progress_bar(self) -> None:
        box = self._make_progress_bar()
        self.children = (self.children[0], box)


def _none_wg(wg: Optional[ipw.DOMWidget]) -> ipw.DOMWidget:
    return dongle_widget() if wg is None else wg


class TypedBase:
    def __init__(self) -> None:
        self._main: Optional[ReferenceType["TypedBox"]] = None

    @property
    def main(self) -> "TypedBox":
        assert self._main is not None
        return cast("TypedBox", self._main())

    def __setattr__(self, name: str, value: ipw.DOMWidget) -> None:
        super().__setattr__(name, value)
        if (
            self.__annotations__
            and name in self.__annotations__
            and self._main is not None
        ):
            if not self.main.children:
                self.main.children = [
                    dongle_widget() for _ in self.__annotations__.keys()
                ]
            if value is None:
                value = dongle_widget()
            self.main.set_child(name, value)


class TypedBox:
    Typed: type

    def __init__(self) -> None:
        self.child = self.Typed()
        self.c_ = self.child
        self.child._main = ref(self)
        self.children: Sequence[ipw.DOMWidget] = ()

    def set_child(self, name: str, child: ipw.DOMWidget) -> None:
        schema = list(self.child.__annotations__)  # TODO: cache it
        i = schema.index(name)
        children = list(self.children)
        children[i] = child
        self.children = tuple(children)


class VBoxTyped(VBox, TypedBox):
    def __init__(self, *args: Any, **kw: Any) -> None:
        VBox.__init__(self, *args, **kw)
        TypedBox.__init__(self)
        self.frozen_kw: AnyType = None

    def run(self) -> None:
        raise ValueError("run() not defined")


class IpyVBoxTyped(ipw.VBox, TypedBox):
    def __init__(self, *args: Any, **kw: Any) -> None:
        ipw.VBox.__init__(self, *args, **kw)
        TypedBox.__init__(self)


class IpyHBoxTyped(ipw.HBox, TypedBox):
    def __init__(self, *args: Any, **kw: Any) -> None:
        ipw.HBox.__init__(self, *args, **kw)
        TypedBox.__init__(self)
