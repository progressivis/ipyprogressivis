from weakref import ref, ReferenceType
import numpy as np
import json
import base64
import logging
import ipywidgets as ipw
from progressivis.table.dshape import dataframe_dshape
from progressivis.vis import DataShape
from progressivis.core import Sink
from progressivis.core import Module
from progressivis.core.utils import normalize_columns
from ..csv_sniffer import CSVSniffer
from collections import defaultdict
from .. import DagWidgetController  # type: ignore
from .. import PsBoard
from typing import (
    Any,
    Union,
    Tuple,
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
    TYPE_CHECKING
)
from typing_extensions import TypeAlias  # python 3.9
from dataclasses import dataclass, KW_ONLY
from ..backup import BackupWidget
from ..talker import Talker
from sidecar import Sidecar  # type: ignore
if TYPE_CHECKING:
    from ipyprogressivis.widgets import Constructor

Sniffer = CSVSniffer
DAGWidget: TypeAlias = DagWidgetController

logger = logging.getLogger(__name__)

PARAMS: Dict[str, AnyType] = {}


replay_list: List[Dict[str, AnyType]] = []


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
        # hdr.talker = Talker()
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
    # return json.loads(bstr)
    return bstr.split(";")


def backup_to_json() -> AnyType:
    return [b642json(step) for step in PARAMS["header"].backup.value.split(";")]


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


def reset_recorder(previous: str = "") -> None:
    if previous:
        PARAMS["previous_recorder"] = Recorder(previous)
    PARAMS["recorder"] = Recorder()
    labcommand("progressivis:set_backup", backup="")


def restore_recorder() -> None:
    PARAMS["recorder"] = PARAMS["previous_recorder"]
    labcommand("progressivis:set_backup", backup=PARAMS["recorder"].tape)


def replay_next(obj: AnyType) -> None:
    assert replay_list
    stage = replay_list.pop(0)
    print("replay next", stage)
    if not stage:  # i.e. stage == {}, end of tape
        return
    if "ftype" in stage:  # i.e. is a loader
        replay_start_loader(PARAMS["constructor"], **stage)
    else:
        replay_new_stage(obj, **stage)


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


def make_button(
    label: str,
    disabled: bool = False,
    cb: Optional[Callable[..., AnyType]] = None,
    **kw: Any,
) -> ipw.Button:
    btn = ipw.Button(
        description=label,
        disabled=disabled,
        button_style="",  # 'success', 'info', 'warning', 'danger' or ''
        tooltip=label,
        icon="check",  # (FontAwesome names without the `fa-` prefix)
        **kw,
    )
    if cb is not None:
        btn.on_click(cb)
    return btn


"""
def make_guess_types_toc2(
    obj: AnyType, sel: ipw.Select, fun: Callable[..., AnyType]
) -> Callable[..., AnyType]:
    def _guess(m: Module, run_number: int) -> None:
        global parent_dtypes
        assert hasattr(m, "result")
        if m.result is None:
            return
        parent_dtypes = {
            k: "datetime64" if str(v)[0] == "6" else v for (k, v) in m.result.items()
        }
        obj.output_dtypes = parent_dtypes
        fun(obj, sel.value)
        with m.scheduler() as dataflow:
            deps = dataflow.collateral_damage(m.name)
            dataflow.delete_modules(*deps)

    return _guess
"""

stage_register: Dict[str, AnyType] = {}
parent_widget: Optional["NodeVBox"] = None
parent_dtypes: Optional[Dict[str, str]] = None
# last_created = None
widget_by_id: Dict[int, "NodeVBox"] = {}
widget_by_key: Dict[Tuple[str, int], "NodeVBox"] = {}
widget_numbers: Dict[str, int] = defaultdict(int)


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
    # global last_created
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
    guest.init()
    widget_numbers[key] += 1
    assert obj not in obj.subwidgets
    obj.subwidgets.append(stage)
    widget_by_key[(key, stage.number)] = stage
    widget_by_id[id(stage)] = stage
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

    loader: Union[CsvLoaderW, ParquetLoaderW]
    if ftype == "csv":
        loader = CsvLoaderW()
    else:
        assert ftype == "parquet"
        loader = ParquetLoaderW()
    if frozen is not None:
        loader.frozen_kw = frozen
    stage = NodeCarrier(ctx, loader)
    loader.init()
    widget_numbers[key] += 1
    obj.subwidgets.append(stage)
    widget_by_id[id(stage)] = stage
    if alias:
        widget_by_key[(alias, 0)] = stage
    else:
        widget_by_key[(key, stage.number)] = stage
    return stage


def get_widget_by_id(key: int) -> "NodeVBox":
    return widget_by_id[key]


def get_widget_by_key(key: str, num: int) -> "NodeVBox":
    return widget_by_key[(key, num)]


"""
def _make_btn_chain_it_cb(
    obj: AnyType, sel: AnyType, fun: Callable[[Any, Any], None]
) -> Callable[..., None]:
    def _cbk(btn: ipw.Button) -> None:
        global parent_widget
        parent_widget = obj
        assert parent_widget
        if obj._output_dtypes is None:
            s = obj._output_module.scheduler()
            with s:
                ds = DataShape(scheduler=s)
                ds.input.table = obj._output_module.output.result
                ds.on_after_run(make_guess_types_toc2(obj, sel, fun))
                sink = Sink(scheduler=s)
                sink.input.inp = ds.output.result
        else:
            fun(obj, sel.value)

    return _cbk
"""


def _make_btn_start_loader(
    obj: "NodeVBox", ftype: str, alias: WidgetType
) -> Callable[..., None]:
    def _cbk(btn: ipw.Button) -> None:
        global parent_widget
        parent_widget = obj
        assert parent_widget
        add_new_loader(obj, ftype, alias.value)
        alias.value = ""

    return _cbk


def replay_start_loader(
    obj: "NodeVBox", ftype: str, alias: str, frozen: AnyType | None = None
) -> None:
    global parent_widget
    parent_widget = obj
    assert parent_widget
    add_new_loader(obj, ftype, alias, frozen=frozen)
    """
    def make_guess_types_for_replay(self, sel: ipw.Select) -> Callable[..., AnyType]:
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
            add_new_stage(self, sel.value)  # type: ignore
            with m.scheduler() as dataflow:
                deps = dataflow.collateral_damage(m.name)
                dataflow.delete_modules(*deps)

        return _guess
    """


def replay_new_stage(
    obj: "NodeVBox", title: str, frozen: AnyType | None = None
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


remove_js_func = """
(function(){{
  let indices = [];
  IPython.notebook.get_cells().forEach( function(cell) {{
    if (cell.metadata !== undefined){{
      if(cell.metadata.progressivis_tag === "{tag}"){{
        cell.metadata.editable = true;
        cell.metadata.deletable = true;
        let i = IPython.notebook.find_cell_index(cell);
        indices.push(i);
      }}
    }}
  }});
  let uIndices = [...new Set(indices)];
  IPython.notebook.delete_cells(uIndices);
}})();
"""

jslab_func_remove = """
(function(){{
  var NotebookActions = ProgressiVis.nbactions;
  var crtWidget = ProgressiVis.nbtracker().currentWidget;
  var notebook = crtWidget.content;
  let toDelete = [];
  notebook.widgets.forEach( function(cell) {{
      if(cell.model.metadata.progressivis_tag === "{tag}"){{
        cell.model.sharedModel.setMetadata("deletable", true);
        cell.model.sharedModel.setMetadata("editable", true);
        toDelete.push(cell);
      }}
  }});
  for(let c of toDelete){{
     let i = notebook.widgets.findIndex((x)=> x == c)
     notebook.model.sharedModel.transact(() => {{
     notebook.model.sharedModel.deleteCell(i);
     }});
  }}
}})();
"""


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
    _output_module: Module

    def make_guess_types_toc2(self, sel: ipw.Select) -> Callable[..., AnyType]:
        ...

    def _make_btn_chain_it_cb(self, sel: AnyType) -> Callable[..., None]:
        ...


class ChainingMixin:
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
        self: ChainingProtocol, sel: AnyType
    ) -> Callable[..., None]:
        def _cbk(btn: ipw.Button) -> None:
            global parent_widget
            parent_widget = self  # type: ignore
            if self._output_dtypes is None:
                s = self._output_module.scheduler()
                with s:
                    ds = DataShape(scheduler=s)
                    ds.input.table = self._output_module.output.result
                    ds.on_after_run(self.make_guess_types_toc2(sel))
                    sink = Sink(scheduler=s)
                    sink.input.inp = ds.output.result
            else:
                add_new_stage(self, sel.value)  # type: ignore

        return _cbk

    def _make_chaining_box(self: ChainingProtocol) -> ipw.HBox:
        sel = ipw.Dropdown(
            options=[""] + list(stage_register.keys()),
            value="",
            # rows=10,
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

        sel.observe(_on_sel_change, names="value")
        return ipw.HBox([sel, btn, del_btn])

    def _make_replay_chaining_box(self: ChainingProtocol) -> ipw.HBox:
        next_stage = replay_list.pop(0)
        if "ftype" in next_stage:
            title = next_stage["alias"]
        else:
            title = next_stage["title"]
        sel = ipw.Dropdown(
            options=[title],
            value=title,
            # rows=10,
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
                cb=_make_btn_start_loader(cons, next_stage["ftype"], fake_sel),
            )
        else:
            btn = make_button(
                "Chain it", disabled=False, cb=self._make_btn_chain_it_cb(sel)
            )
        del_btn = make_button("Remove subtree", disabled=True)

        def _on_sel_change(change: Any) -> None:
            if change["new"]:
                btn.disabled = False
            else:
                btn.disabled = True

        sel.observe(_on_sel_change, names="value")
        return ipw.HBox([sel, btn, del_btn])


class LoaderMixin:
    def make_loader_box(self, ftype: str = "csv") -> ipw.HBox:
        # reset_backup()
        alias_inp = ipw.Text(
            value="",
            placeholder="optional alias",
            description=f"{ftype.upper()} loader:",
            disabled=False,
        )
        btn = make_button(
            "Create",
            disabled=False,
            cb=_make_btn_start_loader(self, ftype, alias_inp),  # type:ignore
        )
        return ipw.HBox([alias_inp, btn])


# https://github.com/jupyterlab/jupyterlab/issues/5660


cleanup_js_func = """
(function(){{
  let indices = [];
  IPython.notebook.get_cells().forEach( function(cell) {{
    if (cell.metadata !== undefined){{
      if(cell.metadata.progressivis_tag !== undefined){{
        cell.metadata.editable = true;
        cell.metadata.deletable = true;
        let i = IPython.notebook.find_cell_index(cell);
        indices.push(i);
      }}
    }}
  }});
  let uIndices = [...new Set(indices)];
  IPython.notebook.delete_cells(uIndices);
}})();
"""

jslab_func_cleanup = """
(function(){{
  var NotebookActions = ProgressiVis.nbactions;
  var crtWidget = ProgressiVis.nbtracker().currentWidget;
  var notebook = crtWidget.content;
  let toDelete = [];
  notebook.widgets.forEach( function(cell) {{
      console.log("meta", cell.model.sharedModel.metadata);
      console.log("ptag", cell.model.sharedModel.metadata.progressivis_tag);
      if(cell.model.sharedModel.metadata.progressivis_tag != undefined){{
        cell.model.sharedModel.setMetadata("deletable", true);
        cell.model.sharedModel.setMetadata("editable", true);
        toDelete.push(cell);
      }}
  }});
  console.log("toDelete", toDelete);
  for(let c of toDelete){{
     let i = notebook.widgets.findIndex((x)=> x == c)
     //notebook.model.sharedModel.transact(() => {{
     notebook.model.sharedModel.deleteCell(i);
     //}});
  }}
}})();
"""


def cleanup_cells() -> None:
    manager = PARAMS["header"].manager
    manager.exec_js(jslab_func_cleanup)


js_func_toc = """
(function(){{
  let i = -1;
  IPython.notebook.get_cells().forEach( function(cell) {{
    if (cell.metadata !== undefined){{
      if(cell.metadata.progressivis_tag === "{prev}"){{
        cell.metadata.editable = true;
        cell.metadata.deletable = true;
        i = IPython.notebook.find_cell_index(cell);
      }}
    }}
  }});
  if(i<0){{
   i = IPython.notebook.get_cell_elements().length;
  }} else {{
    i = i+1;
  }}
  IPython.notebook.insert_cell_at_index("markdown", i).set_text("{md}");
  IPython.notebook.select(i);
  IPython.notebook.execute_cell(i);
  let meta = {{
    "trusted": true,
    "editable": false,
    "deletable": false,
    "progressivis_tag": "{tag}"
   }};
  IPython.notebook.get_cell(i).metadata = meta;
  IPython.notebook.insert_cell_at_index("code", i+1).set_text("{code}");
  IPython.notebook.select(i+1);
  IPython.notebook.execute_cell(i+1);
  IPython.notebook.get_cell(i+1).metadata = meta;
}})();
"""

jslab_func_toc = """
(function(){{
  var NotebookActions = ProgressiVis.nbactions;
  var crtWidget = ProgressiVis.nbtracker().currentWidget;
  var notebook = crtWidget.content;
  let i = -1;
  notebook.widgets.forEach( function(cell) {{
      if(cell.model.metadata.progressivis_tag === "{tag}"){{
        cell.model.sharedModel.setMetadata("deletable", true);
        cell.model.sharedModel.setMetadata("editable", true);
        let i = notebook.widgets.findIndex((x)=> x == cell)
      }}
  }});
  if(i<0){{
   i = notebook.widgets.length;
  }} else {{
    i = i+1;
  }}
  notebook.model.sharedModel.insertCell(i, {{
        "cell_type": "markdown",
        source: "{md}"
      }});
  notebook.activeCellIndex = i;
  var cell = notebook.widgets[i];
  NotebookActions.run(notebook, crtWidget.sessionContext);
  cell.model.sharedModel.setMetadata("trusted", true);
  cell.model.sharedModel.setMetadata("editable", false);
  cell.model.sharedModel.setMetadata("deletable", false);
  cell.model.sharedModel.setMetadata("progressivis_tag", "{tag}");
  notebook.model.sharedModel.insertCell(i+1, {{
        "cell_type": "code",
        source: "{code}"
      }});
  notebook.activeCellIndex = i+1;
  var cell = notebook.widgets[i+1];
  NotebookActions.run(notebook, crtWidget.sessionContext);
  cell.model.sharedModel.setMetadata("trusted", true);
  cell.model.sharedModel.setMetadata("editable", false);
  cell.model.sharedModel.setMetadata("deletable", false);
  cell.model.sharedModel.setMetadata("progressivis_tag", "{tag}");
}})();
"""

js_func_cell_index = """
(function(){{
  IPython.notebook.insert_cell_at_index("{kind}", {index}).set_text("{text}");
  IPython.notebook.select({index});
  IPython.notebook.execute_cell({index});
  let meta = {{
    "trusted": true,
    "editable": false,
    "deletable": false,
    "progressivis_tag": "{tag}"
   }};
  IPython.notebook.get_cell({index}).metadata = meta;
  }})();
"""

jslab_func_cell_index = """
(function(){{
  var NotebookActions = ProgressiVis.nbactions;
  var crtWidget = ProgressiVis.nbtracker().currentWidget;
  var notebook = crtWidget.content;
  notebook.model.sharedModel.insertCell({index}, {{
        "cell_type": "{kind}",
        source: "{text}"
      }});
  notebook.activeCellIndex = {index};
  var cell = notebook.widgets[{index}];
  cell.model.sharedModel.setMetadata("trusted", true);
  cell.model.sharedModel.setMetadata("editable", false);
  cell.model.sharedModel.setMetadata("deletable", false);
  cell.model.sharedModel.setMetadata("progressivis_tag", "{tag}");
  NotebookActions.run(notebook, crtWidget.sessionContext);
  }})();
"""


def insert_cell_at_index(kind: str, text: str, index: int, tag: str) -> None:
    """display(  # type:ignore
        Javascript(  # type:ignore
            js_func_cell_index.format(kind=kind, text=text, index=index, tag=tag)
        )
    )"""
    get_dag().exec_js(
        jslab_func_cell_index.format(kind=kind, text=text, index=index, tag=tag)
    )


def get_previous(obj: "ChainingWidget") -> "ChainingWidget":
    if not obj.subwidgets:
        return obj
    return get_previous(obj.subwidgets[-1])


new_stage_cell_0 = "Constructor.widget('{key}'){end}"
new_stage_cell = "Constructor.widget('{key}', {num}){end}"


def add_new_stage(parent: "ChainingWidget", title: str, frozen: AnyType = None) -> None:
    stage = create_stage_widget(title, frozen)
    tag = id(stage)
    n = stage.number
    end = ""
    if frozen is not None:
        end = ".run();"
    md = "## " + title + (f"[{n}]" if n else "")
    code = new_stage_cell.format(key=title, num=n, end=end)
    # s = jslab_func_toc.format(prev=prev, tag=tag, md=md, code=code)
    # get_dag().exec_js(s)
    labcommand("progressivis:create_stage_cells", tag=tag, md=md, code=code)
    # get_dag().exec_js(";")
    add_to_record(dict(title=title))


def add_new_loader(
    parent: "ChainingWidget", ftype: str, alias: str, frozen: AnyType = None
) -> None:
    title = f"{ftype.upper()} loader"
    stage = create_loader_widget(title, ftype, alias, frozen)
    tag = id(stage)
    n = stage.number
    end = ""
    if frozen is not None:
        end = ".run();"
    if alias:
        md = f"## {alias}"
        code = new_stage_cell_0.format(key=alias, end=end)
    else:
        md = "## " + title + (f"[{n}]" if n else "")
        if n:
            code = new_stage_cell.format(key=title, num=n, end=end)
        else:
            code = new_stage_cell_0.format(key=title, end=end)
    print("new loader", md, frozen)
    # s = jslab_func_toc.format(prev=prev, tag=tag, md=md, code=code)
    # get_dag().exec_js(s)
    labcommand("progressivis:create_stage_cells", tag=tag, md=md, code=code)
    # labcommand("progressivis:args", a=1,b=2)
    add_to_record(dict(ftype=ftype, alias=alias))


class ChainingWidget:
    def __init__(self, kw: Any) -> None:
        assert "parent" in kw
        self.parent: Optional["NodeVBox"] = kw["parent"]
        assert "dtypes" in kw
        self._dtypes: Dict[str, str] = kw["dtypes"]
        assert "input_module" in kw
        self._input_module: Module = cast(Module, kw["input_module"])
        self._input_slot: str = kw.get("input_slot", "result")
        self._output_module: Module = self._input_module
        self._output_slot = self._input_slot
        self._output_dtypes: Optional[Dict[str, str]]
        if self._dtypes is not None:  # i.e. not a loader
            self._output_dtypes = None
        self._dag = kw["dag"]
        self.subwidgets: List[ChainingWidget] = []
        self.managed_modules: List[Module] = []

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

    # @property
    # def _frame(self) -> int:
    #     return self.parent._frame+1

    @property
    def title(self) -> str:
        if self._dag._alias:
            return cast(str, self._dag._alias)
        return f"{self.label}[{self.number}]" if self.number else self.label


class GuestWidget:
    def __init__(self) -> None:
        self.__carrier: Union[int, ReferenceType["NodeCarrier"]] = 0
        self.frozen_kw: Dict[str, Any]

    def init(self) -> None:
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
    def input_module(self) -> Module:
        return self.carrier._input_module

    @property
    def input_slot(self) -> str:
        return self.carrier._input_slot

    @property
    def output_module(self) -> Module:
        return self.carrier._output_module

    @output_module.setter
    def output_module(self, value: Module) -> None:
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
        self.children[0].run()  # type: ignore

    def make_chaining_box(self) -> None:
        if len(self.children) > 1:
            raise ValueError("The chaining box already exists")
        if replay_list:
            box = self._make_replay_chaining_box()
        else:
            box = self._make_chaining_box()
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
