from __future__ import annotations

from weakref import ref, ReferenceType
import numpy as np
import pandas as pd
import os
import json
import base64
import time
import logging
import ipywidgets as ipw
import fsspec  # type: ignore
from glob import glob
import random
from functools import wraps, partial
from progressivis.table.dshape import dataframe_dshape
from progressivis.table.dshape import dshape_fields
from progressivis.core.api import Module, Scheduler
from progressivis.table.api import TableFacade
from progressivis.core.utils import normalize_columns
from progressivis.core import aio
from progressivis.datasets import get_dataset
from ipyprogressivis.hook_tools import make_css_marker, parse_tag
import asyncio
from ipyprogressivis.csv_sniffer.backend import CSVSniffer
from collections import defaultdict
from .. import DagWidgetController  # type: ignore
from ..quality_visualization import QualityVisualization
from ..psboard import PsBoard
from ..json_editor import JsonEditor
from .custom import SnippetResult
from ipyprogressivis.ipywel import Proxy, restore
from pathlib import Path
import copy
import io
import importlib
from typing import (
    Any,
    Tuple,
    Union,
    Type,
    Any as AnyType,
    Optional,
    Callable,
    Iterable,
    Sequence,
    cast,
    Protocol,
    TypeAlias,
    TYPE_CHECKING,
)
from dataclasses import dataclass, KW_ONLY
from ..backup import BackupWidget
from ..talker import Talker
from sidecar import Sidecar  # type: ignore

if TYPE_CHECKING:
    from ipyprogressivis.widgets.chaining.constructor import Constructor


Sniffer = CSVSniffer
DAGWidget: TypeAlias = DagWidgetController
ModuleOrFacade: TypeAlias = Module | TableFacade

logger = logging.getLogger(__name__)

PARAMS: dict[str, AnyType] = {}

HOME = os.getenv("HOME")
assert HOME is not None


QUAL_W = 512
QUAL_H = 128
ITRASH = 0
IGUEST = 1
BOX_SIZE = 5

def json_editor(descr: str | None = None, **kw: AnyType) -> Proxy:
    """
    ipywel wrapper for the custom JSonEditor

    Args:
        descr: description
        **kw: other widget attributes to set
    """
    kw2 = dict() if descr is None else dict(description=descr)
    proxy = Proxy(JsonEditor())
    proxy.attrs(**kw, **kw2)
    return proxy

def dongle_widget(v: str = "") -> ipw.HTML:
    return ipw.HTML(v)


def get_dag() -> DAGWidget:
    assert "dag_widget" in PARAMS
    return PARAMS["dag_widget"]


def make_button(
    label: str,
    disabled: bool = False,
    cb: Optional[Callable[..., AnyType]] = None,
    icon: str = "check",
    button_style: str = "",
    tooltip: str = "",
    **kw: Any,
) -> ipw.Button:
    """
    Shortcut function to create a button with its on_click() callback

    Args:
        label: see ipywidgets.Button
        disabled: see ipywidgets.Button
        cb: on_click() callback
        icon: see ipywidgets.Button
        button_style: see ipywidgets.Button
        tooltip: see ipywidgets.Button

    Returns:
        the created button
    """
    btn = ipw.Button(
        description=label,
        disabled=disabled,
        button_style=button_style,
        tooltip=tooltip or label,
        icon=icon,
        **kw,
    )
    if cb is not None:
        btn.on_click(cb)
    return btn


BTN_DEL = ipw.HBox(
    [make_button("", icon="trash", button_style="danger", disabled=True)]
)
BTN_DEL.display = "flex"
BTN_DEL.layout.justify_content = "flex-end"


def enable_all(wg: Any, exceptions: Sequence[Any] = tuple()) -> None:
    """
    Enable an ipywidget, recurse on containers, skip the exceptions

    Args:
        wg: the "root" widget
        exceptions: sub-widgets to skip
    """
    if hasattr(wg, "disabled") and wg not in exceptions:
        wg.disabled = False
    if hasattr(wg, "children") and wg not in exceptions:
        for ch in wg.children:
            enable_all(ch, exceptions)

def disable_all(wg: Any, exceptions: Sequence[Any] = tuple()) -> None:
    """
    the opposite of the function above
    """
    if hasattr(wg, "disabled") and wg not in exceptions:
        wg.disabled = True
    if hasattr(wg, "children") and wg not in exceptions:
        for ch in wg.children:
            disable_all(ch, exceptions)

def _process_trash(b: AnyType, *, box: ipw.HBox, obj: "NodeCarrier") -> None:
    """
    The Trash button's callback (applied via a partial) deletes a subgraph of widgets.
    It does not delete the objects directly, but instead builds a list of objects to be
    deleted and displays a dialog box with the list of items to be deleted, allowing the user to confirm or cancel.

    Args:
       b: the button widget (part of the standard on_click() callback signature, unused)
       box: box where to include the suppression dialog - provided via partial()
       obj: c-widget proposed for deletion (as well as its sub-widgets) - provided via partial()
    """
    guest_backup = cast(ipw.Box, obj.children[IGUEST]).children
    cast(ipw.Box, obj.children[IGUEST]).children = [dongle_widget()]
    objects = [obj]

    def _aux(obj_: "NodeCarrier") -> None:
        for sw in obj_.subwidgets:
            objects.append(sw)  # type: ignore
            _aux(sw)  # type: ignore

    _aux(obj)
    modules: list[str] = []
    for obj_ in objects:
        modules.extend(obj_.managed_modules)
    with obj._input_module.scheduler as dataflow:
        deps = dataflow.collateral_damage(*modules)
    others = set()
    m_set = set(modules)
    if m_set != deps:
        others = deps.difference(m_set)
    messg = (
        "<b>WARNING:</b> This action will permanently delete the widgets listed below and"
        " their underlying modules"
    )
    begin = f"<table style='border: 1px solid;background-color:red;'><tr><td>&#9888;</td><td>{messg}</td></tr></table>"
    end = "</ul>\n"
    sio = io.StringIO()
    sio.write(begin)
    for obj_ in objects:
        sio.write(f"<li><b>{obj_.title}:&nbsp;</b>")
        sio.write(", ".join(obj_.managed_modules))
        sio.write("</li>\n")
    if others:
        sio.write("<li><b>Others:&nbsp;</b>")
        sio.write(" ,".join(others))
        sio.write("</li>\n")
    sio.write(end)

    def _cancel(b: AnyType) -> None:
        """
        Cancel button callback
        """
        make_trash_box(obj, box)
        cast(ipw.Box, obj.children[IGUEST]).children = guest_backup

    def _confirm(b: AnyType) -> None:
        """
        Confirm button callback
        """
        if obj.parent is not None and obj in obj.parent.subwidgets:
            obj.parent.subwidgets.remove(obj)
        i = obj.children[IGUEST]._record_index  # type: ignore
        assert i is not None
        amend_nth_record(i, {"deleted": True})
        tags = [obj_.title for obj_ in objects]
        with obj._input_module.scheduler as dataflow:
            dataflow.delete_modules(*deps)
        for tag in tags:
            labcommand("progressivis:remove_tagged_cells", tag=tag)
        for obj_ in objects:
            get_dag().remove_widget(obj_.title)
            if (obj_.label, obj_.number) in widget_by_key:
                del widget_by_key[(obj_.label, obj_.number)]
        if not len(widget_by_key):
            enable_all(PARAMS["header"].constructor)
    # building the deletion dialog box
    vbox = ipw.VBox(
        [
            ipw.HTML(sio.getvalue()),
            ipw.HBox(
                [
                    make_button("Cancel", cb=_cancel),
                    make_button("Confirm", cb=_confirm, button_style="danger"),
                ]
            ),
        ]
    )
    box.children = [vbox]
    box.display = None
    box.layout.justify_content = None  # 'flex-start'


def make_trash_box(obj: "NodeCarrier", box: ipw.HBox | None = None) -> ipw.HBox:
    """
    Build the trash button (first-level, the red trash) with it's callback
    """
    trash_btn = make_button("", icon="trash", button_style="danger")
    if box is None:
        box = ipw.HBox([trash_btn])
    else:
        box.children = [trash_btn]
    box.display = "flex"
    box.layout.justify_content = "flex-end"
    trash_btn.on_click(partial(_process_trash, box=box, obj=obj))
    return box


replay_list: list[dict[str, AnyType]] = []
md_list: list[str] = []
widget_list: AnyType = []
REPLAY_BATCH: bool = False


FSSPEC_HTTPS = fsspec.filesystem("https")

LOADERS = {"CSV loader": "csv", "PARQUET loader": "parquet", "CUSTOM loader": "custom"}


def dot_progressivis() -> str:
    """
    Returns:
        the path to the `.progressivis` dir if it exists else returns ""
        It check the existence of `.progressivis` in this order:
        1. In the homedir
        2. In the root of the source code (where `.progressivis` exists in dev mode)
    """
    home = HOME
    pv_dir: Path | str = f"{home}/.progressivis/"
    if os.path.isdir(pv_dir):
        return str(pv_dir)
    dn = os.path.dirname
    repo_dir = dn(dn(dn(dn(__file__))))
    pv_dir = Path(repo_dir) / ".progressivis"
    if os.path.isdir(pv_dir):
        return str(pv_dir)
    return ""


def glob_url(url: str) -> list[str]:
    return cast(list[str], FSSPEC_HTTPS.glob(url))


def expand_urls(urls: list[str]) -> list[str]:
    exp_urls = [os.path.expanduser(url) for url in urls if url]
    res = []
    for url in exp_urls:
        if url.startswith("http://") or url.startswith("https://"):
            res.extend(glob_url(url))
        elif url.startswith("progressivis_dataset://"):
            res.append(get_dataset(url.replace("progressivis_dataset://", "")))
        else:
            res.extend(glob(url))
    return res


def _relative_url(url: str) -> str:
    assert HOME is not None
    if url.startswith(HOME):
        return url.replace(HOME, "~", 1)
    return url


def relative_urls(urls: list[str]) -> list[str]:
    return [_relative_url(url) for url in urls if url]


def shuffle_urls(urls: list[str]) -> list[str]:
    shuffled_urls = random.sample(urls, k=len(urls))
    assert sorted(urls) == sorted(shuffled_urls)
    return shuffled_urls


def runner(func: Callable[..., AnyType]) -> Callable[..., AnyType]:
    """
    Decorator for the `run()` method, whitch defines for each c-widget
    the replay processing.

    NB: In the vast majority of cases, you don't have to apply this decorator yourself.
    This decorator is silently applied on the `run()` method via the `@chaining_widget`
    class decorator. Only if `run()` is inherited from a class which is not `@chaining_widget`
    decorated you have to explicitly decorate the `run()` method with `@runner`

    Args:
        func: the run() method to be wrapped

    Returns:
        the wrapper below
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> "NodeCarrier":
        """
        Wraps `func` in different ways depending on the replay mode:
        * In step-by-step mode, creates and displays a button bar with
        the appropriate callbacks for Continue/Edit/Delete
        * In batch mode adds a post-processing that ensures chaining
        """
        self_ = args[0]
        assert isinstance(self_, GuestWidget)
        if PARAMS["step_by_step"]:
            wg_copy = copy.copy(self_.carrier.children)

            def _continue_cb(b: Any) -> "NodeCarrier":
                assert isinstance(self_, GuestWidget)
                self_.carrier.children = wg_copy
                func(*args, **kwargs)
                content = copy.copy(self_.frozen_kw)
                amend_last_record({"frozen": content})
                return self_.post_run(self_.carrier.title)

            def _edit_cb(b: Any) -> "NodeCarrier":
                assert isinstance(self_, GuestWidget)
                self_.carrier.children = wg_copy
                self_._do_replay_next = True
                return self_.carrier

            def _delete_cb(b: Any) -> "NodeCarrier":
                assert isinstance(self_, GuestWidget)
                self_.carrier.children = wg_copy
                amend_last_record({"deleted": True})
                PARAMS["deleted_stages"].add(
                    (self_.carrier.label, self_.carrier.number)
                )
                assert self_.carrier.parent is not None
                parent_title = self_.carrier.parent.title
                title = self_.carrier.title
                self_.dag.remove_parent(title, parent_title)
                self_.dag.remove_node(title)
                return self_.post_delete()

            btn_c = make_button("Next", cb=_continue_cb)
            btn_e = make_button("Edit", cb=_edit_cb, disabled=not is_recording())
            btn_d = make_button("Delete", cb=_delete_cb, disabled=not is_recording())
            box = ipw.HBox([btn_c, btn_e, btn_d])
            self_.carrier.children = (
                BTN_DEL,
                box,
            )
            return self_.carrier
        else:  # batch mode
            print("batch mode", self_)
            func(*args, **kwargs)
            content = copy.copy(self_.frozen_kw)
            if not is_replay_batch():
                amend_last_record({"frozen": content})
            return self_.post_run(self_.carrier.title)
    # wrapper._already_decorated = True  # avoids to decorate twice?
    return wrapper


@dataclass
class Header:
    _: KW_ONLY
    """
    Brings together the global objects necessary for a progressibook to run.
    They are:

    * Talker: custom ipywidgets-alike object handling communications between the
      kernel and the jupyterlab frontend
    * BackupWidget: custom ipywidgets-alike object responsible for saving the
      states of widgets in the progressibook's metadata
    * DagWidgetController: custom ipywidgets-alike object responsible for displaying
      the DAG of c-widgets of the current scenario
    * Constructor: this is the bootstrap interface
    * PsBoard: displays infos about modules

    DagWidgetController and PsBoard objects are displayed in individual panels outside
    the notebook via two Sidecar widgets
    NB: In practice `Header()` is used as a singleton (unique instance) even though
    it is not implemented as one
    """
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
    Creates and initialize the global objects of the progressibook then
    groups them into a `Header()` object.

    NB: call this function ONLY from the first cell of the notebook!!
    """
    from ipyprogressivis.widgets.chaining.constructor import Constructor

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
    """
    Sends [jupyterlab commands](https://jupyterlab.readthedocs.io/en/4.4.x/user/commands.html)
    from the kernel to the jupyterlab frontend. In practice, this function is used mainly to
    send custom commands defined [here](https://github.com/progressivis/ipyprogressivis/blob/main/ipyprogressivis/js/src/labplugin.js)
    There is one exception to this behaviour: when "progressivis:create_stage_cells" command
    is asked in replay-batch mode, the command is not sent to the frontend. Instead, the
    underlying code is execute synchronously via `exec()` and the cell is not displayed (actually
    it will be displyed later). That's because the execution of cells via the jupyterlab
    command is not synchronous and the order of execution is not guaranteed.

    Args:
        cmd: the jupyterlab command
        **kw: the command (always named) arguments
    """
    if is_replay_batch() and cmd == "progressivis:create_stage_cells":
        code = kw["code"]
        cell_content = code
        _ = cell_content
        line = code.split("\n")[0]
        wg = line.replace(".replay()", "").replace("await ", "")
        md = kw["md"]
        tag = kw["tag"]
        widget_list.append((md, wg, tag))
        code = (
            "from ipyprogressivis.widgets.chaining.constructor import Constructor\n"
            "from ipyprogressivis.widgets.chaining.utils import get_header\n"
        ) + code.replace("await ", "").replace(".replay()", ".run()")
        exec(code)
        return
    hdr = PARAMS["header"]
    hdr.talker.labcommand(cmd, kw)

"""
NB: Backups are base64-encoded,  but not the entire scenario at once.
To avoid encoding/decoding operations on excessively large volumes, each widget state
is encoded in its own base-64 block, and the whole set is saved by concatenating the blocks, separated by ";" like this:

    block1;block2; ... ;blockN
"""


def json2b64(json_: AnyType) -> str:
    return base64.b64encode(json.dumps(json_).encode()).decode()


def b642json(b64str: str) -> AnyType:
    return json.loads(base64.b64decode(b64str.encode()).decode())


def bpack(bak: list[AnyType]) -> str:
    """
    Concatenate backup blocks
    """
    return ";".join([json2b64(elt) for elt in bak])


def bunpack(bstr: str) -> list[AnyType]:
    """
    Split the entire backup in base-64 blocs
    """
    return bstr.split(";")


def backup_to_json() -> AnyType:
    return [b642json(step) for step in PARAMS["header"].backup.value.split(";")]


def dump_backup(s: str) -> AnyType:
    return [b642json(step) for step in s.split(";")]


class Recorder:
    """
    Process the entire backup as a tape, where each base-64 block is a "record"
    NB: Do not confuse this Recorder() and BackupWidget() !
    The Recorder is a one-way, write-only tool which send the tape content to
    the jupyterlab frontend via "progressivis:set_backup" jupyterlab command.
    Lab commands are not able to read content from the frontend
    In order to read a previous backend stored in the notebook (for replay it) one need
    BackupWidget()
    NB: The Record() instance is unique
    """
    def __init__(self, value: str = "") -> None:
        self.tape = value

    def is_empty(self) -> bool:
        return not self.tape

    def add_to_record(self, content: dict[str, AnyType]) -> None:
        """
        Add a new record to the end of the tape and send the whole tape to the frontend
        If the tape is empty it create a one element tape
        """
        self.tape = (
            self.tape + ";" + json2b64(content) if self.tape else json2b64(content)
        )
        labcommand("progressivis:set_backup", backup=self.tape)

    def amend_nth_record(self, nth: int, content: dict[str, AnyType]) -> None:
        """
        Replace a record identified by its index (position in the tape)
        """
        unpacked = bunpack(self.tape)
        current = b642json(unpacked[nth])
        current.update(content)
        unpacked[nth] = json2b64(current)
        self.tape = ";".join(unpacked)
        labcommand("progressivis:set_backup", backup=self.tape)

    def amend_last_record(self, content: dict[str, AnyType]) -> None:
        self.amend_nth_record(-1, content)

    def get_last_record_index(self) -> int:
        return len(bunpack(self.tape)) - 1


def get_recorder() -> Recorder:
    return cast(Recorder, PARAMS.get("recorder"))

#
# Since Recorder instance is unique all Recorder methods are available
# via homonymes shortcut functions below:
#

def add_to_record(content: dict[str, AnyType]) -> None:
    rec = get_recorder()
    if rec is None:
        return
    rec.add_to_record(content)


def amend_last_record(content: dict[str, AnyType]) -> None:
    rec = get_recorder()
    if rec is None:
        return
    rec.amend_last_record(content)


def amend_nth_record(i: int, content: dict[str, AnyType]) -> None:
    rec = get_recorder()
    if rec is None:
        return
    rec.amend_nth_record(i, content)


def get_last_record_index() -> int | None:
    rec = get_recorder()
    if rec is None:
        return None
    return rec.get_last_record_index()


def reset_recorder(previous: str = "", init_val: str = "") -> None:
    if previous:
        PARAMS["previous_recorder"] = Recorder(previous)
    PARAMS["recorder"] = Recorder(value=init_val)
    labcommand("progressivis:set_backup", backup=init_val)


def replay_next(obj: Optional[Union["Constructor", "NodeVBox"]] = None) -> None:
    """
    Replay the next stage (c-widget). It is the first element in `replay_list`
    because elements are removed from the list before where replayed

    Args:
        obj: known when it is the `Constructor()` or in step-by-step mode. Else it is None
    """
    if not is_replay():
        return
    if not replay_list:
        return
    assert replay_list
    stage = replay_list.pop(0)
    parent = stage.get("parent", None)
    if (
        parent is not None and tuple(parent) in PARAMS["deleted_stages"]
    ):  # skipping deleted
        return replay_next_if()
    if "deleted" in stage:
        PARAMS["deleted_stages"].add((stage["title"], stage["number"]))
        return replay_next_if()
    if obj is None and stage and "ftype" not in stage:  # not a loader => has a parent
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


def replay_next_if(obj: Optional[Union["Constructor", "NodeVBox"]] = None) -> None:
    """
    Conditional replay_next. See `replay_next()` above
    """
    if is_replay_batch():
        return
    return replay_next(obj)


def replay_sequence(obj: "Constructor") -> None:
    """
    Replay the current scenario in batch mode. Triggered from the "Replay all" button in Constructor() interface
    The replay is done in two steps:

    1. The cell codes are executed synchronously (see the exception explained in `labcommand()`
    docstring) and the `widget_list` is filled. REPLAY_BATCH is switched to False which will
    change the labcommand() behaviour (accoding to the exception mentioned above).
    2. The `widget_list` is iterate over and the underlying cells are displayed
    """
    global REPLAY_BATCH
    REPLAY_BATCH = True
    md_list.clear()
    widget_list.clear()
    replay_next(obj)
    while True:
        replay_next()
        if not replay_list:
            break
    REPLAY_BATCH = False
    for md, code, tag in widget_list:
        tag_class = get_tag_class(tag)
        labcommand(
            "progressivis:create_stage_cells",
            tag=tag,
            tag_class=tag_class,
            md=md,
            code=code,
            rw=False,
            run=True,
        )


def create_root(backup: BackupWidget) -> None:
    """
    Function in charge of the bootstrap. It is called from the cell "zero"
    of the progressibook (see ipyprogressivis/blob/main/ipyprogressivis/js/nb/ProgressiBook.ipynb)
    and it must not be called elsewhere. It creates the "root" cells (two cells: markdown+code)
    """
    code = (
        "# do not run this cell\n"
        "display(header.constructor)\n"
        "header.constructor.start_scheduler()\n"
        "header.talker.labcommand('notebook:hide-cell-code')"
    )

    async def _func() -> None:
        await aio.sleep(0.2)
        extra = backup.root_markdown
        md = f"## root\n {extra}" if extra else "## root"
        labcommand(
            "progressivis:create_stage_cells",
            tag="root",
            tag_class="root",
            md=md,
            code=code,
            rw=False,
            run=True,
        )

    loop = asyncio.get_event_loop()
    loop.create_task(_func())


def set_dag(dag: DAGWidget) -> None:
    assert "dag_widget" not in PARAMS
    PARAMS["dag_widget"] = dag


WidgetType = AnyType


def get_param(d: dict[str, list[str]], key: str, default: list[str]) -> list[str]:
    "normalized missing or null value"
    if key not in d:
        return default
    val = d[key]
    if not val:
        return default
    return val




def norm_rename_cols(sniffer: Sniffer) -> list[str]:
    """
    auxiliary of get_schema()
    """
    return normalize_columns(sniffer.get_names())

def get_schema(sniffer: Sniffer) -> AnyType:
    """
    Formatting cols info generated by the sniffer to meet the needs of c-widgets
    """
    params = sniffer.params
    usecols = params.get("usecols")
    parse_dates = get_param(params, "parse_dates", [])
    retype = params.get("dtype", {}) or {}  # "dtype" key may exist and be None

    def _ds(col: str, dt: str) -> str:
        if col in parse_dates:
            return "datetime64"
        if isinstance(dt, pd.StringDtype):
            return dataframe_dshape(np.dtype(object))
        return dataframe_dshape(np.dtype(dt))

    assert hasattr(sniffer, "_df")
    assert sniffer._df is not None
    norm_cols = dict(zip(sniffer.get_names(), norm_rename_cols(sniffer)))
    dtypes = {col: _ds(col, dt)  # type: ignore
              for (col, dt) in sniffer.get_names_types().items()}
    for col, dt in retype.items():
        dtypes[col] = dt
    if usecols is not None:
        dtypes = {norm_cols[col]: dtypes[col] for col in usecols}
    else:
        dtypes = {norm_cols[col]: t for (col, t) in dtypes.items()}
    return dtypes


def make_replay_next_btn() -> ipw.Button:
    """
    Shortkut for creating a "Next" button with its appopriate callback.
    Currently used only by DumpTableW
    """
    def _fnc(btn: ipw.Button) -> None:
        replay_next()
        btn.disabled = True

    return make_button("Next", cb=_fnc, disabled=False)

#: Associates labels with widget types. Populated by the class decorator @chaining_widget
stage_register: dict[str, AnyType] = {} # NB: dict values are widget classes

# c-widget instances indexing

#: associates ids (made using id() calls) with (label, number) tuples
key_by_id: dict[int, Tuple[str, int]] = {}

#: associates ids (made using id() calls) with the actual widget instance
widget_by_id: dict[int, "NodeCarrier"] = {}

#: associates (label, number) ids with the actual widget instance
widget_by_key: dict[Tuple[str, int], "NodeCarrier"] = {}

#: numbering widget instances register
widget_numbers: dict[str, int] = defaultdict(int)



#parent_dtypes: Optional[dict[str, str]] = None

def get_tag_class(tag: str) -> str:
    key, nb = parse_tag(tag)
    node = widget_by_key[(key, nb)]
    return type(node.children[IGUEST]).__name__

parent_widget: Union["NodeCarrier", "Constructor"] | None = None

def set_parent_widget(obj: Union["NodeCarrier", "Constructor"]) -> None:
    """
    ...
    """
    global parent_widget
    parent_widget = obj


class _Dag:
    """
    Convenience wrapper for DAGWidget
    """
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

"""
Below, we'll distinguish between two types of widgets:
1. loader_widgets (currently CSV, PARQUET and Custom). They are always attached to the "root"
2. stage_widget (the others)

They are created via `create_stage_widget` and `create_loader_widget` below

A NodeCarrier is a vertical box widget that houses (and carry) the functional widget (a.k.a guest)
by framing it with button and display bars at the top and bottom.

The NodeCarrier is generic (identical for all stages) asd the guest is specific and implements a
fragment of the scenario.
"""

def create_stage_widget(
    key: str, alias: str, frozen: AnyType = None, number: int | None = None
) -> "NodeCarrier":
    """
    Create a NodeCarrier with its guest (not yet initialized). Link the created
    NodeCarrier to it's parent (var parent_widget). The link is bidirectional (
    the parent knows it's subwidgets)

    Args:
        key: the label provided via @chaining_widget(label="MyCWidget") class decorator
        alias: Custom name provided by the user (Ex: Scatterplot instead AnyVega)
        frozen: backup info
        number: numbering for widgets of the same type

    Returns:
        the created NodeCarrier
    """
    obj = parent_widget
    assert obj is not None
    dtypes = obj._output_dtypes
    #if dtypes is None:
    #    dtypes = parent_dtypes
    if number is not None and number > widget_numbers[key]:
        widget_numbers[key] = number
    number_ = widget_numbers[key] if number is None else number
    dag = _Dag(label=key, number=number_, dag=get_dag(), alias=alias)
    ctx = dict(parent=obj, dtypes=dtypes, input_module=obj._output_module, dag=dag)
    guest = stage_register[key]()  # the guest is created but not yet initialized
    guest.add_class("progressivis_guest_widget")
    if frozen is not None:
        guest.frozen_kw = frozen
    stage = NodeCarrier(ctx, guest)
    if is_recording():
        guest._record_index = cast(int, get_last_record_index()) + 1
    widget_numbers[key] += 1
    assert obj not in obj.subwidgets
    obj.subwidgets.append(stage)
    if alias:
        widget_by_key[(alias, 0)] = stage
        key_by_id[id(stage)] = (alias, 0)
        guest.add_class(make_css_marker(alias))
    else:
        widget_by_key[(key, stage.number)] = stage
        widget_by_id[id(stage)] = stage
        key_by_id[id(stage)] = (key, stage.number)
        guest.add_class(make_css_marker(key, stage.number))
    return stage


def create_loader_widget(
    key: str, ftype: str, alias: str, frozen: AnyType = None, number: int | None = None
) -> "NodeCarrier":
    """
    Create a NodeCarrier with its guest it is always a loader (csv, parquet, custom).

    Args:
        key: the label provided via @chaining_widget(label="MyCWidget") class decorator
        ftype: csv, parquet or custom. Specific parameter for loaders
        alias: Custom name provided by the user (Ex: Scatterplot instead AnyVega)
        frozen: backup info
        number: numbering for widgets of the same type

    Returns:
        the created NodeCarrier
    """
    obj = parent_widget
    dtypes = None
    assert obj is not None
    assert obj not in obj.subwidgets
    if number is not None and number > widget_numbers[key]:
        widget_numbers[key] = number
    number_ = widget_numbers[key] if number is None else number
    dag = _Dag(label=key, number=number_, dag=get_dag(), alias=alias)
    ctx = dict(parent=obj, dtypes=dtypes, input_module=obj._output_module, dag=dag)
    from .csv_loader import CsvLoaderW
    from .parquet_loader import ParquetLoaderW
    from .snippet import SnippetW

    loader: CsvLoaderW | ParquetLoaderW | SnippetW
    if ftype == "csv":
        loader = CsvLoaderW()
    elif ftype == "parquet":
        loader = ParquetLoaderW()
    else:
        assert ftype == "custom"
        loader = SnippetW()
    if frozen is not None:
        loader.frozen_kw = frozen
    stage = NodeCarrier(ctx, loader)
    loader.add_class("progressivis_guest_widget")
    widget_numbers[key] += 1
    obj.subwidgets.append(stage)
    widget_by_id[id(stage)] = stage
    if alias:
        widget_by_key[(alias, 0)] = stage
        key_by_id[id(stage)] = (alias, 0)
        loader.add_class(make_css_marker(alias))
    else:
        widget_by_key[(key, stage.number)] = stage
        key_by_id[id(stage)] = (key, stage.number)
        loader.add_class(make_css_marker(key, stage.number))
    return stage


def get_widget_by_id(key: int) -> "NodeCarrier":
    return widget_by_id[key]


def get_widget_by_key(key: str, num: int) -> "NodeCarrier":
    return widget_by_key[(key, num)]


#: global variable used only by the following 2 functions
recording_state: bool = False

def is_recording() -> bool:
    return recording_state

def set_recording_state(val: bool) -> None:
    global recording_state
    recording_state = val



def replay_start_loader(
    obj: "NodeCarrier",
    ftype: str,
    alias: str,
    frozen: AnyType | None = None,
    number: int | None = None,
    **kw: AnyType,
) -> None:
    """
    Calling `add_new_loader()` in a replay context. The parent is "forced" at each step.
    """
    #global parent_widget
    #parent_widget = obj
    set_parent_widget(obj)
    assert parent_widget
    add_new_loader(
        obj, ftype, alias, frozen=frozen, number=number, markdown=kw.get("markdown", "")
    )


def replay_new_stage(
    obj: "NodeCarrier",
    title: str,
    alias: str,
    frozen: AnyType | None = None,
    number: int | None = None,
    **kw: AnyType,
) -> None:
    """
    See above
    """
    set_parent_widget(obj)
    add_new_stage(
        obj,
        title,
        alias=alias,
        frozen=frozen,
        number=number,
        markdown=kw.get("markdown", ""),
    )


class ChainingProtocol(Protocol):
    """
    Allows duck typing via typing.Protocol for the main
    chaining mechanism
    """
    _output_dtypes: dict[str, str] | None
    _output_module: ModuleOrFacade
    _chain_it_btn: ipw.Button | None = None
    _chain_it_sel: ipw.Dropdown | None = None
    title: str
    guest: "GuestWidget"

    def _make_btn_chain_it_cb(
        self,
        sel: AnyType,
        alias: AnyType,
        frozen: AnyType | None = None,
        number: int | None = None,
    ) -> Callable[..., None]: ...


class ChainingMixin:
    """
    Implements the chaining mechanism
    """
    _output_module: ModuleOrFacade
    _output_dtypes: dict[str, str] | None
    managed_modules: set[str]
    _chain_it_btn: ipw.Button | None = None
    _chain_it_sel: ipw.Dropdown | None = None

    def _make_btn_chain_it_cb(
        self: ChainingProtocol,
        sel: AnyType,
        alias: AnyType,
        frozen: AnyType = None,
        number: int | None = None,
    ) -> Callable[..., None]:
        """
        Chaaining button callback
        """
        def _cbk(btn: ipw.Button) -> None:
            #global parent_widget
            if sel.value in LOADERS:
                cons = PARAMS["constructor"]
                #parent_widget = cons
                set_parent_widget(cons)
                add_new_loader(cons, LOADERS[sel.value], alias.value, frozen)
            else:
                #parent_widget = self  # type: ignore
                set_parent_widget(self)  # type: ignore
                add_new_stage(self, sel.value, alias.value, frozen=frozen, number=number)  # type: ignore
            sel.value = ""

        return _cbk

    def _progress_bar(self) -> ipw.IntProgress:
        """
        create the progress bar widget with the underlying logic
        """
        prog_wg = ipw.IntProgress(
            description="Progress", min=0, max=1000, layout={"width": "100%"}
        )
        mod_ = self._output_module
        if not isinstance(mod_, Module):
            mod_ = mod_.module  # i.e. mod_ is a Facade

        def _proc(m: Module, r: int) -> None:
            val_, max_ = m.get_progress()
            prog_wg.value = val_
            if prog_wg.max != max_:
                prog_wg.max = max_

        mod_.on_after_run(_proc)
        return prog_wg

    def _quality_bar(self) -> QualityVisualization | None:
        """
        create the quality bar widget with the underlying logic
        """
        from ipyprogressivis.views.quality import display_quality

        scheduler = self._output_module.scheduler
        scheduler._update_modules()
        modules = scheduler.modules()
        managed_m = [
            m
            for (n, m) in modules.items()
            if n in self.managed_modules and Module.TAG_QUALITY in m.tags
        ]
        if not managed_m:
            return None
        qv = display_quality(managed_m)
        qv.width = QUAL_W  # type: ignore
        qv.height = QUAL_H  # type: ignore
        return qv

    def _make_footer(self: ChainingProtocol, batch: bool = False) -> ipw.Box:
        """
        Creates the main footer bar (implementing chaining options)
        """
        after_run_bar = None
        guest = self.guest
        if hasattr(guest, "after_run"):
            guest.after_run.leaf = guest
            after_run_bar = guest.after_run.bar
        prog_wg = self._progress_bar() if guest._show_progress else None  # type: ignore
        qual_wg = self._quality_bar() if guest._show_quality else None  # type: ignore
        if guest._is_chainable and not batch:
            self._chain_it_sel = sel = ipw.Dropdown(
                options=[""]
                + list(sorted(stage_register.keys()))
                + list(LOADERS.keys()),
                value="",
                description="Next stage",
                disabled=False,
            )
            alias = ipw.Text(
                value="",
                placeholder="optional alias",
                description="",
                disabled=False,
                style={"description_width": "initial"},
            )
            def _on_sel_change(change: Any) -> None:
                if change["new"] and self._output_dtypes is not None:
                    btn.disabled = False
                    alias.value = ""
                else:
                    btn.disabled = True

            self._chain_it_btn = btn = make_button(
                "Chain it", disabled=True, cb=self._make_btn_chain_it_cb(sel, alias)
            )
            sel.observe(_on_sel_change, names="value")
            chaining_ = ipw.HBox([sel, alias, btn])
        else:
            chaining_ = None
        children_ = [
            elt
            for elt in (after_run_bar, prog_wg, qual_wg, chaining_)
            if elt is not None
        ]
        return ipw.VBox(children_)


def get_previous(obj: "ChainingWidget") -> "ChainingWidget":
    if not obj.subwidgets:
        return obj
    return get_previous(obj.subwidgets[-1])


def is_replay() -> bool:
    return cast(bool, PARAMS.get("is_replay", False))


def is_replay_only() -> bool:
    return is_replay() and not PARAMS["step_by_step"]


def is_step() -> bool:
    return cast(bool, PARAMS["step_by_step"])


def is_replay_batch() -> bool:
    return REPLAY_BATCH and not PARAMS["step_by_step"]

#: template instanciate below
new_stage_cell = "{begin}Constructor.widget('{key}', {num}){end}"

def get_stage_cell(
    key: str, num: int, end: str, frozen: AnyType = None
) -> tuple[str, bool, bool]:
    begin = "await " if end else ""
    return new_stage_cell.format(key=key, num=num, begin=begin, end=end), False, True


def get_loader_cell(
    key: str, ftype: str, num: int, end: str, frozen: AnyType = None
) -> tuple[str, bool, bool]:
    begin = "await " if end else ""
    return new_stage_cell.format(key=key, num=num, begin=begin, end=end), False, True


def add_new_stage(
    parent: "ChainingWidget",
    title: str,
    alias: str,
    frozen: AnyType = None,
    number: int | None = None,
    markdown: str = "",
) -> None:
    """
    - create the widget (carrier+guest) via `create_stage_widget()`
    - initialize the guest widget,
    - prepare the content for the 2 cells (mardown+code) to be created
    - send (via labcommand()) the jupyterlab command which creates the 2 cells and execute them
    - create the record for replay
    """
    stage = create_stage_widget(title, alias, frozen, number=number)
    parent_key = key_by_id[id(parent)]
    n = stage.number
    end = ""
    if frozen is not None and is_replay():
        end = ".replay()"
        stage._is_replaying = True
    stage.children[IGUEST].initialize()  # type: ignore
    if alias:
        md = f"## {alias}"
        tag = alias
    else:
        tag = title + (f"[{n}]" if n else "")
        md = "## " + tag
    if markdown:
        md = md + "\n" + markdown
    code, rw, run = get_stage_cell(key=alias or title, num=n, end=end, frozen=frozen)
    tag_class = get_tag_class(tag)
    labcommand(
        "progressivis:create_stage_cells",
        frozen=frozen,
        tag=tag,
        tag_class=tag_class,
        md=md,
        code=code,
        rw=rw,
        run=run,
        markdown=markdown,
    )
    add_to_record(
        dict(
            title=title,
            parent=parent_key,
            number=stage.number,
            alias=alias,
            frozen=frozen,
            markdown=markdown,
        )
    )


def add_new_loader(
    parent: "ChainingWidget",
    ftype: str,
    alias: str,
    frozen: AnyType = None,
    number: int | None = None,
    markdown: str = "",
) -> None:
    """
    Same tasks as `add_new_stage()` for loaders
    """
    title = f"{ftype.upper()} loader"
    stage = create_loader_widget(title, ftype, alias, frozen=frozen, number=number)
    n = stage.number
    end = ""
    if frozen is not None and is_replay():
        end = ".replay()"
        stage._is_replaying = True
    stage.children[IGUEST].initialize()  # type: ignore
    if alias:
        md = f"## {alias}"
        tag = alias
    else:
        tag = title + (f"[{n}]" if n else "")
        md = "## " + tag
    if markdown:
        md = md + "\n" + markdown
    code, rw, run = get_loader_cell(
        key=alias or title, ftype=ftype, num=n, end=end, frozen=frozen
    )
    tag_class = get_tag_class(tag)
    labcommand(
        "progressivis:create_stage_cells",
        frozen=frozen,
        tag=tag,
        tag_class=tag_class,
        md=md,
        code=code,
        rw=rw,
        run=run,
        markdown=markdown,
    )
    add_to_record(
        dict(
            title=title,
            number=stage.number,
            ftype=ftype,
            alias=alias,
            frozen=frozen,
            markdown=markdown,
        )
    )


class ChainingWidget:
    """
    Base class for "carrier" widgets
    """
    def __init__(self, kw: Any) -> None:
        assert "parent" in kw
        self.parent: Optional["NodeVBox"] = kw["parent"]
        assert "dtypes" in kw
        self._dtypes: dict[str, str] = kw["dtypes"]
        assert "input_module" in kw
        self._input_module: ModuleOrFacade = cast(ModuleOrFacade, kw["input_module"])
        self._input_slot: str = kw.get("input_slot", "result")
        self._output_module: ModuleOrFacade = self._input_module
        self._output_slot: str = self._input_slot
        self._output_dtypes: Optional[dict[str, str]] = None
        if self._dtypes is not None:  # i.e. not a loader
            self._output_dtypes = None
        self._dag = kw["dag"]
        self.subwidgets: list[ChainingWidget] = []
        self.managed_modules: set[str] = set()

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
        return self.title.replace(" ", "-").replace(".", "_")

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
    """
    Base class for functional widgets (like GroupByW, HeatmapW etc) a.k.a "guests"
    """
    _show_progress: bool = True
    _show_quality: bool = True
    _is_chainable: bool = True

    def __init__(self) -> None:
        self.__carrier: Union[int, ReferenceType["NodeCarrier"]] = 0
        self.frozen_kw: dict[str, Any]
        self._do_replay_next: bool = False
        self._record_index: int = 0
        self._proxy: Proxy | None = None

    def process_replay(self) -> None:
        """
        when replay it restore the widget tree from the backup (see ipywel for details)
        """
        if self.is_replaying:
            cls = type(self)
            m = importlib.import_module(cls.__module__)
            content = self.record
            self._proxy = restore(content, m.__dict__, obj=self, custom=dict(JsonEditor=json_editor))
            assert hasattr(self._proxy.widget, "children")
            self.children = self._proxy.widget.children
            #for bk in self._proxy._backends.values():
            #    bk().update_backend(self._proxy)

    def initialize(self) -> None:
        pass

    @property
    def carrier(self) -> "NodeCarrier":
        assert not isinstance(self.__carrier, int)
        return cast("NodeCarrier", self.__carrier())

    @property
    def is_replaying(self) -> bool:
        return self.carrier._is_replaying

    @property
    def dtypes(self) -> dict[str, str]:
        return self.carrier._dtypes

    @property
    def input_dtypes(self) -> dict[str, str]:
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
    def output_dtypes(self) -> Optional[dict[str, str]]:
        return self.carrier._output_dtypes

    @output_dtypes.setter
    def output_dtypes(self, value: dict[str, str]) -> None:
        self.carrier._output_dtypes = value

    @property
    def parent(self) -> "VBox":
        assert isinstance(self.carrier, NodeCarrier)
        assert self.carrier.parent is not None
        assert len(self.carrier.parent.children)
        return cast("VBox", self.carrier.parent.children[IGUEST])

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
        return cast("VBox", widget_by_key[key].children[IGUEST])

    def dag_running(self) -> None:
        self.carrier.dag_running()

    def make_footer(self, batch: bool = False) -> None:
        self.carrier.make_footer(batch=batch)

    @property
    def record(self) -> dict[str, Any]:
        return self.frozen_kw

    @record.setter
    def record(self, value: dict[str, Any]) -> None:
        if is_recording():
            if isinstance(value, dict):
                value["previous_input_dtypes"] = self.input_dtypes
            amend_last_record({"frozen": value})

    @property
    def widget_dir(self) -> str:
        """
        Get the settings path for the current guest type, i.e.
        ~/.progressivis/widget_settings/MyGuestW

        Returns:
            thedir path
        """
        pv_dir = dot_progressivis()
        if not pv_dir:
            return ""
        settings_dir = f"{pv_dir}/widget_settings/"
        if not os.path.isdir(settings_dir):
            os.mkdir(settings_dir)
        widget_dir = f"{settings_dir}/{type(self).__name__}/"
        if not os.path.isdir(widget_dir):
            os.mkdir(widget_dir)
        return widget_dir

    def init_ui(self) -> None:
        pass

    def post_run(self, title: str) -> "NodeCarrier":
        """
        Post processing triggered by the "Next" button
        to ensure continuity
        (see @runner decorator)
        """
        self.dag_running()
        self.carrier.children = (
            BTN_DEL,
            self,
        )  # type: ignore
        batch = not PARAMS["replay_before_resume"]
        self.make_footer(batch=batch)
        replay_next_if(self.carrier)
        return self.carrier

    def post_delete(self) -> "NodeCarrier":
        """
        Post processing triggered by the "Delete" button
        to ensure the netx step after delete
        (see @runner decorator)
        """
        self.carrier.children = (
            BTN_DEL,
            ipw.Label("deleted"),
        )
        replay_next_if()
        return self.carrier

    def manage_replay(self) -> None:
        """
        Used by the @starter_callback post processing
        to ensure continuity
        """
        if self._do_replay_next:
            replay_next_if()


class VBox(ipw.VBox, GuestWidget):
    """
    We opted for multiple inheritance rather than having GuestWidget inherit directly
    from VBox, to account for the possibility that GuestWidget might be another kind
    of container (HBox, Tab etc.)
    """
    def __init__(self, *args: Any, **kw: Any) -> None:
        ipw.VBox.__init__(self, *args, **kw)
        GuestWidget.__init__(self)



class LeafVBox(ipw.VBox, ChainingWidget):
    """
    We opted for multiple inheritance here for the same reasons as above
    """
    def __init__(
        self, ctx: dict[str, Any], children: Sequence[GuestWidget] = ()
    ) -> None:
        ipw.VBox.__init__(self, children)
        ChainingWidget.__init__(self, ctx)
        self.dag_register()


class NodeVBox(LeafVBox, ChainingMixin):
    """
    Abstract super-class of NodeCarrier
    """
    def __init__(
        self, ctx: dict[str, Any], children: Sequence[GuestWidget] = ()
    ) -> None:
        super().__init__(ctx, children)
        self.dag_register()


class RootVBox(LeafVBox):
    """
    Inherited by the Constructor
    """
    def __init__(
        self, ctx: dict[str, Any], children: Sequence[GuestWidget] = ()
    ) -> None:
        super().__init__(ctx, children)
        self.dag_register()


class NodeCarrier(NodeVBox):
    """
    Currently this is the unique wrapper for all "guests" in use
    """
    def __init__(self, ctx: dict[str, Any], guest: GuestWidget) -> None:
        super().__init__(
            ctx,
            (make_trash_box(self), guest),  # type: ignore
        )
        guest._GuestWidget__carrier = ref(self)  # type: ignore
        self._is_replaying = False
        self.dag_register()

    def run(self) -> None:
        assert self.children[IGUEST].frozen_kw is not None  # type: ignore
        self._is_replaying = True
        self.children[IGUEST].initialize()  # type: ignore
        return self.children[IGUEST].run()  # type: ignore

    async def replay(self) -> None:
        # TODO: add here the logic for waiting input_dtypes to be computed
        assert self.children[IGUEST].frozen_kw is not None  # type: ignore
        self._is_replaying = True
        self.children[IGUEST].initialize()  # type: ignore
        return self.children[IGUEST].run()  # type: ignore

    def make_footer(self, batch: bool = False) -> None:
        if len(self.children) > BOX_SIZE:
            raise ValueError("The chaining box already exists")
        box = self._make_footer(batch=batch)  # type: ignore
        if not box:
            return
        self.children = (self.children[ITRASH], self.children[IGUEST], box)
        # self.children = list(self.children) + [box]

    @property
    def guest(self) -> GuestWidget:
        return cast(GuestWidget, self.children[IGUEST])

    @property
    def scheduler(self) -> "Scheduler":
        return self.guest.input_module.scheduler

    @property
    def profiler(self) -> Any:
        return self.scheduler.profiler

    def dump_stats(self, fname: str | None = None) -> str:
        if fname is None:
            fname = f"/tmp/ipyprogressivis_stats{id(self)}.prof"
        self.profiler.dump_stats(fname)
        return fname

class TypedBase:
    """
    TypedBase and TypedBox are the two base classes that allow for the definition of
    typed containers. One advantage of typed containers is the ability to define paths
    to widgets within the container.

    A typed container must:

    1. inherit from TypedBox + an ipywidgets container (ex: VBox)
    2. define an inner class always named Types which inherits from TypedBase

    This technique allows defining clean access paths to an inner widgets this way:
    `self.child.widget1.child.widget2` etc.
    For a concrete example, take a look at Constructor class and also at slides 28-37 (in french)
    of https://sed.saclay.inria.fr/demandez-le-programme/presentations/2023-06-27.pdf

    This technique was initially used for all guest widgets and has been replaced by ipywel
    wherever widget persistence was required. It is still used in the definition of the
    Constructor widget (constructor.py) and CoroBar below.
    """
    def __init__(self) -> None:
        self._main: Optional[ReferenceType["TypedBox"]] = None

    @property
    def main(self) -> "TypedBox":
        assert self._main is not None
        return cast("TypedBox", self._main())

    def __setattr__(self, name: str, value: ipw.DOMWidget) -> None:
        super().__setattr__(name, value)
        if (
            type(self).__annotations__
            and name in type(self).__annotations__
            and self._main is not None
        ):
            if not self.main.children:
                self.main.children = [
                    dongle_widget() for _ in type(self).__annotations__.keys()
                ]
            if value is None:
                value = dongle_widget()
            self.main.set_child(name, value)


class TypedBox:
    """
    See explanations above
    """
    Typed: type

    def __init__(self) -> None:
        self.child = self.Typed()
        self.c_ = self.child
        self.child._main = ref(self)
        self.children: Sequence[ipw.DOMWidget] = ()

    def set_child(self, name: str, child: ipw.DOMWidget) -> None:
        schema = list(type(self.child).__annotations__)  # TODO: cache it
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


class CoroBar(IpyHBoxTyped):
    class Typed(TypedBase):
        display_t: ipw.IntSlider
        is_active: ipw.Checkbox
        message: ipw.HTML


class Coro:
    """
    A base class that allows you to write `after_run` callbacks with a predefined,
    consistent rendering for all guest widgets. To use it, the guest must define
    a subclass of `Coro` that implements the `action` method
    """
    __name__ = "action"  # raise clean exceptions in Module

    def __init__(self, m: Module | None = None) -> None:
        self.leaf: GuestWidget | None = None  # TODO: use a weakref here
        self._last_display: int = 0
        self.calls_counter: int = 0
        self.bar = CoroBar()
        self.bar.c_.display_t = ipw.IntSlider(
            value=1,
            min=1,
            max=10,
            step=1,
            description="Display T:",
            style={"description_width": "initial"},
            disabled=False,
            continuous_update=False,
            orientation="horizontal",
            readout=True,
            readout_format="d",
        )
        self.bar.c_.is_active = ipw.Checkbox(
            description="Active", value=True, disabled=False
        )
        self.bar.c_.message = ipw.HTML()
        if m is not None:
            m.on_after_run(self)

    async def action(self, m: Module, run_n: int) -> None:
        """
        Mandatory in any subclass
        """
        raise ValueError("'action' method must be defined in a 'Coro' subclass")

    async def __call__(self, m: Module, run_n: int) -> None:
        if not self.bar.c_.is_active.value:
            return
        now = int(time.time())
        if now - self._last_display < self.bar.child.display_t.value:
            return
        await self.action(m, run_n)
        self._last_display = int(time.time())
        self.calls_counter += 1

def restore_on_replay(to_decorate: Callable[..., AnyType]) -> Callable[..., AnyType]:
    """
    Decorate the ``initialize()`` method whenever restoration is desired during replay and
    the restoration does not involve any special considerations
    NB: You don't have to apply this decorator yourself.
    This decorator is silently applied on the ``initialize()`` method via the ``@chaining_widget``
    class decorator unless initialize() is decorated with ``@customized_restore``

    Args:
        to_decorate: the `initialize()` method

    Returns:
        the wrapped `initialized()` method
    """

    @wraps(to_decorate)
    def _wrapper(self_: GuestWidget, *args: AnyType, **kwargs: AnyType) -> AnyType:
        """
        Get a trace of modules created by to_decorate() method
        """
        if self_.is_replaying:
            return self_.process_replay()
        return to_decorate(self_, *args, **kwargs)
    # _wrapper._already_decorated = True  # avoids to decorate twice?
    return _wrapper

def customized_restore(to_decorate: Callable[..., AnyType]) -> Callable[..., AnyType]:
    """
    Decorate the ``initialize()`` with customized_restore to prevent this method to be silently
    decorated with ``restore_on_replay``

    Args:
        to_decorate: the `initialize()` method

    Returns:
        the function ``to_decorate`` tagged with the ``_customized_restore``
    """
    to_decorate._customized_restore = True  # type: ignore
    return to_decorate

def output_dtypes_proc_factory(guest: GuestWidget) -> Callable[..., AnyType]:
    """
    Provides an `on_after_run` callback capable tu provide the module output_dtypes when
    missing. Triggered on `@modules_producer`

    Args:
        guest: a GuestWidget

    Returns:
        the callback capable to provide  output_dtypes
    """
    async def dtype_proc_cb(m: Module, run_n: int) -> None:
        res = getattr(m, guest.output_slot, None)
        if res is None:
            return
        dtypes = {
            k: "datetime64" if str(v)[0] == "6" else str(v)
            for (k, v) in  dshape_fields(res.dshape)
        }
        guest.output_dtypes = dtypes
        for fnc in m._after_run:
            if getattr(fnc, "__name__", None) == "dtype_proc_cb":
                m._after_run.remove(fnc)
                break
        carrier = guest.carrier
        if carrier._chain_it_btn is not None and carrier._chain_it_sel is not None and carrier._chain_it_sel.value:
            carrier._chain_it_btn.disabled = False


    return dtype_proc_cb

def modules_producer(to_decorate: Callable[..., AnyType]) -> Callable[..., AnyType]:
    """
    Decorator for method which create modules (usually named `init_modules()`)
    Serves two purposes:

    1. Determine the list of modules created by the current stage (useful on stage deletion)
    2. Compute triggers output_dtypes_proc_factory (see above)

    NB: In the vast majority of cases, you don't have to apply this decorator yourself.
    This decorator is silently applied on the `init_modules()` method via the `@chaining_widget`
    class decorator. Only if `init_modules()` is inherited from a class which is not `@chaining_widget`
    decorated you have to explicitly decorate the `init_modules()` method with `@modules_producer`

    """

    @wraps(to_decorate)
    def _wrapper(self_: GuestWidget, *args: AnyType, **kwargs: AnyType) -> AnyType:
        """
        Get a trace of modules created by to_decorate() method
        """
        s = self_.input_module.scheduler
        mods_before = set(s.modules().keys())
        ret_m = to_decorate(self_, *args, **kwargs)
        if s.dataflow:
            mods_after = set(s.dataflow.modules().keys())
        else:
            mods_after = set(s.modules().keys())
        self_.carrier.managed_modules = mods_after.difference(mods_before)
        if ret_m is not None and self_.output_dtypes is None:
            try:
                ret_m.on_after_run(output_dtypes_proc_factory(self_))
            except AttributeError:
                if isinstance(ret_m, SnippetResult):
                    ret_m.output_module.on_after_run(output_dtypes_proc_factory(self_))
                else:
                    raise
        return ret_m
    # _wrapper._already_decorated = True  # avoids to decorate twice?
    return _wrapper


def chaining_widget(label: str) -> Callable[..., AnyType]:
    """
    Provide the decorator which registers a Guest with a displayable label
    It is mandatory on all GuestWidget subclasses
    """
    def decorator(cls: AnyType) -> AnyType:
        stage_register[label] = cls
        if hasattr(cls, "run"):
            cls.run = runner(cls.run)
        else:
            pass # logger.warning(f"'run()' method is missing on {cls.__name__}")
        if hasattr(cls, "init_modules"):
            cls.init_modules = modules_producer(cls.init_modules)
        else:
            pass # logger.warning(f"'init_modules()' method is missing on {cls.__name__}")
        if hasattr(cls, "starter_callback"):
            cls.starter_callback = starter_callback(cls.starter_callback)
        else:
            pass # logger.warning(f"'starter_callback()' method is missing on {cls.__name__}")
        if not hasattr(cls.initialize, "_customized_restore"):  # initialize() is mandatory
            cls.initialize = restore_on_replay(cls.initialize)
        return cls

    return decorator


def starter_callback(
    func: Callable[..., AnyType] | None = None,
    *,
    disable_btn: bool = True,
    disable_ui: bool = True,
    footer: bool = True,
    dag_running: bool = True,
    manage_display: bool = True,
) -> Callable[..., AnyType]:
    """
    Since a widget can contain multiple buttons, the `@starter_callback` decorator is used
    only on the button that triggers the processing to declare its role. The decorator
    will add post-processing specific to that role. When no button push (or any other event)
    is necessary to start the c-widget @starter_callback must decorate the ``initialize()``
    method (ex: ``DumpTableW``)

    NB: Technically ``starter_callback(...)`` with parenthesis is not a decorator but a
    function returning a decorator. In the vast majority of cases, you don't have to apply
    ``starter_callback(...)`` yourself.
    If all default parameter values are convenient for you, it's enough to give the name
    "starter_callback" ``to func``. In this case, ``starter_callback`` is silently applied on the
    ``func`` method via the ``@chaining_widget`` class decorator. On the other hand, if you want to
    apply ``@starter_callback`` with customized (i.e. non default) parameter values you have to
    explicitly decorate the ``func`` callback with the present decorator. In this case the name
    of func must be different from "starter_callback" (for an example see SnippetW)

    Args:
        func: the raw callback
        disable_btn: disabled the current button after being clicked (to prevent other clicks)
        disable_ui: disable the entire UI after current button being clicked when subsequent UI
            actions are not pertinent anymore after current button being clicked
        footer: when False prevent displaying the standard footer (used when displaying
            the footer is not pertinent)
        dag_running: mark the underlying node as "running" in the DAG widget
        manage_display: allow chaining the next step

    Returns:
        the parameterized decorator
    """
    def decorator(func: Callable[..., AnyType]) -> Callable[..., AnyType]:
        @wraps(func)
        def wrapper(
            self_: GuestWidget,
            btn: ipw.Button | None = None,
            *args: AnyType,
            **kw: AnyType,
        ) -> AnyType:
            if btn:
                ret = func(self_, btn, *args, **kw)
            else:
                ret = func(self_, *args, **kw)
            if btn and disable_btn:
                btn.disabled = True
            if disable_ui:
                disable_all(self_)
            if footer:
                self_.make_footer()
            if dag_running:
                self_.dag_running()
            if manage_display:
                self_.manage_replay()
            return ret

        return wrapper

    if func is None:
        return decorator
    return decorator(func)


def is_leaf(cls: Type[GuestWidget]) -> Type[GuestWidget]:
    cls._is_chainable = False
    return cls


def no_progress_bar(cls: Type[GuestWidget]) -> Type[GuestWidget]:
    cls._show_progress = False
    return cls


def no_quality_bar(cls: Type[GuestWidget]) -> Type[GuestWidget]:
    cls._show_quality = False
    return cls
