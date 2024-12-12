"""
Jupyter notebook magics for (Ipy)Progressivis
"""

from __future__ import annotations

import os
import json
from pathlib import Path
from IPython.core.magic import (
    Magics,
    magics_class,
    cell_magic,
    line_magic,
    line_cell_magic,
    needs_local_scope,
)
from IPython.display import display
import ipywidgets as ipw
from IPython.core import magic_arguments as ma
from .widgets.chaining.utils import amend_nth_record, labcommand, make_button
from ipyprogressivis.widgets.chaining.utils import get_header
from progressivis.core import aio
import asyncio
from typing import Any

HOME = os.getenv("HOME")
assert HOME is not None


@magics_class
class IpyProgressivisMagic(Magics):
    def _save_cell(self, base_name: str, force: bool, cell: str) -> None:
        assert base_name.endswith(".py")
        assert HOME is not None
        home = Path(HOME)
        pv_dir = home / ".progressivis/"
        if not os.path.isdir(pv_dir):
            print(f"no {pv_dir} directory found, quit")
            return
        settings_dir = pv_dir / "widget_settings"
        if not os.path.isdir(settings_dir):
            if force:
                os.mkdir(settings_dir)
            else:
                print(
                    f"no {settings_dir} directory found,"
                    " use `--force` or `-f` option to create it."
                )
                return
        py_dir = settings_dir / "Python"
        if not os.path.isdir(py_dir):
            if force:
                os.mkdir(py_dir)
            else:
                print(
                    f"no {py_dir} directory found,"
                    " use `--force` or `-f` option to create it."
                )
                return
        py_name = py_dir / base_name
        if os.path.exists(py_name):
            if not force:
                print(
                    f"no {py_name} already exists,"
                    " use `--force` or `-f` option to overwrite it."
                )
                return
        with open(py_name, "w") as f:
            f.write(cell)
            print(f"cell saved in {py_name}")

    def _get_py_files_path(self) -> Path:
        assert HOME is not None
        home = Path(HOME)
        return home / ".progressivis" / "widget_settings" / "Python"

    @ma.magic_arguments()  # type: ignore
    @ma.argument(  # type: ignore
        "--force",
        "-f",
        action="store_true",
        help="""
        Force writings
        """,
    )
    @ma.argument("base_name", type=str, help="A Python file name")  # type: ignore
    @cell_magic  # type: ignore
    def pv_save_cell(self, line: str, cell: str) -> Any:
        args = ma.parse_argstring(self.pv_save_cell, line)  # type: ignore
        self._save_cell(args.base_name, args.force, cell)

    @ma.magic_arguments()  # type: ignore
    @ma.argument("--proxy", "-p",  help="The proxy id")  # type: ignore
    @ma.argument(  # type: ignore
        "--file",
        "-f",
        default="",
        help="""
        Python file to run
        """,
    )
    @ma.argument(  # type: ignore
        "--save",
        "-s",
        default="",
        help="""
        Save the cell (do not run)
        """,
    )
    @ma.argument(  # type: ignore
        "--overwrite",
        "-o",
        action="store_true",
        help="""
        Force writings
        """,
    )
    @line_cell_magic  # type: ignore
    @needs_local_scope  # type: ignore
    def pv_run_cell(self, line: str, cell: str = "",
                    local_ns: dict[str, Any] | Any = None) -> Any:
        args = ma.parse_argstring(self.pv_run_cell, line)  # type: ignore
        if args.save:
            self._save_cell(args.save, args.overwrite, cell)
            return
        if args.file and cell:
            print("The --file(-f) option and cell code are mutually exclusive."
                  " You must choose one of them!")
            return
        if args.file:
            with open(self._get_py_files_path() / args.file) as f:
                content = f.read()
        else:
            assert cell
            content = cell
        wg, ns = args.proxy.split(",")
        n = int(ns)
        Constructor = local_ns["Constructor"]
        proxy = Constructor.proxy(wg, n)
        proxy.cell_content = "\n".join([f"%%pv_run_cell -p {args.proxy}", content])
        local_ns["proxy"] = proxy
        self.shell.run_cell(content)  # type: ignore

    @ma.magic_arguments()  # type: ignore
    @ma.argument("--proxy", "-p",  help="The proxy id")  # type: ignore
    @ma.argument("base_name", type=str, help="A Python file name")  # type: ignore
    @line_magic  # type: ignore
    @needs_local_scope  # type: ignore
    def pv_run_file(self, line: str, local_ns: dict[str, Any] | Any = None) -> Any:
        # print("run_file", line, local_ns)
        args = ma.parse_argstring(self.pv_run_file, line)  # type: ignore
        print("args", args)
        print("pair", args.proxy)
        wg, ns = args.proxy.split(",")
        n = int(ns)
        Constructor = local_ns["Constructor"]
        proxy = Constructor.proxy(wg, n)
        local_ns["proxy"] = proxy
        with open(self._get_py_files_path() / args.base_name) as f:
            content = f.read()
        local_ns["__pv_cell__"] = "%pv_run_file " + line
        self.shell.run_cell(content)  # type: ignore

    def _parse_header(self, header: str) -> tuple[str, int]:
        if "[" not in header:
            return header, 0
        assert header[-1] == "]"
        pos = header.index("[")
        return header[:pos], int(header[pos+1:-1])

    def _sync_markdown(self, btn: Any) -> None:
        header = get_header()
        header.backup.load()

        async def _func() -> None:
            for i in range(3):
                await aio.sleep(1)
                self.bar.value = i+1
            arch_dict = {}
            for i, stage in enumerate(header.constructor._arch_list):
                key1 = stage.get("alias", "") or stage["title"]
                key2 = stage["number"]
                arch_dict[(key1, key2)] = i
            cells = json.loads(header.backup.markdown)  # type: ignore
            for cell in cells:
                if not cell.startswith("## "):
                    continue
                if "\n" not in cell:
                    continue
                head, extra = cell.split("\n", 1)
                if not extra:
                    continue
                wg = head[3:].strip()  # i.e. without "## "
                wg, nb = self._parse_header(wg)
                rec_index = arch_dict[(wg, nb)]
                amend_nth_record(rec_index, {"markdown": extra})
            self.lab.value = "Don't forget to save your work!"
        loop = asyncio.get_event_loop()
        loop.create_task(_func())

    def _unlock_markdown(self, btn: Any) -> None:
        labcommand("progressivis:unlock_markdown_cells")

    @line_magic  # type: ignore
    def pv_markdown(self, line: str) -> Any:
        self.bar = ipw.IntProgress(
            description="", min=0, max=3, value=0
        )
        self.lab = ipw.Label("")
        unlock_btn = make_button("Unlock md. cells", cb=self._unlock_markdown)
        sync_btn = make_button("Sync md. cells", cb=self._sync_markdown)
        hbox = ipw.HBox([unlock_btn, sync_btn])
        vbox = ipw.VBox([hbox, self.bar, self.lab])
        display(vbox)  # type: ignore


def load_ipython_extension(ipython: Any) -> None:
    ipython.register_magics(IpyProgressivisMagic)
