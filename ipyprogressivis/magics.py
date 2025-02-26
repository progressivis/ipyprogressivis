"""
Jupyter notebook magics for (Ipy)Progressivis
"""

from __future__ import annotations

import json
from IPython.core.magic import (
    Magics,
    magics_class,
    line_magic,
)
from IPython.display import display
import ipywidgets as ipw
from .widgets.chaining.utils import (amend_nth_record, labcommand,
                                     make_button, is_recording, get_header)
from progressivis.core import aio
import asyncio
from typing import Any


@magics_class
class IpyProgressivisMagic(Magics):

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
                self.bar.value = i + 1
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
                if wg == "root":
                    labcommand("progressivis:set_root_backup", backup=extra)
                    continue
                wg, nb = self._parse_header(wg)
                rec_index = arch_dict[(wg, nb)]
                amend_nth_record(rec_index, {"markdown": extra})
            self.lab.value = "Don't forget to save your work!"
        loop = asyncio.get_event_loop()
        loop.create_task(_func())

    def _unlock_markdown(self, btn: ipw.Button) -> None:
        labcommand("progressivis:unlock_markdown_cells")
        btn.disabled = True

    @line_magic  # type: ignore
    def pv_markdown(self, line: str) -> Any:
        if not is_recording():
            print("You must allow overwriting record to edit markdown cells")
            return
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
