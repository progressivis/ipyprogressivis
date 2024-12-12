from .utils import stage_register, VBox, make_replay_next_btn, is_step
from ..slot_wg import SlotWg
from typing import List, cast
from progressivis.core.api import Scheduler, Module


class DumpPTableW(VBox):
    def __init__(self) -> None:
        super().__init__()
        self.frozen_kw = dict(fake="fake")

    def initialize(self) -> None:
        self.dag_running()
        input_ = (
            self.input_module
            if isinstance(self.input_module, Module)
            else self.input_module.module
        )
        sl_wg = SlotWg(input_, self.input_slot)
        if is_step():
            next_btn = make_replay_next_btn()
            self.children = (sl_wg, next_btn)
        else:
            self.children = (sl_wg,)
        input_.scheduler().on_tick(self._refresh_proc)

    async def _refresh_proc(self, scheduler: Scheduler, run_number: int) -> None:
        await cast(SlotWg, self.children[0]).refresh()

    def get_underlying_modules(self) -> List[str]:
        return []


stage_register["Dump_table"] = DumpPTableW
