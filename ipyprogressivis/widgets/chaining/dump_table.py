from .utils import (
    stage_register,
    VBox,
    replay_next
)
from ..slot_wg import SlotWg
from typing import List, cast
from progressivis.core.api import Scheduler, Module


class DumpPTableW(VBox):
    def __init__(self) -> None:
        super().__init__()

    def initialize(self) -> None:
        self.dag_running()
        sl_wg = SlotWg(cast(Module, self.input_module), self.input_slot)
        self.children = (sl_wg,)
        self.input_module.scheduler().on_tick(self._refresh_proc)
        replay_next()

    async def _refresh_proc(self, scheduler: Scheduler, run_number: int) -> None:
        await cast(SlotWg, self.children[0]).refresh()

    def get_underlying_modules(self) -> List[str]:
        return []


stage_register["Dump_table"] = DumpPTableW
