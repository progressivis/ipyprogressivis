from .utils import VBox, make_replay_next_btn, is_step, Coro, chaining_widget
from ..slot_wg import SlotWg
from typing import List, cast
from progressivis.core.api import Module
from progressivis.core import aio


class AfterRun(Coro):
    async def action(self, m: Module, run_number: int) -> None:
        assert self.leaf is not None
        await cast(SlotWg, self.leaf.children[0]).refresh()  # type: ignore

@chaining_widget(label="Dump table")
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
        after_run = AfterRun()
        input_.on_after_run(after_run)
        self.make_leaf_bar(after_run)
        if input_.state == input_.state_terminated:  # useful for little tables
            aio.create_task(after_run.action(input_, 42))

    def get_underlying_modules(self) -> List[str]:
        return []
