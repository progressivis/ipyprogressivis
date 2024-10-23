from .utils import (VBoxTyped, TypedBase, stage_register,
                    make_replay_next_btn, is_replay_only)

import ipywidgets as ipw


class CodeCellW(VBoxTyped):
    class Typed(TypedBase):
        dongle: ipw.Label | ipw.Button

    def initialize(self) -> None:
        self.c_.dongle = (make_replay_next_btn() if is_replay_only()
                          else ipw.Label("Chaining ..."))


stage_register["Python"] = CodeCellW
