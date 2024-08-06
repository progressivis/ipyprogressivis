from .utils import VBoxTyped, TypedBase, stage_register
import ipywidgets as ipw


class CodeCellW(VBoxTyped):
    class Typed(TypedBase):
        dongle: ipw.Label

    def initialize(self) -> None:
        self.c_.dongle = ipw.Label("Chaining ...")


stage_register["Python"] = CodeCellW
