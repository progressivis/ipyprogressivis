from .utils import VBoxTyped, TypedBase
import ipywidgets as ipw


class CustomLoaderW(VBoxTyped):
    class Typed(TypedBase):
        dongle: ipw.Label

    def initialize(self) -> None:
        self.c_.dongle = ipw.Label("Custom loader")
