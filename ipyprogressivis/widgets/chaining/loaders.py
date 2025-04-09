import ipywidgets as ipw
import json
from ..json_editor import JsonEditor
from .utils import (
    TypedBase,
    IpyVBoxTyped,
    IpyHBoxTyped,
)
from typing import Any


class JsonEditorW(IpyVBoxTyped):
    class Typed(TypedBase):
        files: ipw.Select
        edit: ipw.Checkbox
        editor: JsonEditor

    def __init__(self, parent: Any) -> None:
        super().__init__()
        self._parent = parent  # TODO: use a weakref

    def initialize(self) -> None:
        file_ = "/".join([self._parent.widget_dir,
                          self._parent.c_.bookmarks.value])
        with open(file_) as f:
            content = json.load(f)
        self.c_.editor = JsonEditor()
        self.c_.editor.data = content


class BtnBar(IpyHBoxTyped):
    class Typed(TypedBase):
        start: ipw.Button
        save: ipw.Button | ipw.Label
        sniff_btn: ipw.Button | ipw.Label
        text: ipw.Text | ipw.Label
