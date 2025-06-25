from PIL import Image
import base64
import io
import os
from pathlib import Path

from typing import Any

eye_img = Image.open(Path(os.path.dirname(__file__)) / "resources" / "snapshot_marker.png")
eye_img = eye_img.resize((64, 64))


def add_snapshot_tag(data: Any) -> str:
    img = Image.open(io.BytesIO(base64.b64decode(data)))
    # img = img.filter(ImageFilter.BLUR)
    img.paste(eye_img, (0, 0), eye_img)
    buffered = io.BytesIO()
    img.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode("utf-8")

def add_snapshot_tag_from_bytes(data: Any) -> str:
    img = Image.open(io.BytesIO(data))
    # img = img.filter(ImageFilter.BLUR)
    img.paste(eye_img, (0, 0), eye_img)
    buffered = io.BytesIO()
    img.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode("utf-8")

def parse_tag(tag: str) -> tuple[str, int]:
    if "[" not in tag:
        return tag, 0
    assert tag[-1] == "]"
    pos = tag.index("[")
    return tag[:pos], int(tag[pos+1:-1])


def make_css_marker(key: str, nb: int = 0) -> str:
    key = key.replace(" ", "_").replace(".", "-")
    return f"progressivis_guest_widget--{key}_{nb}"

