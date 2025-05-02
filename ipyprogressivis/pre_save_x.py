import json
from pathlib import Path
from tempfile import TemporaryDirectory
from playwright.async_api import async_playwright
from nbconvert.exporters.html import HTMLExporter
from .hook_tools import add_snapshot_tag, add_snapshot_tag_from_bytes, parse_tag, make_css_marker
from typing import Any

_html_exporter = None

#  See: https://stackoverflow.com/questions/36125589/how-to-wait-for-task-created-by-create-task-to-complete

async def pre_save_impl(model: dict[str, Any], contents_manager: Any, **kwargs: Any) -> None:
    """copy ProgressiVis snapshots to cells outputs before saving notebooks"""
    # only run on notebooks
    log = contents_manager.log
    if model['type'] != 'notebook':
        return
    # only run on nbformat v4
    if model['content']['nbformat'] != 4:
        return
    metadata = model["content"]["metadata"]
    outs = metadata.get("progressivis_outs", [])
    global _html_exporter
    if _html_exporter is None:
        _html_exporter = HTMLExporter(parent=contents_manager)  # type: ignore

    with TemporaryDirectory() as tmpdirname:
        tmpdir = Path(tmpdirname)
        nb_fname = tmpdir / "this.ipynb"
        html_fname = tmpdir / "this.html"
        with open(nb_fname, "w") as nbfw:
            json.dump(model['content'], nbfw)
        html, resources = _html_exporter.from_filename(nb_fname)  # type: ignore
        with open(html_fname, 'w', encoding='utf-8') as f:
            f.write(html)
        async with async_playwright() as p:
            browser_type = p.chromium  # or p.firefox or p.webkit
            browser = await browser_type.launch()
            page = await browser.new_page()
            await page.goto(f"file://{html_fname}")
            cells = model["content"]["cells"]
            if len(cells) < 2:
                log.info("Too short to be a ProgressiBook => no post_save")
                return
            cell_1 = cells[1]
            if cell_1['cell_type'] != 'code':
                log.info("Unconsistent ProgressiBook, exit")
                return
            for out in cell_1.get('outputs', []):
                if out.get("data", {}).get("text/plain") in ("Talker()", "BackupWidget()"):
                    out["data"]["text/plain"] = ""
            for i, cell in enumerate(cells):
                if cell['cell_type'] != 'code':
                    continue
                meta_cell = cell["metadata"]
                if not (pv_tag := meta_cell.get("progressivis_tag")):
                    continue
                key, nb = parse_tag(pv_tag)
                if key in ("Dump_table", "Dump table", "Heatmap", "Any Vega"):
                    prefix, b64_data = outs[i].split(",", 1)
                    b64_data = add_snapshot_tag(b64_data)
                    outs[i] = prefix + "," + b64_data
                else:
                    css_marker =  make_css_marker(key, nb)
                    buff = await page.locator(f".{css_marker}").screenshot()
                    b64_data = add_snapshot_tag_from_bytes(buff)
                for j, out in enumerate(cell['outputs']):
                    if out["output_type"] not in ("execute_result", "display_data"):
                        continue
                    if not out.get("data", {}).get("application/vnd.jupyter.widget-view+json"):
                        continue
                    out["data"] = {
                        "image/png": b64_data
                    }
            await browser.close()
            metadata["progressivis_prev_outs"] = metadata.get("progressivis_outs", [])
            if not (dag_png := metadata.get("progressivis_dag_png")):
                return
            _, b64_data = dag_png.split(",", 1)
            if not (wg_values := metadata.get("widgets", {})
                    .get('application/vnd.jupyter.widget-state+json', {})
                    ).values():
                return
            for _, wg in wg_values.get("state", {}).items():
                if wg.get("model_module") != "@jupyter-widgets/jupyterlab-sidecar":
                    continue
                for out in wg.get("state", {}).get("outputs", []):
                    if out.get("data", {}).get("text/plain") != "DagWidgetController()":
                        continue
                    out["data"] = {
                        "image/png": b64_data,
                        "output_type": "display_data",
                        "metadata": {}
                    }

