from .hook_tools import add_snapshot_tag

from typing import Any

def pre_save_impl(model: dict[str, Any], contents_manager: Any, **kwargs: Any) -> None:
    """copy ProgressiVis snapshots to cells outputs before saving notebooks"""
    # only run on notebooks
    log = contents_manager.log
    if model['type'] != 'notebook':
        return
    # only run on nbformat v4
    if model['content']['nbformat'] != 4:
        return
    metadata = model["content"]["metadata"]
    if not (outs := metadata.get("progressivis_outs")):
        return
    cell_1 = model['content']['cells'][1]  # if progressivis_outs exists then 1 index exists too
    if cell_1['cell_type'] != 'code':
        log.info("Unconsistent ProgressiBook, exit")
        return
    for out in cell_1.get('outputs', []):
        if out.get("data", {}).get("text/plain") in ("Talker()", "BackupWidget()"):
            out["data"]["text/plain"] = ""
    for cell in model['content']['cells']:
        if cell['cell_type'] != 'code':
            continue
        meta_cell = cell["metadata"]
        if not (pv_tag := meta_cell.get("progressivis_tag")):
            continue
        if pv_tag not in outs:
            continue
        prefix, b64_data = outs[pv_tag].split(",", 1)
        b64_data = add_snapshot_tag(b64_data)
        outs[pv_tag] = prefix + "," + b64_data
        for out in cell['outputs']:
            if out["output_type"] not in ("execute_result", "display_data"):
                continue
            if not out.get("data", {}).get("application/vnd.jupyter.widget-view+json"):
                continue
            out["data"] = {
                "image/png": b64_data
            }
    metadata["progressivis_prev_outs"] = metadata["progressivis_outs"]
    # metadata["progressivis_outs"] = []
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
