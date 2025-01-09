# type: ignore
from ._version import __version__

_ = __version__


def _jupyter_labextension_paths():
    """Called by Jupyter Lab Server to detect if it is a valid labextension and
    to install the widget

    Returns
    =======
    src: Source directory name to copy files from. Webpack outputs generated files
        into this directory and Jupyter Lab copies from this directory during
        widget installation
    dest: Destination directory name to install widget files to. Jupyter Lab copies
        from `src` directory into <jupyter path>/labextensions/<dest> directory
        during widget installation
    """
    return [{
        'src': 'labextension',
        'dest': 'jupyter-progressivis',
    }]


def _jupyter_nbextension_paths():
    """Called by Jupyter Notebook Server to detect if it is a valid nbextension and
    to install the widget

    Returns
    =======
    section: The section of the Jupyter Notebook Server to change.
        Must be 'notebook' for widget extensions
    src: Source directory name to copy files from. Webpack outputs generated files
        into this directory and Jupyter Notebook copies from this directory during
        widget installation
    dest: Destination directory name to install widget files to. Jupyter Notebook copies
        from `src` directory into <jupyter path>/nbextensions/<dest> directory
        during widget installation
    require: Path to importable AMD Javascript module inside the
        <jupyter path>/nbextensions/<dest> directory
    """
    return [{
        'section': 'notebook',
        'src': 'nbextension',
        'dest': 'jupyter-progressivis',
        'require': 'jupyter-progressivis/extension'
    }]


def _jupyter_server_extension_points():
    """
    Returns a list of dictionaries with metadata describing
    where to find the `_load_jupyter_server_extension` function.
    """
    return [{"module": "ipyprogressivis.app"}]


def pre_save(model, **kwargs):
    """copy ProgressiVis snapshots to cells outputs before saving notebooks"""
    # only run on notebooks
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
        print("Unconsistent ProgressiBook, exit")
        return
    for out in cell_1.get('outputs', []):
        if out.get("data", {}).get("text/plain") in ("Talker()", "BackupWidget()"):
            out["data"]["text/plain"] = ""
    for i, cell in enumerate(model['content']['cells']):
        if cell['cell_type'] != 'code':
            continue
        if i >= len(outs) or not outs[i]:  # an empty dict actually
            continue
        _, b64_data = outs[i].split(",", 1)
        for j, out in enumerate(cell['outputs']):
            if out["output_type"] not in ("execute_result", "display_data"):
                continue
            out["data"] = {
                "image/png": b64_data
            }
    metadata["progressivis_outs"] = []
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


__all__ = ["__version__"]
