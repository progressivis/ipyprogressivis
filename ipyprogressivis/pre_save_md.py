from ipyprogressivis.widgets.chaining.utils import dump_backup, bpack
from .hook_tools import parse_tag
from typing import Any

def pre_save_md_impl(model: dict[str, Any], contents_manager: Any, **kwargs: Any) -> None:
    """copy ProgressiVis snapshots to cells outputs before saving notebooks"""
    # only run on notebooks
    log = contents_manager.log
    if model['type'] != 'notebook':
        return
    # only run on nbformat v4
    if model['content']['nbformat'] != 4:
        return
    if not model['content']['cells']:
        return
    cell_0 = model['content']['cells'][0]
    if cell_0['cell_type'] != 'markdown':
        return
    if "progressivis_backup" not in cell_0["metadata"]:
        return
    backup_str = cell_0["metadata"]["progressivis_backup"]
    if not backup_str:
        return
    log.info("this notebook has a backup!")
    backup = dump_backup(backup_str)
    backup_dict = {}
    for elt in backup:
        title = elt.get("title", "")
        alias = elt.get("alias", "")
        number = elt.get("number", 0)
        key = (alias or title, number)
        backup_dict[key] = elt
    for i, cell in enumerate(model['content']['cells']):
        if cell['cell_type'] != 'markdown':
            continue
        if not (pv_tag := cell["metadata"].get("progressivis_tag")):
            continue
        title = f"## {pv_tag}"
        source = cell["source"]
        if isinstance(source, str):
            source = source.split("\n")
        if len(source) <= 1:
            continue
        src_0 = source[0]
        if src_0 != title:
            log.info(f"Title {src_0} was fixed. Now it is {title}")
        extra = "\n".join(source[1:])
        if pv_tag == "root":
            cell_0["metadata"]["progressivis_root_backup"] = extra
            continue
        key_nb = parse_tag(pv_tag)
        if key_nb not in backup_dict:
            log.info(f"{key_nb} not processed")
            continue
        elt = backup_dict[key_nb]
        elt["markdown"] = extra
    cell_0["metadata"]["progressivis_backup"] = bpack(backup)
