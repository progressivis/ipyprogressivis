# type: ignore
from ._version import __version__
import asyncio

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


def pre_save(model, contents_manager,  **kwargs):
    from .pre_save_md import pre_save_md_impl
    from .pre_save import pre_save_impl
    log = contents_manager.log
    log.info("Starting pre_save ...")
    pre_save_md_impl(model, contents_manager, **kwargs)
    pre_save_impl(model, contents_manager, **kwargs)
    log.info("... end pre_save")


def pre_save_x(model, contents_manager,  **kwargs):
    import nest_asyncio
    nest_asyncio.apply()
    from .pre_save_md import pre_save_md_impl
    from .pre_save_x import pre_save_impl
    log = contents_manager.log
    log.info("Starting pre_save_x ...")
    pre_save_md_impl(model, contents_manager, **kwargs)
    loop = asyncio.get_event_loop()
    task = loop.create_task(pre_save_impl(model, contents_manager, **kwargs))
    loop.run_until_complete(task)
    log.info("... end pre_save_x")

def pre_save_md(model, contents_manager,  **kwargs):
    from .pre_save_md import pre_save_md_impl
    log = contents_manager.log
    log.info("Starting pre_save_md ...")
    pre_save_md_impl(model, contents_manager, **kwargs)
    log.info("... end pre_save_md")

__all__ = ["__version__"]
