# type: ignore
from jupyter_server.base.handlers import JupyterHandler
from jupyter_server.serverapp import ServerApp
import tornado
import json
from os.path import join as pjoin


class ProgressivisHandler(JupyterHandler):
    @tornado.web.authenticated
    def get(self) -> None:
        import ipyprogressivis
        nb_name = pjoin(ipyprogressivis.__path__[0],
                        "notebook_templates",
                        "ProgressiBook.ipynb")
        with open(nb_name) as fd:
            payload = fd.read()
            self.finish(json.dumps(dict(payload=payload)))


def _load_jupyter_server_extension(serverapp: ServerApp) -> None:
    """
    This function is called when the extension is loaded.
    """
    handlers = [("/progressivis/template", ProgressivisHandler)]
    serverapp.web_app.add_handlers(".*$", handlers)  # type: ignore
