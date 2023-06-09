# type: ignore

from __future__ import print_function
from setuptools import setup, Command
from setuptools.command.sdist import sdist
from setuptools.command.build_py import build_py
from setuptools.command.egg_info import egg_info
from subprocess import check_call
import os
import sys
import platform
from distutils import log


here = os.path.dirname(os.path.abspath(__file__))
node_root = os.path.join(here, "ipyprogressivis", "js")
is_repo = os.path.exists(os.path.join(here, ".git"))

npm_path = os.pathsep.join(
    [
        os.path.join(node_root, "node_modules", ".bin"),
        os.environ.get("PATH", os.defpath),
    ]
)


log.set_verbosity(log.DEBUG)
log.info("setup.py entered")
log.info("$PATH=%s" % os.environ["PATH"])

LONG_DESCRIPTION = "A Custom Jupyter Widget Library for Progressivis"
PACKAGES = ["ipyprogressivis", "ipyprogressivis.nbwidgets"]


def js_prerelease(command, strict=False):
    """decorator for building minified js/css prior to another command"""

    class DecoratedCommand(command):
        def run(self):
            jsdeps = self.distribution.get_command_obj("jsdeps")
            if not is_repo and all(os.path.exists(t) for t in jsdeps.targets):
                # sdist, nothing to do
                command.run(self)
                return

            try:
                self.distribution.run_command("jsdeps")
            except Exception as e:
                missing = [t for t in jsdeps.targets if not os.path.exists(t)]
                if strict or missing:
                    log.warn("rebuilding js and css failed")
                    if missing:
                        log.error("missing files: %s" % missing)
                    raise e
                else:
                    log.warn("rebuilding js and css failed (not a problem)")
                    log.warn(str(e))
            command.run(self)
            update_package_data(self.distribution)

    return DecoratedCommand


def update_package_data(distribution):
    """update package_data to catch changes during setup"""
    build_py = distribution.get_command_obj("build_py")
    # distribution.package_data = find_package_data()
    # re-init build_py options which load package_data
    build_py.finalize_options()


class NPM(Command):
    description = "install package.json dependencies using npm"

    user_options = []

    node_modules = os.path.join(node_root, "node_modules")

    targets = [
        os.path.join(
            here, "ipyprogressivis", "nbwidgets", "static", "extension.js"
        ),
        os.path.join(
            here, "ipyprogressivis", "nbwidgets", "static", "index.js"
        ),
    ]

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def get_npm_name(self):
        npmName = "yarn"
        if platform.system() == "Windows":
            npmName = "yarn.cmd"

        return npmName

    def has_npm(self):
        npmName = self.get_npm_name()
        try:
            check_call([npmName, "--version"])
            return True
        except Exception:
            return False

    def run(self):
        has_npm = self.has_npm()
        if not has_npm:
            log.error(
                "`npm` unavailable.  If you're running this command using sudo,"
                " make sure `npm` is available to sudo"
            )

        env = os.environ.copy()
        env["PATH"] = npm_path

        if self.has_npm():
            log.info(
                "Installing build dependencies with npm.  This may take a while..."
            )
            npmName = self.get_npm_name()
            check_call(
                [npmName, "install"],
                cwd=node_root,
                stdout=sys.stdout,
                stderr=sys.stderr,
            )
            os.utime(self.node_modules, None)
        check_call(
            [npmName, "run", "build"],
            cwd=node_root,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        for t in self.targets:
            if not os.path.exists(t):
                msg = "Missing file: %s" % t
                if not has_npm:
                    msg += "\nnpm is required to build a development version of a widget extension"
                raise ValueError(msg)

        # update package data in case this created new files
        update_package_data(self.distribution)


version_ns = {}
with open(
    os.path.join(here, "ipyprogressivis", "nbwidgets", "_version.py")
) as f:
    exec(f.read(), {}, version_ns)

setup_args = {
    "name": "ipyprogressivis",
    "version": version_ns["__version__"],
    "description": "A Custom Jupyter Widget Library for Progressivis",
    "long_description": LONG_DESCRIPTION,
    "include_package_data": True,
    "data_files": [
        (
            "share/jupyter/nbextensions/jupyter-progressivis",
            [
                "ipyprogressivis/nbwidgets/static/extension.js",
                "ipyprogressivis/nbwidgets/static/index.js",
                "ipyprogressivis/nbwidgets/static/index.js.map",
            ],
        ),
        ("etc/jupyter/nbconfig/notebook.d", ["jupyter-progressivis.json"]),
    ],
    "install_requires": ["progressivis",
                         "ipywidgets>=7.0.0",
                         "vega>=4.0.0",
                         "ipydatawidgets>=4.0.1"],
    "packages": PACKAGES,
    "zip_safe": False,
    "cmdclass": {
        "build_py": js_prerelease(build_py),
        "egg_info": js_prerelease(egg_info),
        "sdist": js_prerelease(sdist, strict=True),
        "jsdeps": NPM,
    },
    "author": "Jean-Daniel Fekete",
    "author_email": "Jean-Daniel.Fekete@inria.fr",
    "url": "https://github.com/progressivis/ipyprogressivis",
    "keywords": [
        "ipython",
        "jupyter",
        "widgets",
    ],
    "classifiers": [
        "Development Status :: 4 - Beta",
        "Framework :: IPython",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Multimedia :: Graphics",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
}

setup(**setup_args)
