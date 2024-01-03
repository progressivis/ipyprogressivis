# IpyProgressiVis: A Custom Jupyter Widget Library for ProgressiVis

[![Python Tests](https://github.com/progressivis/ipyprogressivis/actions/workflows/ui.yml/badge.svg?branch=main&event=push)](https://github.com/progressivis/ipyprogressivis/actions/workflows/ui.yml)
[![Documentation Status](https://readthedocs.org/projects/progressivis/badge/?version=latest)](https://progressivis.readthedocs.io/en/latest/?badge=latest)
[![linting - Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![types - Mypy](https://img.shields.io/badge/types-Mypy-blue.svg)](https://github.com/python/mypy)
[![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch)
[![License](https://img.shields.io/badge/License-BSD_2--Clause-orange.svg)](https://opensource.org/licenses/BSD-2-Clause)


## Installation

See the installation instructions [provided here](https://progressivis.readthedocs.io/en/latest/install.html).

## Running demos (on your computer)

After installing `ipyprogressivis` do:

```
cd ipyprogressivis/notebooks
jupyter lab
```
then run the notebooks of your choice

<!--
### Running under Voilà

Install voilà :

$ conda install -c conda-forge voila

#### As a standalone app :

voila --enable_nbextensions=True YourNotebook.ipynb

#### As a server extension to notebook or jupyter_server

First, you have to enable the extension:

$ jupyter serverextension enable voila --sys-prefix


Then run:

$jupyter lab --VoilaConfiguration.enable_nbextensions=True

or

$jupyter notebook --VoilaConfiguration.enable_nbextensions=True

When running the Jupyter server, the Voilà app is accessible from the base url suffixed with voila

#### Using a JupyterLab extension to render a notebook with voila

Install the extension :

jupyter labextension install @jupyter-voila/jupyterlab-preview

Display the notebook with Voilà like showed here: https://user-images.githubusercontent.com/591645/59288034-1f8d6a80-8c73-11e9-860f-c3449dd3dcb5.gif
-->