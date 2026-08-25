# Packaging `ipyprogressivis`

Before you begin, make sure you have the necessary permissions on **PyPI** and **GitHub**.

Perform the following steps in a development environment for `ipyprogressivis` (e.g., `conda` or equivalent) that also includes the `hatch` package:

1. Create a new release at https://github.com/progressivis/ipyprogressivis/releases (let's say v0.3.7)

2. From the root of the repository, run:

    ```shell
    cd scripts
    ./hatch_build.sh v0.3.7
    ```
    When the process is complete, the script will display a message similar to this one:

    ```shell
    ...
    Version in _version.py is: 0.3.7
    Get your files here /tmp/tmp.<random-str>/ipyprogressivis/dist
    ```
3. Check the content of `/tmp/tmp.<random-str>/ipyprogressivis/dist`. It should contain two files: `progressivis-0.3.7-py3-none-any.whl`  `progressivis-0.3.7.tar.gz`

4. Publish the package on PyPI:

    ```shell
    hatch publish /tmp/tmp.<random-str>/ipyprogressivis/dist
    ```
