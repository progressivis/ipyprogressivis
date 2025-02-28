import sys
import json
import subprocess
import shlex
import shutil
import click
from pathlib import Path
from tempfile import TemporaryDirectory
import os


@click.command()
@click.option("-p", "--progressibook", help="Input ProgressiBook")
@click.option("-w", "--overwrite", is_flag=True, help="Overwrite HTML file if exists")
@click.option("-o", "--output", help=("Destination HTML. If it is a directory"
                                                  " the ProgressiBook name is used with extension '.html'"))


def main(progressibook: str, overwrite: bool, output: str) -> None:
    print(progressibook, output)
    pb_name = os.path.basename(progressibook)
    bare_name, extn = pb_name.rsplit(".", 1)
    assert extn == "ipynb"
    html_name = f"{bare_name}.html"
    if os.path.isdir(output):
        out_name  = Path(output) / html_name
    else:
        out_name  = Path(output)
    if os.path.exists(out_name) and not overwrite:
        print(f"{out_name=} already exists. Use --overwrite to replace it")
        sys.exit(1)
    with open(progressibook) as f:
        pb = json.load(f)
    cells = pb.get("cells", [])
    if len(cells) < 4 or not cells[0].get("metadata", {}).get("progressivis_backup"):
        print("probably not a ProgressiBook")
        sys.exit(1)
    cell_1 = cells[1]
    cell_1["source"] = ["..."]
    if dag_png := pb.get("metadata", {}).get("progressivis_dag_png"):
        prefix, b64_data = dag_png.split(",", 1)
        cell_1["outputs"] = [{"data": {"image/png": b64_data},
                              "output_type": "display_data", "metadata": {}}]
    else:
        cell_1["outputs"] = []
    #root_cell = cells[3]
    #root_cell["source"] = ["..."]
    #root_cell["outputs"] = []
    for cell in cells:
        if cell.get("source") and cell["source"][-1].startswith("header.talker.labcommand('notebook:hide-cell-code')"):
            cell["source"] = ["..."]
            cell["outputs"] = []
    with TemporaryDirectory() as tmpdirname:
        tmpdir = Path(tmpdirname)
        nbdoc_path = tmpdir / pb_name
        with open(nbdoc_path, "w") as f:
            json.dump(pb, f)
        cmd = f"jupyter nbconvert --to html --template lab {nbdoc_path}"
        subprocess.run(shlex.split(cmd))
        shutil.copy(tmpdir / html_name, out_name)

if __name__ == "__main__":
    main()

