import sys
import json
import click
from pathlib import Path
import os
from more_itertools import split_before  # type: ignore

def split_md(code: str) -> dict[str, str]:
    """
    """
    line_list = [line.rstrip() + "\n" for line in code.split("\n") if line.strip()]
    title_token = "## "
    return dict(
        [
            (item[0].rstrip("\n"), item)
            for item in split_before(line_list, lambda x: x.startswith(title_token))
        ]
    )


@click.command()
@click.option("-p", "--progressibook", help="Input ProgressiBook")
@click.option("-w", "--overwrite", is_flag=True, help="Overwrite the current book. If false, --output is mandatory")
@click.option("-o", "--output", default="", help="Output book name or directory")
@click.option("-m", "--markdown", default="", help=("Markdown file."
                                                    "If missing try to merge the markdown homonym"))
def main(progressibook: str, overwrite: bool, output: str, markdown: str | Path) -> None:
    pb_dir = os.path.dirname(progressibook)
    pb_name = os.path.basename(progressibook)
    bare_name, extn = pb_name.rsplit(".", 1)
    assert extn == "ipynb"
    if not output:
        output = progressibook
    if os.path.isdir(output):
        out_name  = Path(output) / pb_name
    else:
        out_name  = Path(output)
    if os.path.exists(out_name) and not overwrite:
        print(f"{out_name=} already exists. Use --overwrite to replace it")
        sys.exit(1)
    if not markdown:
        md_name = f"{bare_name}.md"
        markdown = Path(pb_dir) / md_name
    if not os.path.exists(markdown):
        print(f"Markdown file {markdown} does not exist")
        sys.exit(1)
    with open(markdown) as f:
        md_content = f.read()
    md_dict = split_md(md_content)
    with open(progressibook) as f:
        pb = json.load(f)
    cells = pb.get("cells", [])
    if len(cells) < 4 or not cells[0].get("metadata", {}).get("progressivis_backup"):
        print("probably not a ProgressiBook")
        sys.exit(1)
    for cell in cells:
        if cell.get("cell_type") != "markdown" or not (src := cell.get("source")):
            continue
        if (title := src[0].strip("\n")) in md_dict:
            cell["source"] = md_dict[title]
    with open(out_name, "w") as f:
        json.dump(pb, f)

if __name__ == "__main__":
    main()

