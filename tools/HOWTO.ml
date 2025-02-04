# Documentation tools HOWTO

## Merging ProgressiBooks and markdown files (`merge_nb_markdown.py` script)

The `merge_nb_markdown.py` script merges a `ProgressiBook` with a markdown file containing exactly the same titles.

In other words, you can edit and manage the documentation of a `ProgressiBook` in a separate file and reinject it into the same `ProgressiBook` (or a new one) when you need to redo it from scratch.

Indeed, you can merge the `ProgressiBook` and the underlying markdown file into a new `ProgressiBook` this way:

```sh
$ python tools/merge_nb_markdown.py -p notebooks/userguide-widgets1.1.ipynb -m notebooks/userguide-widgets1.1.md -o /tmp/foo.ipynb
notebooks/userguide-widgets1.1.ipynb /tmp/foo.ipynb
```

You can also write the result to the current ProgressiBook using the overwrite option:

```sh
$ python tools/merge_nb_markdown.py -w -p notebooks/userguide-widgets1.1.ipynb -m notebooks/userguide-widgets1.1.md
```

A help is also available:

```sh
$ python tools/merge_nb_markdown.py --help
Usage: merge_nb_markdown.py [OPTIONS]

Options:
  -p, --progressibook TEXT  Input ProgressiBook
  -w, --overwrite           Overwrite the current book. If false, --output is
                            mandatory
  -o, --output TEXT         Output book name or directory
  -m, --markdown TEXT       Markdown file.If missing try to merge the markdown
                            homonym
  --help                    Show this message and exit.

```

## Generating HTML files from ProgressiBooks (`nb_to_doc_html.py` script)

ProgressiBooks cannot be converted to clean html via `jupyter nbconvert`command due to some technical peculiarities (mainly hidden cells and use of the Sidecar widget). The following script overcomes these problems and produces self-sufficient html files that can be easily integrated into Sphinx documentation. As the output is a directory, the file generated will have the same name as the `ProgressiBook`, but with the extension changed to 'html':

Translated with DeepL.com (free version)

```sh
$ python tools/nb_to_doc_html.py -p notebooks/userguide-widgets1.1.ipynb -o notebooks/
```

A help is also available:

```sh
$ python tools/nb_to_doc_html.py --help
Usage: nb_to_doc_html.py [OPTIONS]

Options:
  -p, --progressibook TEXT  Input ProgressiBook
  -w, --overwrite           Overwrite HTML file if exists
  -o, --output TEXT         Destination HTML. If it is a directory the
                            ProgressiBook name is used with extension '.html'
  --help                    Show this message and exit.
```
