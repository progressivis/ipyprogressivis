# progressivis-snippet
from progressivis import SimpleCSVLoader, Sink

@register_snippet
def csv_as_array(input_module, input_slot, columns):
    s = scheduler = input_module.scheduler
    with scheduler:
        csv = SimpleCSVLoader(
            "https://aviz.fr/progressivis/mnist_784.csv.bz2",
            nrows=10_000,
            as_array=lambda cols: {"array": [c for c in cols if c != "class"]},
            scheduler=s,
        )
        sink = Sink(name="sink", scheduler=s)
        sink.input.inp = csv.output.result
    return SnippetResult(output_module=csv, output_slot="result")
