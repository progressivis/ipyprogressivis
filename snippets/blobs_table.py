# progressivis-snippet
import warnings
from progressivis.stats.blobs_table import BlobsPTable
from progressivis.core.api import Sink
warnings.filterwarnings('ignore')

@register_snippet
def blobs_table(input_module, input_slot, columns):
    n_samples = 100_000_000
    n_components = 2
    rtol = 0.01
    centers = [(0.1, 0.3, 0.5), (0.7, 0.5, 3.3), (-0.4, -0.3, -11.1)]
    scheduler = input_module.scheduler
    with scheduler:
        data = BlobsPTable(columns=['_0', '_1', '_2'],  centers=centers,
                           cluster_std=0.2, rows=n_samples, scheduler=scheduler)
        sink = Sink(scheduler=scheduler)
        sink.input.inp = data.output.result
    return SnippetResult(output_module=data, output_slot="result")
