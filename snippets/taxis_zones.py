# progressivis-snippet
import tempfile
from pathlib import Path
from progressivis.datasets.wget import wget_file
from progressivis import Sink, ArrowBatchLoader
import duckdb
import pyarrow.parquet as pq
td = tempfile.TemporaryDirectory()
taxis_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2015-05.parquet"
taxis_file = Path(td.name) / "yellow_tripdata_2015-05.parquet"
zones_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
zones_file = Path(td.name) / "taxi_zone_lookup.csv"
SQL = (
    f"SELECT tx.tpep_pickup_datetime, tx.tpep_dropoff_datetime, tx.passenger_count, zn.Borough"
    f" FROM read_parquet('{taxis_file}') tx, read_csv('{zones_file}') zn"
    " WHERE tx.PULocationID=zn.LocationID"
)
try:
    wget_file(url=taxis_url, filename=taxis_file)
    wget_file(url=zones_url, filename=zones_file)
    con = duckdb.connect(database=":memory:")
    n_rows = pq.ParquetFile(taxis_file).metadata.num_rows
    con.execute(SQL)
    reader = con.fetch_record_batch(1000)
finally:
    td.cleanup()

@register_snippet
def taxis_zones(input_module, input_slot, columns):
    scheduler = input_module.scheduler
    with scheduler:
        data =  ArrowBatchLoader(reader=reader, n_rows=n_rows, scheduler=scheduler)
        sink = Sink(scheduler=scheduler)
        sink.input.inp = data.output.result
    return SnippetResult(output_module=data, output_slot="result")
