## root

### Progressive Loading and Visualization

This **ProgressiBook** demonstrates the use of chaining widgets to implement the scenario introduced in [userguide1.1.ipynb](https://github.com/progressivis/progressivis/blob/master/notebooks/userguide1.1.ipynb). It progressively downloads New York Yellow Taxi trip data and visualizes the pickup locations.

## Taxis

This first widget handles the configuration and progressive loading of CSV files. It utilizes the sniffer available in the [CSV loader](https://progressivis.readthedocs.io/en/latest/notebooks.html#csv-loader) for more details.

## Quantiles

This widget creates a `Quantiles` module and connects it to the output of the previously defined CSV loader.

The `Quantiles` module maintains an internal data structure, known as a data sketch, to quickly (though approximately) compute quantiles for all loaded numerical columns. This approach is necessary because the dataset's minimum and maximum values are noisy.

At this stage, the Quantiles module is created and partially configured: both the pickup_latitude and pickup_longitude columns are selected (although the incomplete snapshot below only displays pickup_longitude).

## Heatmap

This widget finalizes the configuration of the `min` and `max` values for the previously created `Quantiles`.

It also creates the `Histogram2D` module to count all pickup locations on a `512x512` grid.

Finally, a `Heatmap` module is created and connected to the output of the Histogram2D module. This module converts the 2D histogram into an image, displayed below.
