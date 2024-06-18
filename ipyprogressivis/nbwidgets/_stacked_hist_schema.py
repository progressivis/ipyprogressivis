stacked_hist_spec_no_data = {
  "data": {"name": "data"},
  "width": 150,
  "height": 40,
  "mark": "bar",
  "config": {
    "axis": {
      "titleFontSize": 0
      },

  },
  "transform": [{
    "calculate": "if(datum.Origin === 'raw', 0, 1)",
    "as": "OriginOrder"
  }],
  "encoding": {
    "x": {
      "field": "nbins",
      "axis": None,
      "type": "ordinal",
    },
    "y": {
        "type": "quantitative",
        "field": "level"

    },
    "order": {
      "field": "OriginOrder",
      "type": "quantitative",
    },
    "color": {
      "field": "Origin",
      "type": "nominal",
      "scale": {
        "domain": ["raw", "qry"],
        "range": ["#aaa", "steelblue"]
      },
      "legend": None
    }
  }
}

"""
orig_stacked_hist_spec_no_data = {
  "data": {"name": "data"},
  "width": 150,
  "height": 40,
  "mark": "bar",
  "config": {
    "axis": {
      "titleFontSize": 0
      },

  },
  "transform": [{
    "calculate": "if(datum.Origin === 'Japan', 0, if(datum.Origin === 'USA', 1, 2))",
    "as": "OriginOrder"
  }],
  "encoding": {
    "x": {
      "bin": {"maxbins":10},
      "field": "Miles_per_Gallon",
      "axis": {

      },
      "type": "quantitative",
    },
    "y": {
      "aggregate": "count",
      "type": "quantitative"
    },
    "order": {
      "field": "OriginOrder",
      "type": "quantitative",
    },
    "color": {
      "field": "Origin",
      "type": "nominal",
      "scale": {
        "domain": ["USA","Japan"],
        "range": ["#aaa", "steelblue"]
      },
      "legend": None
    }
  }
}
"""
