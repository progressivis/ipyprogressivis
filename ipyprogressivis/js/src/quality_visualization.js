'use strict';
import * as widgets from '@jupyter-widgets/base';
import { new_id } from './base';
import { elementReady } from './es6-element-ready';

import * as d3 from 'd3';
import "../css/quality_visualization.css";

export class QualityVisualizationModel extends widgets.DOMWidgetModel {
    defaults() {
      return {
        ...super.defaults(),
        _model_name: 'QualityVisualizationModel',
        _view_name:  'QualityVisualizationView',
        _model_module: 'jupyter-progressivis',
        _view_module:  'jupyter-progressivis',
        _model_module_version: '0.1.0',
        _view_module_version: '0.1.0',
        width: 300,
        height: 50,
      };
    }
}

// Custom View. Renders the widget model.
export class QualityVisualizationView extends widgets.DOMWidgetView {
  // Defines how the widget gets rendered into the DOM
  render () {
    this.id = 'quality-vis' + new_id();
    this.el.innerHTML = '<div class = "quality-vis" style="width: 100%;"></div>';
    this.pbar = quality_pbar(this.el, this.model.get('width'), this.model.get('height'));
    // this.data_changed();
    // Observe changes in the value traitlet in Python, and define
    // a custom callback.
    this.model.on('change:width', this.size_changed, this);
    this.model.on('change:height', this.size_changed, this);
    this.model.on("msg:custom", (ev) => {
      if (ev.type != "update") {
        return;
      }
      this.pbar.add(ev.timestamp, ev.measures);
    });
  }

  size_changed () {
    let that = this;
    elementReady('#' + that.id).then(
      () => this.pbar.width(this.model.get('width')),
      this.pbar.height(this.model.get('height')))
  }
}

function quality_pbar(parent, w, h) {
  let n = 0,
      width = w,
      height = h,
      svg = d3.create("svg").attr("width", width).attr("height", height),
      g = svg.append("g"),
      all_measures = {},
      all_polylines = {},
      all_scales = {},
      min_ts = Number.POSITIVE_INFINITY,
      max_ts = Number.NEGATIVE_INFINITY,
      all_elements = {},
      decimate_threshold = width;

  parent.append(svg.node());

   quality_pbar.max_ts = function(_) {
     return arguments.length ? max_ts = +_ : max_ts;
   };

  quality_pbar.width = function(_) {
    return arguments.length ? width = _ & svg.attr("width", width) : width;
  };
  quality_pbar.height = function(_) {
    return arguments.length ? height = _ & svg.attr("height", height) : height;
  };
  quality_pbar.decimate_threshold = function(_) {
    return arguments.length ? decimate_threshold = +_ : decimate_threshold;
  }

  function round3(x) {
    return Math.round(x * 1000) / 1000;
  }

  function decimate() {
    for (const measure in all_measures) {
      if (all_measures[measure].length < decimate_threshold) continue;
      const half = Math.floor(all_measures[measure].length / 2);
      for (let i = half; i > 0; i -= 2) { // always keep index 0
        all_measures[measure].splice(i, 1);
      }
      all_polylines[measure] = "";
      for (const [ts, val] of all_measures[measure]) {
        all_polylines[measure] += ` ${ts}, ${round3(val)}`;
      }
    }
  }

  function add(ts, measures) {
    max_ts = Math.max(max_ts, ts);
    min_ts = Math.min(min_ts, ts);
    const xwidth = max_ts - min_ts,
          xscale = xwidth == 0 ? 1 : width / xwidth;
    g.attr("transform",
           `scale(${round3(xscale)}, -1) translate(${round3(-min_ts)}, ${-height})`);

    for (const [measure, val] of Object.entries(measures)) {
      var scale;
      if (all_measures[measure] === undefined) {
        const scheme = d3.schemeCategory10;
        all_measures[measure] = [];
        all_polylines[measure] = "";
        scale = {xmin: ts, xmax: ts, ymin: val, ymax: val};
        all_scales[measure] = scale;
        all_elements[measure] = g.append("polyline")
          .attr("class", "qline")
          .attr("stroke", scheme[n]);
        all_elements[measure].append("title").text(measure);

        n++;
      }
      else {
        scale = all_scales[measure];
        scale.xmin = Math.min(scale.xmin, ts);
        scale.xmax = Math.max(scale.xmax, ts);
        scale.ymin = Math.min(scale.ymin, val);
        scale.ymax = Math.max(scale.ymax, val);
      }
      const ymin = scale.ymin,
            yheight = scale.ymax - ymin,
            yscale = yheight === 0 ? 1 : height / yheight;
      // decimate();
      all_measures[measure].push([ts, val]);
      all_polylines[measure] += ` ${round3(ts)}, ${round3(val)}`;
      all_elements[measure]
        .attr(
          "transform",
          `scale(1, ${round3(yscale)}) translate(0, ${round3(-ymin)})`
        )
        .attr("points", all_polylines[measure]);
    }
  }
  quality_pbar.add = add;

  return quality_pbar;
}
