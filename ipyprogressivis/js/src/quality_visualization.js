'use strict';
import * as widgets from '@jupyter-widgets/base';
import { new_id } from './base';
import { elementReady } from './es6-element-ready';

import * as d3 from 'd3';
import "../css/quality_visualization.css";


function serializeImgURL(imgURL, mgr) {
  if (mgr.idEnd === undefined) {
    return imgURL;
  }
  let id = mgr.idEnd;
  let svgStr = document.getElementById(id).getHTML();
  if (svgStr === undefined) {
    return imgURL;
  }
  return encodeURIComponent(svgStr);
}

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
        _img_url: "",
      };
    }

  static serializers = {
    ...widgets.DOMWidgetModel.serializers,
    _img_url: { serialize: serializeImgURL },
  };
}

// Custom View. Renders the widget model.
export class QualityVisualizationView extends widgets.DOMWidgetView {
  // Defines how the widget gets rendered into the DOM
  render () {
    this.id = 'quality-vis' + new_id();
    this.el.id = this.id;
    // If we only show a ghost from a saved notebook, insert the svg
    const imgURL = this.model.get("_img_url");
    if (imgURL !== "" && imgURL !== "null") {
      let svgShot = decodeURIComponent(imgURL);
      this.model.set("_img_url", "null");
      this.el.innerHTML = svgShot;
      return;
    }
    this.pbar = quality_pbar(this.el, this.model.get('width'), this.model.get('height'));
    this.model.idEnd = this.id;
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
      topMargin = 5,
      bottomMargin = 15,
      width = w,
      height = h,
      svg = d3.create("svg")
        .classed("quality-vis", true)
        .attr("width", width)
        .attr("height", height),
      g = svg.append("g"),
      gx = svg.append("g")
        .attr("transform", `translate(0, ${height-bottomMargin})`),
      all_measures = {},
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

  // function round3(x) {
  //   return Math.round(x * 1000) / 1000;
  // }

  // function decimate() {
  //   for (const measure in all_measures) {
  //     if (all_measures[measure].length < decimate_threshold) continue;
  //     const half = Math.floor(all_measures[measure].length / 2);
  //     for (let i = half; i > 0; i -= 2) { // always keep index 0
  //       all_measures[measure].splice(i, 1);
  //     }
  //     all_polylines[measure] = "";
  //     for (const [ts, val] of all_measures[measure]) {
  //       all_polylines[measure] += ` ${round3(ts)}, ${round3(val)}`;
  //     }
  //   }
  // }

  function add(ts, measures) {
    const old_max = max_ts,
          old_min = min_ts;
    max_ts = Math.max(max_ts, ts);
    min_ts = Math.min(min_ts, ts);
    const x = d3.scalePow([Math.ceil(min_ts), Math.floor(max_ts)],
                          [topMargin, width-2*topMargin]).exponent(2);
    if (old_max != max_ts || old_min != min_ts) {
      gx.call(d3.axisBottom(x));
      gx.attr("font-size", "8")
        .classed("qaxis");
    }
    //const xwidth = max_ts - min_ts,
    //      xscale = xwidth == 0 ? 1 : (width - 2*margin) / xwidth;
    //g.attr("transform",
    //       `translate(${margin}, ${-margin}) scale(${round3(xscale)}, -1) translate(${round3(-min_ts)}, ${-height})`);

    for (const [measure, val] of Object.entries(measures)) {
      var scale;
      if (all_measures[measure] === undefined) {
        const scheme = d3.schemeCategory10;
        all_measures[measure] = [];
        scale = {xmin: ts, xmax: ts, ymin: val, ymax: val};
        all_scales[measure] = scale;
        all_elements[measure] = g.append("polyline")
          .attr("class", "qline")
          .attr("stroke", scheme[n]);
        all_elements[measure].append("title").text(measure);
        svg.append("text")
          .attr("x", 2)
          .attr("y", 10*n+10)
          .classed("qlabel", true)
          .attr("fill", scheme[n])
          .text(measure);

        n++;
      }
      else {
        scale = all_scales[measure];
        scale.xmin = Math.min(scale.xmin, ts);
        scale.xmax = Math.max(scale.xmax, ts);
        scale.ymin = Math.min(scale.ymin, val);
        scale.ymax = Math.max(scale.ymax, val);
      }
      // const ymin = scale.ymin,
      //       yheight = scale.ymax - ymin,
      //       yscale = yheight === 0 ? 1 : (height - 2*margin) / yheight;
      const y = d3.scaleLinear([scale.ymin, scale.ymax],
                               [height-topMargin-bottomMargin, topMargin]);
      // decimate();
      all_measures[measure].push([ts, val]);
      let polyline = "",
          prev_x;
      for (const [time, value] of all_measures[measure]) {
        const cur_x = x(time),
              cur_y = y(value);
        if (prev_x === undefined || (cur_x - prev_x) > 2) {
          polyline += ` ${cur_x}, ${cur_y}`;
          prev_x = cur_x;
          // Maybe discard the value in all_measures[measure] in the future
        }
      }
      all_elements[measure].attr("points", polyline);
        // .attr(
        //   "transform",
        //   `scale(1, ${round3(yscale)}) translate(0, ${round3(-ymin)})`
        // )
        
    }
  }
  quality_pbar.add = add;

  return quality_pbar;
}
