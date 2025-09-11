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
  let svgStr = document.getElementById(id).parentElement.getHTML();
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
    this.id = "quality_vis_" + new_id();
    // If we only show a ghost from a saved notebook, insert the svg
    const imgURL = this.model.get("_img_url");
    if (imgURL !== "" && imgURL !== "null") {
      let svgShot = decodeURIComponent(imgURL);
      this.model.set("_img_url", "null");
      this.el.innerHTML = svgShot;
      // Maybe remove the svg id to avoid clashes.
      // d3.select(this.el).select("svg").attr("id", null);
      return;
    }
    this.pbar = quality_pbar(this, this.model.get('width'), this.model.get('height'));
    this.model.idEnd = this.id;
    this.el.append(this.pbar.svg());
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

function _format_seconds(ts, formater) {
  return formater.format({
    days: Math.floor(ts / (60*60*24)),
    hours: Math.floor(ts / (60*60)) % 24,
    minutes: Math.floor(ts / 60) % 60,
    seconds: Math.floor(ts) % 60
  });
}

function quality_pbar(view, w, h) {
  let n = 0,
      topMargin = 5,
      bottomMargin = 20,
      leftMargin = 5,
      rightMargin = 5,
      width = w,
      height = h,
      decimate_threshold = 2, // min distance between two visible points
      min_ts = Number.POSITIVE_INFINITY,
      max_ts = Number.NEGATIVE_INFINITY;
  const id = view.id,
        svg = d3.create("svg")
          .attr("id", id)
          .classed("quality-vis", true)
          .attr("width", width)
          .attr("height", height),
        g = svg.append("g"),
        gx = svg.append("g")
          .classed("qaxis", true)
          .attr("transform", `translate(0, ${height-bottomMargin})`),
      all_measures = {},
      all_scales = {},
      all_elements = {},
      durationFormat = new Intl.DurationFormat("en", {style: "digital"}),
      seconds_formatter = ((ts) => _format_seconds(ts - min_ts, durationFormat));


  function _svg() { return svg.node(); }

  function _max_ts(_) {
     return arguments.length ? max_ts = +_ : max_ts;
  }

  function _width(_) {
    return arguments.length ? width = _ & svg.attr("width", width) : width;
  }
  function _height(_) {
    return arguments.length ? height = _ & svg.attr("height", height) : height;
  }
  
  function _decimate_threshold(_) {
    return arguments.length ? decimate_threshold = +_ : decimate_threshold;
  }

  function add(ts, measures) {
    const old_max = max_ts,
          old_min = min_ts;
    max_ts = Math.max(max_ts, ts);
    min_ts = Math.min(min_ts, ts);
    const x = d3.scalePow([Math.floor(min_ts), Math.ceil(max_ts)],
                          [leftMargin, width-rightMargin])
          .exponent(2);
    if (old_max != max_ts || old_min != min_ts) {
      const axis = d3.axisBottom(x)
            .ticks(4)
            .tickFormat(seconds_formatter)
            .tickSizeInner(4);
      gx.call(axis);
    }

    for (const [measure, val] of Object.entries(measures)) {
      var scale;
      if (all_measures[measure] === undefined) {
        const scheme = d3.schemeCategory10;
        all_measures[measure] = [];
        scale = {ymin: val, ymax: val};
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
        scale.ymin = Math.min(scale.ymin, val);
        scale.ymax = Math.max(scale.ymax, val);
      }
      const y = d3.scaleLinear([scale.ymin, scale.ymax],
                               [height-bottomMargin, topMargin]);
      const measures = all_measures[measure];
      measures.push([ts, val]);
      let polyline = "",
          prev_x;
      for (let i = 0; i < measures.length; ) {
        const [time, value] = measures[i];
        const cur_x = x(time),
              cur_y = y(value);
        if (prev_x === undefined || (cur_x - prev_x) > decimate_threshold) {
          polyline += ` ${cur_x.toPrecision(4)}, ${cur_y.toPrecision(4)}`;
          prev_x = cur_x;
          i++;
        }
        else {
          measures.splice(i, 1); // remove point too close to previous
        }
      }
      all_elements[measure].attr("points", polyline);
    }
  }

  return {
    add: add,
    svg: _svg,
    decimate_threshold: _decimate_threshold,
    height: _height,
    width: _width,
    max_ts: _max_ts,
  };
}
