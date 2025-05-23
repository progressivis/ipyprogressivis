//'use strict';
import * as widgets from "@jupyter-widgets/base";
import _ from "lodash";
import $ from "jquery";
import { new_id } from "./base";
import { elementReady } from "./es6-element-ready";
import { Config, Interpreter } from "multiclass-density-maps";
import * as colormaps from "./colormaps";
import * as d3 from "d3";
import History from "./history";
import { register_config_editor } from "./config-editor";
const ndarray = require("ndarray");
const ndarray_unpack = require("ndarray-unpack");
import "../css/scatterplot.css";

const DEFAULT_SIGMA = 0;
const DEFAULT_FILTER = "default";
const MAX_PREV_IMAGES = 3;

import {
  data_union_serialization,
  listenToUnion,
} from "jupyter-dataserializers";

window.ndarray = ndarray;

export class ScatterplotModel extends widgets.DOMWidgetModel {
  defaults() {
    return {
      ...super.defaults(),
      _model_name: "ScatterplotModel",
      _view_name: "ScatterplotView",
      _model_module: "jupyter-progressivis",
      _view_module: "jupyter-progressivis",
      _model_module_version: "0.1.0",
      _view_module_version: "0.1.0",
      hists: ndarray([]),
      samples: ndarray([]),
      data: "Hello Scatterplot!",
      value: "{0}",
      move_point: "{0}",
      modal: false,
      to_hide: [],
    };
  }
  static serializers = {
    ...widgets.DOMWidgetModel.serializers,
    hists: data_union_serialization,
    samples: data_union_serialization,
  };
}

// Custom View. Renders the widget model.
export class ScatterplotView extends widgets.DOMWidgetView {
  // Defines how the widget gets rendered into the DOM
  render() {
    this.id = "view_" + new_id();
    const scatterplot = Scatterplot(this);
    this.scatterplot = scatterplot;
    this.scatterplot.template(this.el);
    let that = this;
    elementReady("#" + scatterplot.with_id("prevImages")).then(() =>
      scatterplot.ready(that),
    );
    listenToUnion(this.model, "hists", this.update.bind(this), true);
    listenToUnion(this.model, "samples", this.update.bind(this), true);
    // Observe changes in the value traitlet in Python, and define
    // a custom callback.
    this.model.on("change:data", this.data_changed, this);
  }
  data_changed() {
    //console.log("data_changed");
    const val = this.model.get("data");
    this.scatterplot.update_vis(JSON.parse(val));
  }
}

function Scatterplot(ipyView) {
  const id = ipyView.id;
  let progressivis_data = null;
  const margin = { top: 20, right: 20, bottom: 30, left: 40 };
  const width = 960 - margin.left - margin.right;
  const height = 500 - margin.top - margin.bottom;
  let svg;
  let prevBounds = null;
  let transform = d3.zoomIdentity;
  let centroid_selection = {};
  let collection_in_progress = false;
  const x = d3.scaleLinear().range([0, width]);
  const y = d3.scaleLinear().range([height, 0]);
  //let color = d3.scaleOrdinal(d3.schemeCategory10),
  const xAxis = d3
    .axisBottom(x)
    .tickSize(height)
    .tickPadding(8 - height);
  const yAxis = d3
    .axisRight(y)
    .tickSize(width)
    .tickPadding(8 - width);
  let zoom = d3
    .zoom()
    //.scaleExtent([1, 32])
    .on("zoom", multiclass2d_zoomed_handler);

  let gX;
  let gY;
  let zoomable;
  let dataURL = null;
  const imageHistory = new History(MAX_PREV_IMAGES);

  function with_id(prefix) {
    return prefix + "_" + id;
  }
  function s(id) {
    return "#" + id;
  }
  function swith_id(id) {
    return s(with_id(id));
  }

  function multiclass2d_dragstart(event, d) {
    event.sourceEvent.stopPropagation();
    d3.select(this).classed("dragging", true);
  }

  function multiclass2d_dragmove(event, d) {
    d[0] = xAxis.scale().invert(event.x);
    d[1] = yAxis.scale().invert(event.y);
    d3.select(this).attr("cx", event.x).attr("cy", event.y);
  }

  function template(element) {
    let temp = document.querySelector("#Scatterplot");
    if (temp === null) {
      // Install the template as a dom template node
      temp = document.createElement("template");
      temp.setAttribute("id", "Scatterplot");
      temp.innerHTML = `<!-- Tab panes -->
  <div class="tab-content">
    <div >
      <div id=''>
        <svg>
          <filter id="gaussianBlur" width="100%" height="100%" x="0" y="0">
            <feGaussianBlur id="gaussianBlurElement" in="SourceGraphic" stdDeviation="0" />
            <feComponentTransfer id="colorMap">
              <feFuncR type="table" tableValues="1 1 1"/>
              <feFuncG type="table" tableValues="0.93 0.001 0"/>
              <feFuncB type="table" tableValues="0.63 0.001 0"/>
            </feComponentTransfer>
          </filter>
        </svg>
        <br/>
        <div class="form-inline">
          <button class='btn btn-default' id='filter' type="button" aria-label='Filter button'>Filter to viewport</button>
          <button class='btn btn-default' id='init_centroids' type="button" aria-label='Centroids selection'>Selection</button>
          <button class='btn btn-default' id='cancel_centroids' type="button" aria-label='Cancel centroids'>Cancel</button>
          <div class="form-group">
            <label>Blur radius</label>
            <input class="form-control" id="filterSlider" type="range" value="0" min="0" max="5" step="0.1"></input>
          </div>
          <div class="form-group">
            <label>Color map</label>
            <select id="colorMapSelect" class="form-control"></select>
          </div>
          <div class="form-group">
            <a id="config-btn" role="button" class="btn btn-large btn-default">
              Configure ...
            </a>
          </div>
          <div  id="historyGrp" style="height:80px;">
            <label>History</label>
            <!--table class="matable" border="1" style="width:500px;height:80px;"><tr-->
            <table style="height:120px;border:1px solid black;border-collapse: collapse;">
             <td width='360px'  id="prevImages"></td></tr>
            </table>
          </div>
          <br/><br/><br/><br/>
        </div>
      </div>
    </div>
  </div>
  <div id="heatmapContainer" style="width:512px; height:512px;display: none;"></div>
  <!-- MDM form -->
  <div id="mdm-form" style="display: none;">
    <div >
      <div >
        <div >
          <h2>Multiclass Density Map Editor</h2>
        </div>
        <table><tr>
            <td id="root"></td>
            <td id='map-legend'></td>
        </tr></table>
      </div>
    </div>
  </div>
  <script>
  </script>`;
      document.body.appendChild(temp);
    }
    const templateClone = temp.content.cloneNode(true);
    // Rename all the ids to be unique
    const with_ids = templateClone.querySelectorAll("[id]");
    const ids = new Set();

    for (const element of with_ids) {
      const eid = element.id ? with_id(element.id) : with_id("Scatterplot");
      if (ids.has(eid)) {
        console.log(`Error in Scatterplot.template(), duplicate id '${eid}'`);
        // TODO fix it
      }
      element.id = eid;
    }

    element.appendChild(templateClone);
    this.cedit = register_config_editor(id);
  }

  function multiclass2d_dragend(event, d, i) {
    const msg = {};
    d3.select(this).classed("dragging", false);
    if (collection_in_progress) {
      d3.select(this).style("fill", "green");
      centroid_selection[i] = d;
    } else {
      msg[i] = d;
      ipyView.model.set("move_point", msg);
      ipyView.touch();
    }
  }

  const node_drag = d3
    .drag()
    .on("start", multiclass2d_dragstart)
    .on("drag", multiclass2d_dragmove)
    .on("end", multiclass2d_dragend);

  function multiclass2d_update_vis(rawdata) {
    progressivis_data = rawdata;
    const heatmapContainer = with_id("heatmapContainer");
    const bounds = rawdata.bounds;
    if (!bounds) return;
    const sc = rawdata.samples_counter;
    const sc_sum = sc.reduce((a, b) => a + b, 0);
    const st = ipyView.model.get("samples");
    const index = [...Array(sc_sum).keys()];
    const rows = Array();
    for (let z in [...Array(st.shape[2])]) {
      z = parseInt(z);
      let nsam = sc[z];
      for (let i in [...Array(nsam)]) {
        rows.push([st.get(i, 0, z), st.get(i, 1, z), z]);
      }
    }
    const dot_color = ["red", "blue", "green", "cyan", "orange"];
    const data_ = rawdata.chart;
    const hist_tensor = ipyView.model.get("hists");
    for (let s in data_.buffers) {
      let i = parseInt(s);
      const h_pick = hist_tensor.pick(null, null, i);
      data_.buffers[i].binnedPixels = ndarray_unpack(h_pick);
    }
    function render(spec, data) {
      const config = new Config(spec);
      config.loadJson(data).then(() => {
        const interp = new Interpreter(config);
        elementReady(s(heatmapContainer)).then(() => {
          interp.interpret();
          return interp.render(document.getElementById(heatmapContainer));
        });
      });
    }
    render(ipyView.scatterplot.cedit.conf.spec, data_);
    elementReady(`#${heatmapContainer} canvas`).then((that) => {
      dataURL = $(that)[0].toDataURL();
      ipyView.scatterplot.cedit.conf.spec.data = {}; // do not remove!
      imageHistory.enqueueUnique(dataURL);
      $(`${swith_id("map-legend")}`).empty();
      $(`#${heatmapContainer} svg`)
        .last()
        .detach()
        .appendTo(`#${with_id("map-legend")}`);
      $(`#${heatmapContainer} canvas`)
        .last()
        .detach()
        .attr("style", "position: relative; left: -120px; top: 10px;")
        .appendTo(swith_id("map-legend")); //blend
      $(s(heatmapContainer)).html("");
      if (prevBounds == null) {
        // first display, not refresh
        prevBounds = bounds;
        x.domain([bounds.xmin, bounds.xmax]).nice();
        y.domain([bounds.ymin, bounds.ymax]).nice();
        zoomable = svg
          .append("g")
          .attr("id", with_id("zoomable"))
          .attr(
            "transform",
            "translate(" + margin.left + "," + margin.top + ")",
          );

        const ix = x(bounds.xmin);
        const iy = y(bounds.ymax);
        const iw = x(bounds.xmax) - ix;
        const ih = y(bounds.ymin) - iy;

        zoomable
          .append("image")
          .attr("class", "heatmap")
          .style("pointer-events", "none")
          .attr("xlink:href", dataURL)
          .attr("preserveAspectRatio", "none")
          .attr("x", ix)
          .attr("y", iy)
          .attr("width", iw)
          .attr("height", ih)
          .attr("filter", `url(${swith_id("gaussianBlur")})`);

        svg
          .append("image")
          .attr("class", "heatmapCompare")
          .style("pointer-events", "none")
          .attr("preserveAspectRatio", "none")
          .attr("opacity", 0.5)
          .attr("x", ix)
          .attr("y", iy)
          .attr("width", iw)
          .attr("height", ih);

        gX = svg.append("g").attr("class", "x axis axis--x").call(xAxis);

        gY = svg.append("g").attr("class", "y axis axis--y").call(yAxis);

        svg.call(zoom);
        //firstTime = false;
      } else {
        // prevBounds != null
        let changed = false;
        for (const v in prevBounds) {
          if (prevBounds[v] != bounds[v]) {
            changed = true;
            break;
          }
        }
        if (changed) {
          console.log("Bounds have changed");
          prevBounds = bounds;
          x.domain([bounds.xmin, bounds.xmax]).nice();
          y.domain([bounds.ymin, bounds.ymax]).nice();
          transform = compute_transform(x, y, xAxis.scale(), yAxis.scale());
          svg.__zoom = transform; // HACK
          multiclass2d_zoomed_impl(transform);
        }

        const ix = x(bounds.xmin);
        const iy = y(bounds.ymax);
        const iw = x(bounds.xmax) - ix;
        const ih = y(bounds.ymin) - iy;
        svg
          .select(`${swith_id("Scatterplot")} .heatmap`)
          .attr("x", ix)
          .attr("y", iy)
          .attr("width", iw)
          .attr("height", ih)
          .attr("xlink:href", dataURL);

        svg
          .select(`${swith_id("Scatterplot")} .heatmapCompare`)
          .attr("x", ix)
          .attr("y", iy)
          .attr("width", iw)
          .attr("height", ih);
      }
      const prevImgElements = d3
        .select(swith_id("prevImages"))
        .selectAll("img")
        .data(imageHistory.getItems(), (d) => d);

      prevImgElements
        .enter()
        .append("img")
        .attr("width", 50)
        .attr("height", 50)
        .on("mouseover", (evt, d) => {
          d3.select(`${swith_id("Scatterplot")} .heatmapCompare`)
            .attr("xlink:href", d)
            .attr("visibility", "inherit");
        })
        .on("mouseout", () => {
          d3.select(`${swith_id("Scatterplot")} .heatmapCompare`).attr(
            "visibility",
            "hidden",
          );
        });

      prevImgElements
        .transition()
        .duration(500)
        .attr("src", (d) => d)
        .attr("width", 100)
        .attr("height", 100);

      prevImgElements
        .exit()
        .transition()
        .duration(500)
        .attr("width", 5)
        .attr("height", 5)
        .remove();

      const dots = zoomable.selectAll(".dot").data(rows, (d, i) => index[i]);

      dots
        .enter()
        .append("circle")
        .attr("class", "dot")
        .attr("r", 3.5 / transform.k)
        .attr("cx", (d) => x(d[0])) // use untransformed x0/y0
        .attr("cy", (d) => y(d[1]))
        .style("fill", (d) => dot_color[d[2]])
        .call(node_drag)
        .append("title")
        .text((d, i) => index[i]);
      dots
        .attr("cx", (d) => x(d[0]))
        .attr("cy", (d) => y(d[1]))
        .style("fill", (d) => dot_color[d[2]]);
      dots.exit().remove();
      dots.order();
    }); //end elementReady
  }
  /*
    function multiclass2d_zoomed(event, t) {
    if (t === undefined) t = event.transform;
    transform = t;
    gX.call(xAxis.scale(transform.rescaleX(x)));
    gY.call(yAxis.scale(transform.rescaleY(y)));
    zoomable.attr('transform', transform);
    svg.selectAll('.dot').attr('r', 3.5 / transform.k);
  }
*/
  function multiclass2d_zoomed_impl(transform) {
    gX.call(xAxis.scale(transform.rescaleX(x)));
    gY.call(yAxis.scale(transform.rescaleY(y)));
    zoomable.attr("transform", transform);
    svg.selectAll(".dot").attr("r", 3.5 / transform.k);
  }
  function multiclass2d_zoomed_handler(event, d) {
    multiclass2d_zoomed_impl(event.transform);
  }
  function delta(d) {
    return d[1] - d[0];
  }

  function compute_transform(x, y, x0, y0) {
    const K0 = delta(x.domain()) / delta(x0.domain());
    const K1 = delta(y.domain()) / delta(y0.domain());
    const K = Math.min(K0, K1);
    const X = -x(x0.invert(0)) * K;
    const Y = -y(y0.invert(0)) * K;
    return d3.zoomIdentity.translate(X, Y).scale(K);
  }

  /**
   * @param select - a select element that will be mutated
   * @param names - list of option names (the value of an option is set to its name)
   */
  function makeOptions(select, names) {
    if (!select) {
      console.warn("makeOptions requires an existing select element");
      return;
    }
    names.forEach((name) => {
      const option = document.createElement("option");
      option.setAttribute("value", name);
      option.innerHTML = name;
      select.appendChild(option);
    });
  }

  // function multiclass2d_filter_debug() {
  //     ipyView.model.set('value', 333);
  //     //ipyView.model.save_changes();
  //     ipyView.touch();
  // }
  function multiclass2d_filter() {
    const xscale = xAxis.scale();
    const xmin = xscale.invert(0);
    const xmax = xscale.invert(width);
    const yscale = yAxis.scale();
    const ymin = yscale.invert(height);
    const ymax = yscale.invert(0);
    const bounds = prevBounds;
    const columns = progressivis_data.columns;
    const min = {};
    const max = {};

    if (xmin != bounds.xmin) min[columns[0]] = xmin;
    else min[columns[0]] = null; // NaN means bump to min
    if (xmax != bounds.xmax) max[columns[0]] = xmax;
    else max[columns[0]] = null;

    if (ymin != bounds.ymin) min[columns[1]] = ymin;
    else min[columns[1]] = null;

    if (ymax != bounds.ymax) max[columns[1]] = ymax;
    else max[columns[1]] = null;
    console.log("min:", min);
    console.log("max:", max);
    ipyView.model.set("value", { min: min, max: max });
    //ipyView.model.save_changes();
    ipyView.touch();
  }

  function move_centroids() {
    const txt = $(swith_id("init_centroids")).html();
    if (txt == "Selection") {
      $(swith_id("init_centroids")).html("Commit");
      $(swith_id("cancel_centroids")).show();
      collection_in_progress = true;
      centroid_selection = {};
      ipyView.model.set("modal", true);
    } else {
      $(swith_id("init_centroids")).html("Selection");
      collection_in_progress = false;
      console.log(centroid_selection);
      ipyView.model.set("move_point", centroid_selection);
      centroid_selection = {};
      ipyView.model.set("modal", false);
      $(swith_id("cancel_centroids")).hide();
    }
    ipyView.touch();
  }

  function cancel_centroids() {
    $(swith_id("init_centroids")).html("Selection");
    collection_in_progress = false;
    console.log("Cancel ...");
    centroid_selection = {};
    ipyView.model.set("modal", false);
    ipyView.touch();
    $(swith_id("cancel_centroids")).hide();
  }

  function multiclass2d_ready() {
    svg = d3
      .select(swith_id("Scatterplot") + " svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom);

    $(swith_id("nav-tabs") + " a").click(function (e) {
      e.preventDefault();
      $(this).tab("show");
    });
    prevBounds = null;
    const gaussianBlur = document.getElementById(
      with_id("gaussianBlurElement"),
    );
    const filterSlider = $(swith_id("filterSlider"));
    filterSlider.change(function () {
      const value = this.value;
      gaussianBlur.setStdDeviation(value, value);
    });
    filterSlider.get(0).value = DEFAULT_SIGMA;
    gaussianBlur.setStdDeviation(DEFAULT_SIGMA, DEFAULT_SIGMA);

    const colorMap = document.getElementById(with_id("colorMap"));
    const colorMapSelect = $(swith_id("colorMapSelect"));
    colorMapSelect.change(function () {
      colormaps.makeTableFilter(colorMap, this.value);
    });
    colorMapSelect.get(0).value = DEFAULT_FILTER;
    makeOptions(colorMapSelect.get(0), colormaps.getTableNames());
    colormaps.makeTableFilter(colorMap, "Default");
    $(swith_id("filter"))
      .unbind("click")
      .click(() => multiclass2d_filter());
    $(swith_id("init_centroids")).click(() => move_centroids());
    $(swith_id("cancel_centroids"))
      .click(() => cancel_centroids())
      .hide();
    let mode = ipyView.model.get("modal");
    let to_hide = ipyView.model.get("to_hide");
    for (const i in to_hide) $(s(to_hide[i])).hide();
    ipyView.model.set("modal", !mode);
    ipyView.touch();
    ipyView.model.set("modal", mode);
    ipyView.touch();
  }

  return {
    update_vis: multiclass2d_update_vis,
    ready: multiclass2d_ready,
    template: template,
    with_id: with_id,
  };
}
