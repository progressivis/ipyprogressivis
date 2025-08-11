/*
  This file contains substantial portions of https://github.com/e-/PANENE/blob/7f4c3b7e84428fc6a60a9c2d3bc80b9408d437a9/examples/tsne/visualizer.html, developped by Jaemin Jo, ean-Daniel Fekete, Jinwook Seo  which has the following license:

BSD 2-Clause License

Copyright (c) 2016-2018, Jaemin Jo (jmjo@hcil.snu.ac.kr)
Copyright (c) 2016-2018, Jean-Daniel Fekete (Jean-Daniel.Fekete@inria.fr)
Copyright (c) 2016-2018, Jinwook Seo (jseo@snu.ac.kr)
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

"use strict";
import * as d3 from "d3";
import * as widgets from "@jupyter-widgets/base";
import { new_id } from "./base";
import { elementReady } from "./es6-element-ready";
import $ from "jquery";
import { table_serialization, rowProxy } from "jupyter-tablewidgets";

function translate(x, y) {
  return "translate(" + x + "," + y + ")";
}

function shuffle(array) {
  let counter = array.length;
  // While there are elements in the array
  while (counter > 0) {
    // Pick a random index
    let index = Math.floor(Math.random() * counter);

    // Decrease counter by 1
    counter--;

    // And swap the last element with it
    let temp = array[counter];
    array[counter] = array[index];
    array[index] = temp;
  }
  return array;
}

function draw(svgId) {
  return function (table) {
    if (table === null) return;
    $(svgId).empty();
    let iter = "iter"; //line.iter;
    let time = "time"; //line.time;
    let error = "error"; //line.error;
    let labels = "labels"; //labels
    let proxy = rowProxy(table);
    let data = Array(table.size);
    let cols = table.columns;
    let col_x = cols[0];
    let col_y = cols[1];
    for (let i = 0; i < data.length; ++i) {
      let pxi = proxy(i);
      data[i] = [pxi[col_x], pxi[col_y]];
    }
    let sampleSize = Math.min(1000, data.length);
    let sampleIndex = shuffle(d3.range(data.length)).slice(0, sampleSize);
    let svg = d3.select(svgId);
    let label = 35;
    let margin = 15;
    let width = svg.attr("width");
    let height = svg.attr("height");
    let te = svg
      .append("text")
      .attr("text-anchor", "middle")
      .attr("dy", "-0.6em")
      .attr("transform", translate(width / 2, height + label - 10))
      .text("Iteration #" + +iter);

    let te2 = svg
      .append("text")
      .attr("text-anchor", "middle")
      .attr("dy", "-0.6em")
      .style("font-size", "0.9em")
      .attr("transform", translate(width / 2, height + label + 5))
      .text(
        "(" +
          d3.format(",.1f")(time) +
          "s, error=" +
          d3.format(".2f")(error) +
          ")",
      );

    let x = d3
      .scaleLinear()
      .domain(d3.extent(data, (d) => d[0]))
      .range([0, width - 2 * margin]);
    let y = d3
      .scaleLinear()
      .domain(d3.extent(data, (d) => d[1]))
      .range([0, height - 2 * margin]);

    let root = svg.append("g").attr("transform", translate(margin, margin));
    let contours = d3
      .contourDensity()
      .x((d) => x(d[0]))
      .y((d) => y(d[1]))
      .size([width - 2 * margin, height - 2 * margin])
      .bandwidth(4)(data);

    let path = root.append("g").selectAll("path").data(contours);

    let color = d3
      .scaleSequential(d3.interpolateViridis)
      .domain(d3.extent(contours, (d) => d.value));

    let enter = path.enter().append("path");

    path
      .merge(enter)
      .attr("d", d3.geoPath())
      .attr("fill", (d) => color(d.value));

    path.exit().remove();

    let circle = root.append("g").selectAll("circle");
    let categorical = d3.scaleOrdinal(d3.schemeCategory10).domain(d3.range(10));

    circle
      .data(sampleIndex)
      .enter()
      .append("circle")
      .attr("cx", (d) => x(data[d][0]))
      .attr("cy", (d) => y(data[d][1]))
      .attr("r", 1.5)
      .style("fill", (d, i) => (labels ? categorical(labels[d]) : "#aaa"));
  };
}

export class ContourDensityModel extends widgets.DOMWidgetModel {
  defaults() {
    return {
      ...super.defaults(),
      _model_name: "ContourDensityModel",
      _view_name: "ContourDensityView",
      _model_module: "jupyter-progressivis",
      _view_module: "jupyter-progressivis",
      _model_module_version: "0.1.0",
      _view_module_version: "0.1.0",
      _df: ndarray([]),
    };
  }
  static serializers = {
    ...widgets.DOMWidgetModel.serializers,
    _df: table_serialization,
  };
}

// Custom View. Renders the widget model.
export class ContourDensityView extends widgets.DOMWidgetView {
  // Defines how the widget gets rendered into the DOM
  render() {
    this.id = `contour_density_${new_id()}`;
    console.log("this id", this.id);
    this.el.innerHTML =
      "<svg width='400' height='400' id='" + this.id + "'></svg>";
    let that = this;
    elementReady(`#${this.id}`).then(() => {
      that.draw_ = draw(`#${that.id}`);
    });
    this.data_changed();
    this.model.on("change:_df", this.data_changed, this);
  }
  data_changed() {
    const that = this;
    elementReady(`#${this.id}`).then(() => {
      that.draw_(that.model.get("_df"));
    });
  }
}
