/*
  This file contains substantial portions of https://github.com/e-/PANENE/blob/97c5fc774a6edb7ce1bf48928a99811e0eda881f/examples/kernel_density/online_visualizer.html, developped by Jaemin Jo, ean-Daniel Fekete, Jinwook Seo  which has the following license:

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

function translate(x, y) {
  return "translate(" + x + "," + y + ")";
}

function DensityPlot(svgId) {
  let width = 380,
    height = 380;
  let svg = d3.select(svgId).append("g").attr("transform", translate(10, 10));
  let bg = svg.append("g");
  let fg = svg.append("g");
  let step = 10;

  return function (data) {
    data.points.forEach((d) => {
      d[0] = +d[0]; // x
      d[1] = +d[1]; // y
    });

    data.samples.forEach((d) => {
      d[0][0] = +d[0][0]; // x
      d[0][1] = +d[0][1]; // y
      d[1] = +d[1]; // density
    });

    let x = d3
      .scaleLinear()
      .domain(d3.extent(data.samples, (d) => d[0][0]))
      .range([0, width]);
    let y = d3
      .scaleLinear()
      .domain(d3.extent(data.samples, (d) => d[0][1]))
      .range([0, height]);

    fg.selectAll("circle")
      .data(data.points)
      .enter()
      .append("circle")
      .attr("r", 3)
      .attr("fill", "white")
      .attr("stroke", "black")
      .attr("stroke-width", "2px")
      .attr("cx", (d) => x(d[0]))
      .attr("cy", (d) => y(d[1]))
      .attr("opacity", 0.7);

    let bins = +data.bins;
    let color = d3
      .scaleSequential(d3.interpolateCool)
      .domain([0, d3.max(data.samples, (d) => d[1])]);
    let densityMax = d3.max(data.samples, (x) => x[1]);

    let paths = bg.selectAll("path").data(
      d3
        .contours()
        .size([bins + 1, bins + 1])
        .thresholds(d3.range(0, densityMax, densityMax / step))(
        data.samples.map((d) => d[1]),
      ),
    );

    let enter = paths.enter().append("path");

    paths
      .merge(enter)
      .attr(
        "d",
        d3.geoPath(
          d3.geoTransform({
            point: function (x, y) {
              let s = width / bins;
              this.stream.point(y * s - s * 0.5, x * s - s * 0.5);
            },
          }),
        ),
      )
      .attr("fill", (d) => color(d.value));
  };
}

export class KNNKernelModel extends widgets.DOMWidgetModel {
  defaults() {
    return {
      ...super.defaults(),
      _model_name: "KNNKernelModel",
      _view_name: "KNNKernelView",
      _model_module: "jupyter-progressivis",
      _view_module: "jupyter-progressivis",
      _model_module_version: "0.1.0",
      _view_module_version: "0.1.0",
      data: "{}",
    };
  }
}

// Custom View. Renders the widget model.
export class KNNKernelView extends widgets.DOMWidgetView {
  // Defines how the widget gets rendered into the DOM
  render() {
    this.id = `knn_kernel_${new_id()}`;
    console.log("this id", this.id);
    this.el.innerHTML =
      "<svg width='400' height='400' id='" + this.id + "'></svg>";
    let that = this;
    elementReady(`#${this.id}`).then(() => {
      that.densityPlot = DensityPlot(`#${that.id}`);
    });
    this.data_changed();
    this.model.on("change:data", this.data_changed, this);
  }
  data_changed() {
    const that = this;
    elementReady(`#${this.id}`).then(() => {
      that.densityPlot(that.model.get("data"));
    });
  }
}
