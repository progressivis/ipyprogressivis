/*
This file contains substantial portions of https://github.com/nivan/jupyter_contrib_nbextensions/blob/750fa49c7457be7367471c197d687e45e5d0934e/src/jupyter_contrib_nbextensions/nbextensions/toc2/toc2.js, developped by Nivan Ferreira  which has the following license:

Licensing terms
===============

This project is licensed under the terms of the Modified BSD License
(also known as New or Revised or 3-Clause BSD), as follows:

- Copyright (c) 2016 jupyter-contrib developers

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the project development team nor the names of its
contributors may be used to endorse or promote products derived from this
software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

import * as d3 from 'd3';

    function installDagCanvas(container) {
        //
        window.attentionDetailsMapTypeID = { "RESCALE_NEEDED": "attAxesLabel", "PROGRESS_NOTIFICATION": "attProgressLabel", "STABILITY_REACHED": "attStabilityLabel", "SAFEGUARD_SATISFIED": "attSafeguardLabel" };
        window.attentionColorScale = d3.scaleOrdinal()
            .domain(["PROCESSING_ERROR", "RESCALE_NEEDED", "STABILITY_REACHED", "SAFEGUARD_SATISFIED", "MULTIPLE"])
            .range(['#fb9a99', '#a6cee3', '#ccebc5', '#b2df8a', '#e31a1c']);
        //
        var field = container.append('fieldset').attr("style", "width:300px");
        field.append('legend').text('Widget Dag ');
        var verticalBar = field.append('div').attr('style', 'margin-inline-start: 1rem;');
        //
        field.append('div')
            .html(`
        <div style="border:1px solid #EEE;width:300px;margin-top: 0;">
<p>Attention Request Types</p>
<div id="dagLegend" style="width:300px;display: grid;grid-template-columns: 150px 150px;margin-left: 10px;">
  <div class="legendItem">
    <div class="circleBase" style="border:1px solid #999;background:#fb9a99;"></div>
    <label>Error</label>
  </div>
  <div class="legendItem">
      <div class="circleBase" style="border:1px solid #999;background:#a6cee3;"></div>
      <label>Axes</label>
    </div>

    <div class="legendItem">
      <div class="circleBase" style="border:1px solid #999;background:#b2df8a;">
      </div>
      <label>Safeguard</label>
    </div>

    <div class="legendItem">
    <div class="circleBase" style="border:1px solid #999;background:#e31a1c;">
    </div>
    <label>Multiple</label>
  </div>

 </div>
</div>`);
        //
        // verticalBar.append('i').attr('class', "fa fa-home").attr('aria-hidden', 'true').attr('style', 'cursor: pointer; ');
        // verticalBar.append('i').attr('class', "fa fa-trash-o").attr('aria-hidden', 'true').attr('style', 'cursor: pointer; ');
        //
        var borderDiv = field.append('div').attr('style', 'border: 1px solid grey;');
        var svg = borderDiv
            .append('div')
            .append('svg')
            .attr('id', 'dagCanvas')
            .attr('width', 300)
            .attr('height', 450);
    }

    function installDetailsForm(container) {
        var mainForm = container.append('form').attr("style", "width:300px");
        var fieldset = mainForm.append('fieldset');
        fieldset.append('legend').text('Details ');

        var fields = fieldset.append('div').attr('style', 'width:300px;display: grid;grid-template-columns: 100px 200px;');
        fields.append("label").text('Widget: ');
        fields.append("label").attr('id', 'detailsNameLabel').text('');
        fields.append("label").text('Progress: ');
        var borderProgressbar = fields.append("div").attr('class', 'w3-border');
        borderProgressbar.append('div').attr('id', 'detailsProgressBar').attr('class', "w3-grey w3-center")
            .attr("style", "color: #000;background-color: #d0d0d0;width:0%;").text('0%');
        //
        var attentionFields = fieldset.append('div').attr('style', 'margin-top:5px;border-top: 1.5px solid grey;width:300px;display: grid;grid-template-columns: 90px 200px 10px;');
        attentionFields.append("label").text('Axes Resize: ');
        attentionFields.append("label").attr('id', 'attAxesLabel');
        attentionFields.append("i")
            .attr('class', 'fa fa-fw fa-refresh')
            .attr('style', 'cursor: pointer;')
            .on('click', function () {
                d3.select(this).classed('fa-spin', true);
                if (window.dagController && window.dagController.removeAttRqs) {
                    window.dagController.removeAttRqs(window.dagController.selectedWidget, 'RESCALE_NEEDED');
                }
                d3.select(this).classed('fa-spin', false);
            });


        attentionFields.append("label").text('Stability: ');
        attentionFields.append("label").attr('id', 'attStabilityLabel');
        attentionFields.append("i")
            .attr('class', 'fa fa-fw fa-refresh')
            .attr('style', 'cursor: pointer;')
            .on('click', function () {
                d3.select(this).classed('fa-spin', true);
                if (window.dagController && window.dagController.removeAttRqs) {
                    window.dagController.removeAttRqs(window.dagController.selectedWidget, 'STABILITY_REACHED');
                }
                d3.select(this).classed('fa-spin', false);
            });

        attentionFields.append("label").text('Progress: ');
        attentionFields.append("label").attr('id', 'attProgressLabel');
        attentionFields.append("i")
            .attr('class', 'fa fa-fw fa-refresh')
            .attr('style', 'cursor: pointer;')
            .on('click', function () {
                d3.select(this).classed('fa-spin', true);
                if (window.dagController && window.dagController.removeAttRqs) {
                    window.dagController.removeAttRqs(window.dagController.selectedWidget, 'PROGRESS_NOTIFICATION');
                }
                d3.select(this).classed('fa-spin', false);
            });

        attentionFields.append("label").text('Safeguard: ');
        attentionFields.append("label").attr('id', 'attSafeguardLabel');
        attentionFields.append("i")
            .attr('class', 'fa fa-fw fa-refresh')
            .attr('style', 'cursor: pointer;')
            .on('click', function () {
                d3.select(this).classed('fa-spin', true);
                if (window.dagController && window.dagController.removeAttRqs) {
                    //console.log('CALLING REMOVE');
                    window.dagController.removeAttRqs(window.dagController.selectedWidget, 'SAFEGUARD_SATISFIED');
                }
                d3.select(this).classed('fa-spin', false);
            });
    }

export function installInterface(container) {
        //
        var dagDiv = container.append('div');
        installDagCanvas(dagDiv);
        //
        var detailsDiv = container.append('div');
        installDetailsForm(detailsDiv);
    }

