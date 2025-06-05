/*
This file contains substantial portions of https://github.com/nivan/dagWidget/blob/daa8f10ac383252da50b52ff4169c4031ac4a69e/js/lib/example.js, developped by Nivan Ferreira  which has the following license:

MIT License

Copyright (c) 2023 Nivan Ferreira

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/
import '../css/main.css';
import { DOMWidgetModel, DOMWidgetView } from '@jupyter-widgets/base';
import * as d3 from 'd3';
import { installInterface } from './functions';
import { elementReady } from './es6-element-ready';
import { new_id } from './base';
var d3_dag = require('d3-dag');

export class DagWidgetModel extends DOMWidgetModel {
    defaults() {
      return {
        ...super.defaults(),
        _model_name : 'DagWidgetModel',
        _view_name : 'DagWidgetView',
        _model_module : 'jupyter-progressivis',
        _view_module : 'jupyter-progressivis',
        _model_module_version : '0.1.0',
        _view_module_version : '0.1.0',
	summaries: '{}',
        dag: '[]',
        attention_requests: '{}',
      };
    }
  }

export class DagWidgetView extends DOMWidgetView {
    render() {
	    this.id = 'dag_widget_' + new_id();
    this.el.innerHTML = `<div id="${this.id}" width="960" height="500"></div>`;
    const that = this;
	elementReady('#' + this.id).then(() => {
	    console.log("dag widget", this.id);
	var mainDiv = d3.select('#' + this.id);
            installInterface(mainDiv, '_'+this.id);
	    try{d3.select('#' + this.id).show();} catch(err){};

    });
        this.value_changed();

        // Observe and act on future changes to the value attribute
        this.model.on('change:value', this.value_changed, this);
        this.model.on('msg:custom', this.exec_js, this);
    }

    value_changed() {
        //this.el.textContent = this.model.get('value');
    }

    exec_js(msg) {
	eval(msg.js_code);
    }

    initialize() {
        console.log("INITIALIZE");
	var extId = '_'+this.id;
        //
        this.tocSupported = true; //Boolean(d3.select('#toc-wrapper').node());
        //
        if (!d3.dagStratify) {
            importDag();
        }

        //
        this._attRqs = {};
        this.selectedWidget = undefined;

        //
        if (!window.dagController)
            window.dagController = this;

        //
        if (this.safeguards == undefined) {
            this.safeguards = {};
        }

        //
        if (!(d3.select('#'+this.id).select('#safeGuardMenu'+extId).node())) {
            var menu = d3.select('#'+this.id).append('div')
                .attr('id', 'safeGuardMenu'+extId)
                .attr('style', 'position: absolute; width: 400px; height: 180px; left: -1000px; top: -1000px; background: white;border: 1px black solid;text-align: center;');

            var field = menu.append('fieldset');
            field.append('legend').attr('id', 'dialogTitleLabel'+extId).text('Add Safeguard to Widget');
            //
            var internalBody = menu.append('div').attr('style', 'display: grid;grid-template-columns: 120px 120px 120px;row-gap: 20px;column-gap:5px;margin-left: 20px;margin-right: 20px;text-align: center;')
            internalBody.append('label').text('Variable');
            internalBody.append('label').text('Operation');
            internalBody.append('label').text('Value');
            //
            var variableSelect = internalBody.append('select').attr('id', 'dialogSelectVariable'+extId).html(`<option value="">Variable</option>
            <option value="dog" class="varOption">Dog</option>
            <option value="cat" class="varOption">Cat</option>
            <option value="hamster" class="varOption">Hamster</option>
            <option value="parrot" class="varOption">Parrot</option>
            <option value="spider" class="varOption">Spider</option>
            <option value="goldfish" class="varOption">Goldfish</option>`);

            var opSelect = internalBody.append('select').attr('id', 'dialogSelectOperation'+extId).html(`<option value="">Operation</option>
<option value="eq"> == </option>
<option value="lt"> <  </option>
<option value="gt"> > </option>`);

            var valueInput = internalBody.append('input').attr('id', 'dialogValueInput'+extId).attr('type', 'number').attr('placeholder', 0.0).attr('step', 0.5).attr('value', 0.0);

            internalBody.append('label');//blank
            //
            var that = this;
            internalBody.append('input').attr('type', 'button').attr('value', 'Cancel')
                .on('click', function () {
                    d3.select('#safeGuardMenu'+extId).style('visibility', 'hidden');
                    //clear
                    d3.select('#dialogValueInput'+extId).node().value = 0.0;
                    d3.select('#dialogSelectVariable'+extId).node().value = "";
                    d3.select('#dialogSelectOperation'+extId).node().value = "";
                });
            internalBody.append('input').attr('type', 'button').attr('value', 'OK')
                .on('click', function () {

                    //
                    var selectedVariable = d3.select('#dialogSelectVariable'+extId).node().selectedOptions[0].innerText;
                    var selectedOp = d3.select('#dialogSelectOperation'+extId).node().selectedOptions[0].innerText;
                    var selectedValue = +(d3.select('#dialogValueInput'+extId).node().value);
                    if (selectedVariable != "Variable" && selectedOp != "Operation") {
                        //
                        var widgetID = d3.select('#safeGuardMenu'+extId).attr('widgetID');
                        that.safeguards[widgetID] = { 'var': selectedVariable, 'opr': selectedOp, 'thr': selectedValue };
                        //clear
                        d3.select('#dialogValueInput'+extId).node().value = 0.0;
                        d3.select('#dialogSelectVariable'+extId).node().value = "";
                        d3.select('#dialogSelectOperation'+extId).node().value = "";
                        d3.select('#safeGuardMenu'+extId).style('visibility', 'hidden');
                        console.log('new safeguard', this);
                    }
                    else {
                        console.log('***', selectedVariable, selectedOp, selectedValue);
                    }
                });
        }

        //
        // var hoverGroup = d3.select('#dagCanvas')
        //     .append('g')
        //     .attr('id', 'dagHoverGroup');
        // hoverGroup.append('rect')
        //     .attr('id', 'dagHoverRect')
        //     .attr('fill', 'white');
        // hoverGroup.append('text')
        //     .attr('id', 'dagHoverText');

        //
        this.model.on('change:dag', this.dag_changed, this);
        this.model.on('change:attention_requests', this.attentionRequests, this);
        this.model.on('change:summaries', this.summariesChanged, this);
    }
    fillDetails(_id) {
        //only work if supported
        if (!this.tocSupported)
            return;

        //
	var extId = '_'+this.id;
        d3.select('#detailsNameLabel'+extId).text(_id);
        var summaries = JSON.parse(this.model.get('summaries'));
        if (_id in summaries) {
            //
            var summary = summaries[_id];
            d3.select('#detailsProgressBar'+extId)
                .style('height', '100%')
                .style('width', summary['progress'] + '%')
                .text(summary['progress'] + '%');
        }
        else {
            console.log('missing sumamries', summaries);
        }
        //
        //
        var attentionRequests = this._attRqs;
        var mapTypeID = window.attentionDetailsMapTypeID;

        //if there is at least one attention request for _id
        if (_id in attentionRequests) {
            var attRequests = attentionRequests[_id];
            for (var key in mapTypeID) {
                var description = "";
                if ((key in attRequests) && ('description' in attRequests[key]))
                    description = attRequests[key]['description'];
                var labelID = mapTypeID[key];
                d3.select("#" + labelID)
                    .text(description);
            }
        }
        //if _id is not in attentionRequest and selected
        //this means that there is no attention request left
        //and so we need to update the interface
        else if (this.selectedWidget == _id) {
            for (var key in mapTypeID) {
                var labelID = mapTypeID[key];
                d3.select("#" + labelID)
                    .text("");
            }
        }
    }
    summariesChanged() {
        //only work if supported
        if (!this.tocSupported)
            return;

        if (this.selectedWidget) {
            this.fillDetails(this.selectedWidget);
        }

        //show status
        // var summaries = JSON.parse(this.model.get('summaries'));
        // for (var key in summaries) {
        //     d3.select('#dagCanvas')
        //         .select('#nD' + key)
        //         .attr('fill', function () {
        //             if ((summaries[key]['status'] == 'RUNNING')) {
        //                 return '#cccccc';
        //             }
        //             else {
        //                 return "white";
        //             }
        //         });
        // }
        this.colorNodesBasedOnStatus();

        //show safeguard alerts
        for (var widgetID in this.safeguards) {
            var safeguard = this.safeguards[widgetID];//{'var':selectedVariable,'opr':selectedOp,'thr':selectedValue};
            if (widgetID in summaries && safeguard['var'] in summaries[widgetID]) {
                var currentValue = summaries[widgetID][safeguard['var']];
                if (eval(`${currentValue} ${safeguard['opr']} ${safeguard['thr']}`)) {
                    //safeguard satisfied
                    var circle = d3.select('#nD' + widgetID);
                    var x = circle.attr('cx');
                    var y = circle.attr('cy');
                    var r = circle.attr('r');
                }
            }
        }
    }
    clearAttentionRequests(_dag) {
        var presentIDs = _dag.map(d => d.id);
        for (var key in this._attRqs) {
            if (presentIDs.indexOf(key) == -1) {
                delete this._attRqs[key];
            }
        }
    }
    dag_changed() {
        //only work if supported
        if (!this.tocSupported)
            return;
        //
        var info = JSON.parse(this.model.get('dag'));
        var _dag = info.dag;
	var extId = '_'+this.id;
        //if nodes were removed clean attention requests
        //to only keep the requests for widgets registered
        //TODO: this should be in another function
        this.clearAttentionRequests(_dag);

        //clear dag view
        const edgeGroup = d3.select('#dagCanvas'+extId).select('#edgeGroup'+extId).node()
            ? d3.select('#dagCanvas'+extId).select('#edgeGroup'+extId)
            : d3.select('#dagCanvas'+extId).append('g')
                .attr('id', 'edgeGroup'+extId);
        edgeGroup.selectAll('*').remove();


        const nodeGroup = d3.select('#dagCanvas'+extId).select('#nodeGroup'+extId).node()
            ? d3.select('#dagCanvas'+extId).select('#nodeGroup'+extId)
            : d3.select('#dagCanvas'+extId).append('g')
                .attr('id', 'nodeGroup'+extId);
        nodeGroup.selectAll('*').remove();
        //
        if (_dag.length == 0) {
            //
            return;
        }
        //
        const dag = d3.dagStratify()(_dag);
        const nodeRadius = 15;
	const rectW = 70;
	const rectH = 15;
        //
        const layout = d3
            .sugiyama() // base layout
            .decross(d3.decrossOpt()) // minimize number of crossings
            .nodeSize(function (node) {
                //Niv return [(node ? 3.6 : 0.25) * nodeRadius, 3 * nodeRadius];
		return [(node ? 1.05 : 0.1) * rectW, 3 * rectH];
                //return [(node ? 3.6 : 0.25) * nodeRadius, 5 * nodeRadius];
            });

        //(node) => [(node ? 3.6 : 0.25) * nodeRadius, 3 * nodeRadius]); // set node size instead of constraining to fit
        const { width, height } = layout(dag);

        const line = d3
            .line()
            .curve(d3.curveCatmullRom)
            .x((d) => d.x)
            .y((d) => d.y);

        // Plot edges
        edgeGroup
            .selectAll("path")
            .data(dag.links())
            .join("path")
            .attr("d", ({ points }) => line(points))
            .attr('id', d => 'eS' + d.source.data.id + 'T' + d.target.data.id)
            .attr("fill", "none")
            .attr("stroke-width", 1)
            .attr("stroke", 'gray');

        //plot nodes
        nodeGroup.style('cursor', 'pointer');
        var that = this;
        const nodes = nodeGroup
            .selectAll("g")
            .data(dag.descendants())
            .enter()
            .append("g")
            .attr('id', d => 'gnD' + d.data.id)
            .attr("transform", ({ x, y }) => `translate(${x}, ${y})`)
            .on('mouseover', function (e) {
                d3.select(this).select('rect').attr('stroke-width', 5);
                console.log('hover');
                //
                // d3.select('#dagHoverText').text('Nivan');
                // var bbox = d3.select('#dagHoverText').node().getBBox();

                // d3.select('#dagHoverRect')
                //     .attr('width', Math.ceil(bbox.width))
                //     .attr('height', Math.ceil(bbox.height));
                // console.log(e);
                // d3.select('#dagHoverGroup').attr('transform', `translate(${e.screenX},${e.screenY})`);
            })
            .on('mouseout', function () {
                d3.select(this).select('rect').attr('stroke-width', 1);
                //d3.select('#dagCanvas').attr('transform', `translate(-1000,-1000)`);
            })
            .on('click', function () {
                var _id = d3.select(this).select('rect').attr('id').slice(2);
                that.selectedWidget = _id;
                var elt = info.dag.find(d => d.id == _id);
                //
                d3.select('#widgetNameForm'+extId).text(_id);
                if (elt) {
		    location.href = "#"+elt.divID;
                }
                //
                that.fillDetails(_id);
            }).on('contextmenu', function (e) {
                e.preventDefault();
                //
                var myID = this.id.slice(3);
                d3.select('#safeGuardMenu'+extId).attr('widgetID', myID);
                //
                var summaryVariables = JSON.parse(that.model.get('dag'))['summaryVariables'];
                if (myID in summaryVariables) {
                    var variables = summaryVariables[myID];
                    console.log('===>', that, variables);
                    //set title
                    d3.select('#dialogTitleLabel'+extId).text('Add Safeguard to ' + myID);
                    //set available variables
                    d3.select('#dialogSelectVariable'+extId)
                        .selectAll('.varOption')
                        .data(variables)
                        .join('option')
                        .attr('value', (d, i) => i + 1)
                        .attr('class', 'varOption')
                        .text(d => d);

                    //
                    var menu = d3.select('#safeGuardMenu'+extId)
                        .style('visibility', 'visible')
                        .style('left', e.pageX + 'px')
                        .style('top', e.pageY + 'px')
                        .style('z-index', '1000');
                }

            });
	function shorten_label(raw_label){
	    let len = raw_label.length;
	    if(len <= 10) return raw_label;
	    return raw_label.slice(0, 5)+"\u2026"+raw_label.slice(len-3, len); // u2026 is "..."

	}
/*
        // Plot node circles
        nodes
            .append('circle')
            .attr('id', d => 'nD' + d.data.id)
            .attr('class', 'dagNodes')
            .attr("r", nodeRadius)
            .attr("stroke", 'black');

        nodes.append('circle')
            .attr('id', d => 'alertnD' + d.data.id)
            .attr('class', 'alertCircles')
            .attr("r", nodeRadius / 3)
            .attr("cx", nodeRadius * 0.8)
            .attr("cy", -nodeRadius * 0.8)
            .attr("fill", 'orange')
            .attr("stroke", 'black')
            .attr("stroke-width", 0.5)
            .style('visibility', 'hidden');
*/
        nodes
            .append('svg:rect')
            .attr('id', d => 'nD' + d.data.id)
            .attr('rx', 5)
            .attr('ry', 5)
            .attr("x", -rectW/2)
            .attr("y", -rectH/2)
            .attr('class', 'dagNodes')
            .attr("width", rectW)
            .attr("height", rectH)
            .attr("stroke", 'black');

        this.colorNodesBasedOnStatus();

        // Add text to nodes
        nodes
            .append("text")
            .text((d) => shorten_label(d.data.label)) //shorten label to fit on
            .attr("font-weight", "normal")
            .attr("font-family", "sans-serif")
            .attr("text-anchor", "middle")
            .attr("alignment-baseline", "middle")
            .attr("fill", "black");

        //
	nodes.append("svg:title")
	    .text(function(d) { return d.data.label; })

        this.refreshAttentionVisuals();
    }
    colorNodesBasedOnStatus() {
	var extId = '_'+this.id;
        var summaries = JSON.parse(this.model.get('summaries'));
        d3.select('#dagCanvas'+extId)
            .selectAll('.dagNodes')
            .attr('fill', function (d) {
                if (d.data.id in summaries) {
                    if ((summaries[d.data.id]['status'] == 'RUNNING')) {
                        return '#a6cee3';
                    }
                    else if ((summaries[d.data.id]['status'] == 'PAUSED')) {
                        return '#cccccc';
                    }
                    else if ((summaries[d.data.id]['status'] == 'FINISHED')) {
                        return '#1f78b4';
                    }
                    else if ((summaries[d.data.id]['status'] == 'IDLE')) {
                        return "white";
                    }
                    else {
                        console.log('ERROR: Invalid Status');
                        return "black";
                    }
                }
                else {
                    return "white";
                }
            });
    }
    removeAttRqs(internalID, eventType) {
        //only work if supported
        if (!this.tocSupported)
            return;
        //
        if ((internalID in this._attRqs) &&
            (eventType in this._attRqs[internalID])) {
            delete this._attRqs[internalID][eventType]
        }

        if ((internalID in this._attRqs) &&
            (Object.keys(this._attRqs[internalID]).length == 0)) {
            delete this._attRqs[internalID];
        }
        //
        this.refreshAttentionVisuals();
    }
    refreshAttentionVisuals() {
        //
        var that = this;
        var colorScale = window.attentionColorScale;
	var extId = '_'+this.id;
        //TODO: CHANGE STROKE FOR SHOWING THAT THERE IS MORE THAN
        //ONE ATTENTION REQUEST
        d3.select('#nodeGroup'+extId)
            .selectAll('.alertCircles')
            .attr('fill', function () {
                var attentionRequests = that._attRqs;
                //
                var _id = d3.select(this).attr("id").slice(7);

                if (_id in attentionRequests) {
                    var attRequests = attentionRequests[_id];
                    var numRequests = Object.keys(attRequests).length;
                    if (numRequests > 1) {
                        return colorScale("MULTIPLE");
                    } else {
                        var attentionRequests = that._attRqs;

                        if (that.selectedWidget == _id) {
                            that.fillDetails(_id);
                        }

                        if (_id in attentionRequests) {
                            var attRequests = attentionRequests[_id];
                            //
                            var attRequest = undefined;
                            var requestTypes = colorScale.domain();
                            for (var index in requestTypes) {
                                var key = requestTypes[index];
                                if (key in attRequests) {
                                    attRequest = attRequests[key];
                                    break;
                                }
                            }
                            //
                            var color = colorScale(attRequest.type);
                            return color;
                        }
                    }
                }
                else {
                    return 'white';
                }
            })
            .style('visibility', function () {
                var _id = d3.select(this).attr("id").slice(7);
                var attentionRequests = that._attRqs;
                if (_id in attentionRequests)
                    return "visible";
                else
                    return "hidden";
            });
    }
    attentionRequests() {
        //only work if supported
        if (!this.tocSupported)
            return;
        //{'entityType':entityType,'widgetID':internalID,'type':eventType,'description':description}
        var newAttentionOperation = JSON.parse(this.model.get('attention_requests'));

        //
        var internalID = newAttentionOperation['widgetID'];
        var eventType = newAttentionOperation['type'];

        if (newAttentionOperation['op'] == 'add') {
            if (!(internalID in this._attRqs)) {
                this._attRqs[internalID] = {}
            }
            this._attRqs[internalID][eventType] = newAttentionOperation;
        }
        else if (newAttentionOperation['op'] == 'remove') {
            this.removeAttRqs(internalID, eventType);
        }

        this.refreshAttentionVisuals();
    }
}

function importDag(){
    d3 = Object.assign(d3 || {}, d3_dag);
}
