'use strict';
import * as widgets from '@jupyter-widgets/base';
import $ from 'jquery';
import { new_id } from './base';
import { elementReady } from './es6-element-ready';
import 'jquery-sparkline';

import '../css/sparkline-progressbar.css';

export class SparkLineProgressBarModel extends widgets.DOMWidgetModel {
    defaults() {
      return {
        ...super.defaults(),
    _model_name: 'SparkLineProgressBarModel',
    _view_name: 'SparkLineProgressBarView',
    _model_module: 'jupyter-progressivis',
    _view_module: 'jupyter-progressivis',
    _model_module_version: '0.1.0',
    _view_module_version: '0.1.0',
    data: '{}',
      };
    }
}

// Custom View. Renders the widget model.
export class SparkLineProgressBarView extends widgets.DOMWidgetView {
  // Defines how the widget gets rendered into the DOM
  render () {
    this.id = 'sparklink-bp_' + new_id();
    this.el.innerHTML = `<div style="width: 100%;"><div class="slpb-bg">
<span id='${this.id}' class="slpb-fill" style="width: 70%;"></span>
</div></div>`;
    this.data_changed();
    // Observe changes in the value traitlet in Python, and define
    // a custom callback.
    this.model.on('change:data', this.data_changed, this);
  }
  data_changed () {
    const that = this;
    elementReady('#' + that.id).then(() => update_slpb(that));
  }
}


export class PlottingProgressBarModel extends widgets.DOMWidgetModel {
    defaults() {
      return {
        ...super.defaults(),
    _model_name: 'PlottingProgressBarModel',
    _view_name: 'PlottingProgressBarView',
    _model_module: 'jupyter-progressivis',
    _view_module: 'jupyter-progressivis',
    _model_module_version: '0.1.0',
    _view_module_version: '0.1.0',
    data: '{}',
      };
    }
}

// Custom View. Renders the widget model.
export class PlottingProgressBarView extends widgets.DOMWidgetView {
  // Defines how the widget gets rendered into the DOM
  render () {
    this.id = 'plotting-pb' + new_id();
    this.el.innerHTML = `<div style="width: 100%;">
         <div class="slpb-bg">
           <div id='${this.id}' class="slpb-fill" style="width: 70%;"></div>
         </div>
        </div>`;
    this.data_changed();
    // Observe changes in the value traitlet in Python, and define
    // a custom callback.
    this.model.on('change:data', this.data_changed, this);
  }

  data_changed () {
    let that = this;
    elementReady('#' + that.id).then(() => update_slpb(that));
  }
}


function update_slpb(view_) {
  let data = view_.model.get('data');
  let values = data.values;
  let progress = data.progress;
  let type_ = data.type || 'line';
  const $el = $('#' + view_.id);
  $el.css('width', progress + '%');
  $el.sparkline(values, { type: type_, height: '100%', width: '100%' });
}

