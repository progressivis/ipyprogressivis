'use strict';
import $ from 'jquery';
import 'jquery-sparkline';
import * as widgets from '@jupyter-widgets/base';
import _ from 'lodash';
import { new_id } from './base';
import { elementReady } from './es6-element-ready';

export class JsonHTMLModel extends widgets.DOMWidgetModel {
    defaults() {
      return {
        ...super.defaults(),
    _model_name: 'JsonHTMLModel',
    _view_name: 'JsonHTMLView',
    _model_module: 'jupyter-progressivis',
    _view_module: 'jupyter-progressivis',
    _model_module_version: '0.1.0',
    _view_module_version: '0.1.0',
    data: '{}',
    config: '{}',
      };
    }
}

// Custom View. Renders the widget model.
export class JsonHTMLView extends widgets.DOMWidgetView {
  // Defines how the widget gets rendered into the DOM
  render () {
    this.id = `json_html_${new_id()}`;
    this.el.innerHTML = "<div id='" + this.id + "'></div>";
    this.data_changed();
    // Observe changes in the value traitlet in Python, and define
    // a custom callback.
    this.model.on('change:config', this.data_changed, this);
    this.model.on('change:data', this.data_changed, this);
  }

  data_changed () {
    const that = this;
    elementReady(`#${this.id}`).then(() => layout_dict_entry(that));
  }
}


function layout_dict_entry(view_) {
  const ipyView = view_;
  const id = ipyView.id;
  let jq_id = `#${id}`;
  let data = view_.model.get('data');
  let config = view_.model.get('config');
  let order = config.order;
  let sparkl = config.sparkline || [];
  let procs = {};

  function makeSparkId(k) {
    return 'ps-spark_' + id + '_' + k;
  }

  function escapeHTML(s) {
    return $('<div>').text(s).html();
  }

  function layout_value(v) {
    let layout = '';
    if (v == null) return '';
    if (Array.isArray(v)) {
      if (v.length === 0) return '';
      for (const i of v) {
        if (layout.length != 0) layout += '<br>';
        layout += layout_value(i);
      }
      return layout;
    }
    if (typeof v === 'string' && v.startsWith('<div')) {
      return v;
    }
    if (typeof v === 'object') {
      const keys = Object.keys(v);
      if (keys.length == 0) return '';
      return layout_dict(v, keys.sort());
    }
    return escapeHTML(v.toString()); // escape
  }

  function sparkline_disp(v, k) {
    const SIZE = 15;
    let last = v[v.length - 1];
    if (last === undefined) return '';
    last = last.toFixed(0);
    last = last + '&nbsp;'.repeat(SIZE - last.length);
    return `<table>
    <tr><td>${last}</td><td><span class='ps-sparkline' id='${makeSparkId(
    k
  )}'>...</span></td></tr>
  </table>`;
  }

  function layout_dict(dict, order, value_func = {}) {
    let layout = '';

    if (!order) order = Object.keys(dict).sort();
    layout += '<dl class="dl-horizontal">';

    for (const k of order) {
      const v = dict[k];
      layout += ' <dt>' + k.toString() + ':</dt>';
      layout += ' <dd>';
      if (value_func[k]) {
        layout += value_func[k](v, k);
      } else {
        layout += layout_value(v);
      }
      layout += '</dd>';
    }

    layout += '</dl>';
    return layout;
  }


  for (const k of sparkl) {
    procs[k] = sparkline_disp;
  }
  $(jq_id).html(layout_dict(data, order, procs));
  elementReady('.ps-sparkline').then(() => {
    for (const k of sparkl) {
      $('#' + makeSparkId(k)).sparkline(data[k]);
    }
  });
}

