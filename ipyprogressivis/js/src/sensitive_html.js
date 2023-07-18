import * as widgets from '@jupyter-widgets/base';
import _ from 'lodash';
import $ from 'jquery';
import { elementReady } from './es6-element-ready';
import { new_id } from './base';
require('sorttable');

export class SensitiveHTMLModel extends widgets.DOMWidgetModel {
    defaults() {
      return {
        ...super.defaults(),
    _model_name: 'SensitiveHTMLModel',
    _view_name: 'SensitiveHTMLView',
    _model_module: 'jupyter-progressivis',
    _view_module: 'jupyter-progressivis',
    _model_module_version: '0.1.0',
    _view_module_version: '0.1.0',
    html: '',
    value: '{0}',
    data: '{}',
    sensitive_css_class: 'aCssClass',
      };
    }
}

// Custom View. Renders the widget model.
export class SensitiveHTMLView extends widgets.DOMWidgetView {
  // Defines how the widget gets rendered into the DOM
  render () {
    this.id = `sensitive_${new_id()}`;
    this.html_changed();
    // Observe changes in the value traitlet in Python, and define
    // a custom callback.
    this.model.on('change:html', this.html_changed, this);
    this.model.on('change:data', this.data_changed, this);
  }

  html_changed () {
    this.el.innerHTML = this.model.get('html');
    const tables = $("table", this.el);
    if (tables.length != 0) {
      this.table = tables[0];
      this.table.id = this.id;
    }
    else {
      this.el.id = this.id;
    }
    let that = this;
    elementReady(`#${this.id}`).then(() => {
      that.update_cb();
      if (that.table) {
        sorttable.makeSortable(that.table);
      }
    });
  }

  data_changed () {
    let that = this;
    let sensitive_class = this.model.get('sensitive_css_class');
    elementReady(`#${that.id} .${sensitive_class}`).then(() =>
      that.update_data()
    );
  }
  update_cb () {
    let cssCls = this.model.get('sensitive_css_class');
    const that = this;
    $(`#${this.id} .${cssCls}`)
      .unbind('click')
      .click(function() {
        console.log('click on row:', this.id);
        that.model.set('value', this.id);
        that.touch();
      });
  }
  update_data() {
    let data = this.model.get('data');
    let k = null;
    for (k in data) {
      $('#' + k).html(data[k]);
    }
  }
}
