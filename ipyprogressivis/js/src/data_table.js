import * as widgets from '@jupyter-widgets/base';
import $ from 'jquery';
import DataTable from 'datatables.net';
import { elementReady } from './es6-element-ready';
import { new_id } from './base';

import 'datatables.net-dt/css/dataTables.dataTables.css';


export class DataTableModel extends widgets.DOMWidgetModel {
    defaults() {
      return {
        ...super.defaults(),
        _model_name : 'DataTableModel',
        _view_name : 'DataTableView',
        _model_module : 'jupyter-progressivis',
        _view_module : 'jupyter-progressivis',
        _model_module_version : '0.1.0',
        _view_module_version : '0.1.0',
	columns: '[a, b, c]',
        data: 'Hello DataTable!',
        page: '{0}',
        dt_id: 'aDtId',

      };
    }
  }

// Custom View. Renders the widget model.

export class DataTableView extends widgets.DOMWidgetView {
  // Defines how the widget gets rendered into the DOM
  render () {
    this.id = 'datatable_' + this.model.get('dt_id') + new_id();
    this.data_table = null;
    this.data_changed();
    // Observe changes in the value traitlet in Python, and define
    // a custom callback.
    this.model.on('change:data', this.data_changed, this);
  }

  data_changed () {
    //const dt_id = this.model.get('dt_id');
    const dt_id = this.id;
    if (document.getElementById(this.id) == null) {
      this.el.innerHTML = `<div style='overflow-x:auto;'><table id='${dt_id}' class='display' style='width:100%;'></div>`;
    }
    let that = this;
    elementReady('#' + this.id).then(() => update_table(that, dt_id));
  }
}


function change_page(wobj) {
  const info = wobj.data_table.page.info();
  info.draw = wobj.data_table.context[0].oAjaxData.draw + 1;
  wobj.model.set('page', info);
  wobj.touch();
}

function update_table(wobj, dt_id) {
  const cols = wobj.model.get('columns');
  if (cols == '') return;
  const columns_ = JSON.parse(cols);
  //console.log(data)
  if (!wobj.data_table) {
    const columns = columns_.map((c) => ({ sTitle: c.toString() }));
      wobj.data_table = new DataTable('#' + dt_id, {
        columns: columns,
        processing: true,
        serverSide: true,
        //"retrieve": true,
        ajax: (data_, callback) => {
          const js_data = JSON.parse(wobj.model.get('data'));
          if (js_data.draw < data_.draw) js_data.draw = data_.draw;
          callback(js_data);
        },
      })
      .on('page.dt', () => change_page(wobj))
      .on('length.dt', () => change_page(wobj));
  } else {
    wobj.data_table.ajax.reload(null, false);
  }
}
