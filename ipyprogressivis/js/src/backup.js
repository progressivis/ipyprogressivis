import { DOMWidgetModel, DOMWidgetView } from '@jupyter-widgets/base';
import { new_id } from './base';

// See example.py for the kernel counterpart to this file.

// Custom Model. Custom widgets models must at least provide default values
// for model attributes, including
//
//  - `_view_name`
//  - `_view_module`
//  - `_view_module_version`
//
//  - `_model_name`
//  - `_model_module`
//  - `_model_module_version`
//
//  when different from the base class.

// When serialiazing the entire widget state for embedding, only values that
// differ from the defaults will be serialized.

function emptyStr(w, m){
    return ""
}

export class BackupWidgetModel extends DOMWidgetModel {
    defaults() {
      return {
        ...super.defaults(),
        _model_name : 'BackupWidgetModel',
        _view_name : 'BackupWidgetView',
        _model_module : 'jupyter-progressivis',
        _view_module : 'jupyter-progressivis',
        _model_module_version : '0.1.0',
        _view_module_version : '0.1.0',
          _value : "{}",
          _previous : "{}"
      };
    }

    static serializers = {
    ...DOMWidgetModel.serializers,
        _previous: {deserialize: emptyStr},
  }
  }

export class BackupWidgetView extends DOMWidgetView {
    firstDisplayFlag = true;
    render() {
	//this.id = 'pvis_backup_' + new_id();
	/*if(window.jupyterBackupWidgetStorage === undefined){
	    window.jupyterBackupWidgetStorage = {};
	}*/

	if (this.model.get('_previous')===""){ // _previous is "" => render called on first display
	    console.log("render backup I", this.id, this.model.get('_value'), "x",this.model.get('_previous'),"x");
	    window.jupyterBackupWidgetStorage = this.model.get('_value');
	} else {
	    console.log("render backup IIIIII", this.id, this.model.get('_value'));
	    this.model.set("_previous", window.jupyterBackupWidgetStorage);
	    this.touch();
	}
        //this.value_changed();

        // Observe and act on future changes to the value attribute
        this.model.on('change:_value', this.value_changed, this);
    }

    value_changed() {
	//this.model.set("_previous", window.jupyterBackupWidgetStorage);
	console.log("changed", window.jupyterBackupWidgetStorage);
	this.touch();
	//let firstDisplayFlag = window.jupyterBackupWidgetStorage === undefined;
	/*if(this.firstDisplayFlag){
	    window.jupyterBackupWidgetStorage = this.model.get('_value');
	    console.log("render backup2", this.firstDisplayFlag);
	    this.firstDisplayFlag = false;
	} else {
	    this.model.set("_previous", window.jupyterBackupWidgetStorage);
	    this.touch();
	    	console.log("render backup3", this.firstDisplayFlag);
	}*/
    }
}
