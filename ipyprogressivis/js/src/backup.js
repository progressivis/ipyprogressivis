import { DOMWidgetModel, DOMWidgetView } from "@jupyter-widgets/base";
import { new_id } from "./base";

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

function emptyStr(w, m) {
  return "";
}

export class BackupModel extends DOMWidgetModel {
  defaults() {
    return {
      ...super.defaults(),
      _model_name: "BackupModel",
      _view_name: "BackupView",
      _model_module: "jupyter-progressivis",
      _view_module: "jupyter-progressivis",
      _model_module_version: "0.1.0",
      _view_module_version: "0.1.0",
      value: "",
    };
  }
}
