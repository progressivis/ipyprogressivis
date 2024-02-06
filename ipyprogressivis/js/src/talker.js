import { DOMWidgetModel } from "@jupyter-widgets/base";

export class TalkerModel extends DOMWidgetModel {
  defaults() {
    return {
      ...super.defaults(),
      _model_name: "TalkerModel",
      _view_name: "TalkerView",
      _model_module: "jupyter-progressivis",
      _view_module: "jupyter-progressivis",
      _model_module_version: "0.1.0",
      _view_module_version: "0.1.0",
    };
  }
}
