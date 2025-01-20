import { DOMWidgetModel, DOMWidgetView } from "@jupyter-widgets/base";

export class CellOutModel extends DOMWidgetModel {
  defaults() {
    return {
      ...super.defaults(),
      _model_name: "CellOutModel",
      _view_name: "CellOutView",
      _model_module: "jupyter-progressivis",
      _view_module: "jupyter-progressivis",
      _model_module_version: "0.1.0",
      _view_module_version: "0.1.0",
      tag: "",
    };
  }
}
