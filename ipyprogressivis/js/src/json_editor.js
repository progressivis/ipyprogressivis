"use strict";
import * as widgets from "@jupyter-widgets/base";
import { new_id } from "./base";
import { elementReady } from "./es6-element-ready";
import { createJSONEditor } from "vanilla-jsoneditor/standalone.js";

export class JsonEditorModel extends widgets.DOMWidgetModel {
  defaults() {
    return {
      ...super.defaults(),
      _model_name: "JsonEditorModel",
      _view_name: "JsonEditorView",
      _model_module: "jupyter-progressivis",
      _view_module: "jupyter-progressivis",
      _model_module_version: "0.1.0",
      _view_module_version: "0.1.0",
      data: "{}",
    };
  }
}

// Custom View. Renders the widget model.
export class JsonEditorView extends widgets.DOMWidgetView {
  // Defines how the widget gets rendered into the DOM
  render() {
    this.id = `json_editor_${new_id()}`;
    console.log("this id", this.id);
    this.el.innerHTML = "<div id='" + this.id + "'></div>";
    let content = {
      text: undefined,
      json: {},
    };
    elementReady(`#${this.id}`).then(() => {
      this.editor = createJSONEditor({
        target: document.getElementById(this.id),
        props: {
          content,
          onChange: (
            updatedContent,
            previousContent,
            { contentErrors, patchResult },
          ) => {
            // content is an object { json: unknown } | { text: string }
            previousContent, contentErrors, patchResult  // ignore
            if (updatedContent.text !== undefined) {
              // seems to be always true
              this.model.set("data", JSON.parse(updatedContent.text));
            } else {
              this.model.set("data", updatedContent.json);
            }
            this.touch();
          },
        },
      });
    });
    this.data_changed();
    this.model.on("change:data", this.data_changed, this);
  }
  data_changed() {
    const that = this;
    elementReady(`#${this.id}`).then(() => {
      this.editor.set({
        text: undefined,
        json: that.model.get("data"),
      });
    });
  }
}
