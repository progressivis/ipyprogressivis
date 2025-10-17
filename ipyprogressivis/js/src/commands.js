import { NotebookActions } from "@jupyterlab/notebook";
//import * as htmlToImage from "html-to-image";
import $ from "jquery";

export function progressivisTemplate(app, data, browser) {
  const { path } = browser.tracker.currentWidget.model;
  const ext = "ipynb";
  return new Promise((resolve) => {
    app.commands
      .execute("docmanager:new-untitled", {
        ext,
        path,
        type: "notebook",
      })
      .then((model) => {
        app.commands
          .execute("docmanager:open", {
            factory: "Notebook",
            path: model.path,
          })
          .then((widget) => {
            widget.isUntitled = true;
            widget.context.ready.then(() => {
              widget.model.fromJSON(data);
              resolve(widget);
            });
          });
      });
  });
}

window.$ = $; // for debug purposes

export function progressivisCleanup(app, nbtracker) {
  // cleanup sidecars
  let widgets = [];
  app.shell._rightHandler._items.forEach(function (item) {
    if (
      item.widget.title._label == "Widgets Output" ||
      item.widget.title._label == "Modules Output"
    ) {
      widgets.push(item.widget);
    }
  });
  widgets.forEach(function (wgt) {
    wgt.close();
  });
  // end cleanup sidecars
  // cleanup tagged cells
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  let toDelete = [];
  // NB: all cells having progressivis_tag (with any value) are deleted
  crtWidget.onloading_cells_content = {};
  notebook.widgets.forEach(function (cell) {
    console.log("meta", cell.model.sharedModel.metadata);
    console.log("ptag", cell.model.sharedModel.metadata.progressivis_tag);
    let pv_tag = cell.model.sharedModel.metadata.progressivis_tag;
    if (pv_tag != undefined) {
      cell.model.sharedModel.setMetadata("deletable", true);
      cell.model.sharedModel.setMetadata("editable", true);
      toDelete.push(cell);
      if (cell.model.sharedModel.cell_type === "code") {
        try {
          crtWidget.onloading_cells_content[pv_tag] = $(cell.node)
            .find($(".progressivis_guest_widget"))
            .first()[0].innerHTML;
        } catch {
          // useful only for older notebooks. TODO: remove ASAP
          try {
            crtWidget.onloading_cells_content[pv_tag] =
              "<img src='" +
              $(cell.node).find($("img")).first()[0].src +
              "'</img>";
          } catch {}
        }
      }
    }
  });
  for (let c of toDelete) {
    let i = notebook.widgets.findIndex((x) => x == c);
    notebook.model.sharedModel.deleteCell(i);
  }
}

export function removeTaggedCells(nbtracker, tag) {
  // only cells having progressivis_tag === tag are deleted
  tag = tag.toString();
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  let toDelete = [];
  notebook.widgets.forEach(function (cell) {
    if (cell.model.metadata.progressivis_tag === tag) {
      cell.model.sharedModel.setMetadata("deletable", true);
      cell.model.sharedModel.setMetadata("editable", true);
      toDelete.push(cell);
    }
  });
  for (let c of toDelete) {
    let i = notebook.widgets.findIndex((x) => x == c);
    notebook.model.sharedModel.transact(() => {
      notebook.model.sharedModel.deleteCell(i);
    });
  }
}

export function runAllSnippetCells(nbtracker) {
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  notebook.widgets.forEach(function (cell) {
    if (cell.model.sharedModel.source.startsWith("# progressivis-snippet")) {
      let i = notebook.widgets.findIndex((x) => x == cell);
      runCellAt(nbtracker, i);
    }
  });
}

export function unlockMarkdownCells(nbtracker) {
  // only cells having progressivis_tag became read-write
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  notebook.widgets.forEach(function (cell) {
    if (
      cell.model.metadata.progressivis_tag !== undefined &&
      cell.model.sharedModel.cell_type === "markdown"
    ) {
      cell.model.sharedModel.setMetadata("editable", true);
    }
  });
}

export function setCellMeta(nbtracker, i, key, value) {
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  var backupCell = notebook.widgets[i];
  console.log("set cell meta", backupCell.model.metadata, key, value);
  backupCell.model.sharedModel.setMetadata(key, value);
}

export function setBackup(nbtracker, backupstring) {
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  var backupCell = notebook.widgets[0];
  console.log("backup cell", backupCell.model.metadata, backupstring);
  backupCell.model.sharedModel.setMetadata("progressivis_backup", backupstring);
}

export function setRootBackup(nbtracker, backupstring) {
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  var backupCell = notebook.widgets[0];
  console.log("backup root markdown", backupCell.model.metadata, backupstring);
  backupCell.model.sharedModel.setMetadata(
    "progressivis_root_backup",
    backupstring,
  );
}

export function createStageCells(nbtracker, tag, tag_class, md, code, rw, run) {
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  tag = tag.toString();
  let i = -1;
  notebook.widgets.forEach(function (cell) {
    if (cell.model.metadata.progressivis_tag === tag) {
      cell.model.sharedModel.setMetadata("deletable", true);
      cell.model.sharedModel.setMetadata("editable", true);
      i = notebook.widgets.findIndex((x) => x == cell);
    }
  });
  if (i < 0) {
    i = notebook.widgets.length;
  } else {
    i = i + 1;
  }

  notebook.model.sharedModel.insertCell(i, {
    cell_type: "markdown",
    source: md,
  });
  notebook.activeCellIndex = i;
  var cell = notebook.widgets[i];
  NotebookActions.run(notebook, crtWidget.sessionContext);
  cell.model.sharedModel.setMetadata("trusted", true);
  cell.model.sharedModel.setMetadata("editable", true);
  cell.model.sharedModel.setMetadata("deletable", false);
  cell.model.sharedModel.setMetadata("progressivis_tag", tag);
  cell.model.sharedModel.setMetadata("progressivis_tag_class", tag_class);
  notebook.model.sharedModel.insertCell(i + 1, {
    cell_type: "code",
    source: code,
  });
  notebook.activeCellIndex = i + 1;
  cell = notebook.widgets[i + 1];
  if (run) {
    NotebookActions.run(notebook, crtWidget.sessionContext);
  }
  cell.model.sharedModel.setMetadata("trusted", true);
  cell.model.sharedModel.setMetadata("editable", rw);
  cell.model.sharedModel.setMetadata("deletable", false);
  cell.model.sharedModel.setMetadata("progressivis_tag", tag);
  cell.model.sharedModel.setMetadata("progressivis_tag_class", tag_class);
}

export function createCodeCell(nbtracker, i, code, run) {
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  //var i = notebook.widgets.length;
  notebook.model.sharedModel.insertCell(i, {
    cell_type: "code",
    source: code,
  });
  notebook.activeCellIndex = i;
  var cell = notebook.widgets[i];
  if (run) {
    NotebookActions.run(notebook, crtWidget.sessionContext);
  }
  cell.model.sharedModel.setMetadata("trusted", true);
}

export function runCellAt(nbtracker, ix) {
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  notebook.activeCellIndex = ix;
  NotebookActions.run(notebook, crtWidget.sessionContext);
}

/* eslint-disable no-unused-vars */
export function shotCellAtIndex(notebook, cell, i, tag, delay) {
  let prevOuts = notebook.model.metadata.progressivis_outs || {};
  // function fun() {
  //   let pvWidget = $(cell.outputArea.node)
  //     .find($(".progressivis_guest_widget"))
  //     .first()[0];
  //   if (pvWidget === undefined) return; // already an image
  //   htmlToImage.toPng(pvWidget).then((png) => {
  //     prevOuts[tag] = png;
  //     notebook.model.sharedModel.setMetadata("progressivis_outs", prevOuts);
  //   });
  // }
  // function fun2() {
  //   htmlToImage.toPng($("[id^='dag_widget_']")[0]).then((png) => {
  //     notebook.model.sharedModel.setMetadata("progressivis_dag_png", png);
  //   });
  // }
  //setTimeout(fun, delay);
  //setTimeout(fun2, delay);
}
/* eslint-enable no-unused-vars */

export function shotCell(nbtracker, tag, delay) {
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  let i = notebook.widgets.findIndex(
    (x) =>
      x.model.metadata.progressivis_tag === tag &&
      x.model.sharedModel.cell_type === "code",
  );
  var cell = notebook.widgets[i];
  shotCellAtIndex(notebook, cell, i, tag, delay);
}
