import { INotebookTracker, NotebookActions } from "@jupyterlab/notebook";

export function progressivisTemplate(app, res, data, browser) {
  const content = res.json();
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
              widget.model.fromString(data.payload);
              resolve(widget);
            });
          });
      });
  });
}

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
  notebook.widgets.forEach(function (cell) {
    console.log("meta", cell.model.sharedModel.metadata);
    console.log("ptag", cell.model.sharedModel.metadata.progressivis_tag);
    if (cell.model.sharedModel.metadata.progressivis_tag != undefined) {
      cell.model.sharedModel.setMetadata("deletable", true);
      cell.model.sharedModel.setMetadata("editable", true);
      toDelete.push(cell);
    }
  });
  console.log("toDelete", toDelete);
  for (let c of toDelete) {
    let i = notebook.widgets.findIndex((x) => x == c);
    notebook.model.sharedModel.deleteCell(i);
  }
}

export function removeTaggedCells(nbtracker, tag) {
  // only cells having progressivis_tag === tag are deleted
  var tag = tag.toString();
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
  backupCell.model.sharedModel.setMetadata("progressivis_root_backup", backupstring);
}

export function createStageCells(nbtracker, tag, md, code, rw, run) {
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  var tag = tag.toString();
  let i = -1;
  notebook.widgets.forEach(function (cell) {
    if (cell.model.metadata.progressivis_tag === tag) {
      cell.model.sharedModel.setMetadata("deletable", true);
      cell.model.sharedModel.setMetadata("editable", true);
      let i = notebook.widgets.findIndex((x) => x == cell);
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
  cell.model.sharedModel.setMetadata("editable", false);
  cell.model.sharedModel.setMetadata("deletable", false);
  cell.model.sharedModel.setMetadata("progressivis_tag", tag);
  notebook.model.sharedModel.insertCell(i + 1, {
    cell_type: "code",
    source: code,
  });
  notebook.activeCellIndex = i + 1;
  var cell = notebook.widgets[i + 1];
  if (run) {
    NotebookActions.run(notebook, crtWidget.sessionContext);
  }
  cell.model.sharedModel.setMetadata("trusted", true);
  cell.model.sharedModel.setMetadata("editable", rw);
  cell.model.sharedModel.setMetadata("deletable", false);
  cell.model.sharedModel.setMetadata("progressivis_tag", tag);
}

export function runCellAt(nbtracker, ix) {
  var crtWidget = nbtracker.currentWidget;
  var notebook = crtWidget.content;
  notebook.activeCellIndex = ix;
  NotebookActions.run(notebook, crtWidget.sessionContext);
}
