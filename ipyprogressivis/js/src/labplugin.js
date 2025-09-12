import * as pwidgets from "./index";
import * as cmds from "./commands";
import { PageConfig } from "@jupyterlab/coreutils";
import { IJupyterWidgetRegistry, DOMWidgetView } from "@jupyter-widgets/base";
import { INotebookTracker, NotebookActions } from "@jupyterlab/notebook";
import { IFileBrowserFactory } from "@jupyterlab/filebrowser";
import { request } from "requests-helper";
import $ from "jquery";

export const progressivisPlugin = {
  id: "jupyter-progressivis:plugin",
  requires: [IJupyterWidgetRegistry, INotebookTracker, IFileBrowserFactory],
  activate: function (app, widgets, nbtracker, browser) {
    window.ProgressiVis = {
      frontend: () => {
        return app;
      },
      nbtracker: () => {
        return nbtracker;
      },
      nbactions: NotebookActions,
    };
    request("get", `${PageConfig.getBaseUrl()}progressivis/template`).then(
      (res) => {
        if (res.ok) {
          const data = res.json();
          app.commands.addCommand("progressivis:templ", {
            label: "New ProgressiBook",
            caption:
              "Initialize a notebook for progressivis chaining widgets usage",
            execute: () => {
              cmds.progressivisTemplate(app, res, data, browser);
            },
          });
        }
      },
    );
    app.commands.addCommand("progressivis:pass", {
      label: "Progressivis pass",
      caption: "Progressivis pass",
      execute: () => {
        console.log("progressivis pass");
      },
    });
    app.commands.addCommand("progressivis:args", {
      label: "Progressivis args",
      caption: "Progressivis args",
      execute: (args) => {
        console.log("progressivis args:", args);
      },
    });
    app.commands.addCommand("progressivis:set_cell_meta", {
      label: "Progressivis set cell meta",
      caption: "Progressivis set cell meta",
      execute: (args) => {
        cmds.setCellMeta(nbtracker, args.i, args.key, args.value);
      },
    });
    app.commands.addCommand("progressivis:set_backup", {
      label: "Progressivis set backup",
      caption: "Progressivis set backup",
      execute: (args) => {
        cmds.setBackup(nbtracker, args.backup);
      },
    });
    app.commands.addCommand("progressivis:set_root_backup", {
      label: "Progressivis set root backup",
      caption: "Progressivis set root backup",
      execute: (args) => {
        cmds.setRootBackup(nbtracker, args.backup);
      },
    });
    app.commands.addCommand("progressivis:cleanup", {
      label: "Cleanup sidecars and tagged cells",
      caption: "Cleanup orphan sidecars and tagged cells",
      execute: () => {
        cmds.progressivisCleanup(app, nbtracker);
      },
    });
    app.commands.addCommand("progressivis:cleanup_and_run", {
      label: "Cleanup and run",
      caption: "Cleanup and run",
      execute: (args) => {
        if (nbtracker.currentWidget.progressivis_started !== undefined) {
          alert("ProgressiVis is already running!");
          return;
        }
        $(".progressivis-cleanup-and-run-btn").hide();
        nbtracker.currentWidget.progressivis_started = true;
        cmds.progressivisCleanup(app, nbtracker);
        //let indices = Number.isInteger(args.index) ? [args.index] : args.index;
        //indices.forEach(function (item) {cmds.runCellAt(nbtracker, item);});
        cmds.runCellAt(nbtracker, args.index);
        cmds.runAllSnippetCells(nbtracker);
      },
    });
    app.commands.addCommand("progressivis:run_cell_at", {
      label: "Run cell at index",
      caption: "Run cell at index",
      execute: (args) => {
        cmds.runCellAt(nbtracker, args.index);
      },
    });
    app.commands.addCommand("progressivis:run_all_snippet_cells", {
      label: "Run snippet cells",
      caption: "Run snippet cells",
      execute: () => {
        cmds.runAllSnippetCells(nbtracker);
      },
    });
    app.commands.addCommand("progressivis:remove_tagged_cells", {
      label: "Remove tagged cells",
      caption: "Remove tagged cells",
      execute: (args) => {
        cmds.removeTaggedCells(nbtracker, args.tag);
      },
    });
    app.commands.addCommand("progressivis:unlock_markdown_cells", {
      label: "Unlock markdown cells",
      caption: "Unlock markdown cells",
      execute: () => {
        cmds.unlockMarkdownCells(nbtracker);
      },
    });
    app.commands.addCommand("progressivis:create_stage_cells", {
      label: "Create stage cells",
      caption: "Create stage cells",
      execute: (args) => {
        cmds.createStageCells(
          nbtracker,
          args.tag,
          args.tag_class,

          args.md,
          args.code,
          args.rw,
          args.run,
        );
      },
    });
    app.commands.addCommand("progressivis:create_code_cell", {
      label: "Create code cell",
      caption: "Create code cell",
      execute: (args) => {
        cmds.createCodeCell(nbtracker, args.index, args.code, args.run);
      },
    });
    app.commands.addCommand("progressivis:shot_cell", {
      label: "Shot cell",
      caption: "Shot cell",
      execute: (args) => {
        cmds.shotCell(nbtracker, args.tag, args.delay);
      },
    });
    NotebookActions.executed.connect((_, args) => {
      // eslint-disable-next-line no-unused-vars
      let { cell, notebook, success } = args;
      let i = notebook.widgets.findIndex((x) => x == cell);
      if (cell.model.metadata.progressivis_tag === undefined) return;
      cmds.shotCellAtIndex(notebook, cell, i, 3000);
    });
    const TalkerView = class extends DOMWidgetView {
      render() {
        this.model.on("msg:custom", (ev) => {
          console.log("messg:", ev);
          let args = { ...ev };
          let cmd = args.cmd;
          delete args.cmd;
          console.log("exec cmd", cmd);
          app.commands.execute(cmd, args);
        });
      }
    };
    const BackupView = class extends DOMWidgetView {
      render() {
        this.model.on("msg:custom", this.load_backup, this);
        var crtWidget = nbtracker.currentWidget;
        var notebook = crtWidget.content;
        var backupCell = notebook.widgets[0];
        this.model.set(
          "value",
          backupCell.model.metadata.progressivis_backup || "",
        );
        //this.model.set("markdown", "<!-- -->");
        this.model.set(
          "root_markdown",
          backupCell.model.metadata.progressivis_root_backup || "",
        );
        this.touch();
      }

      load_backup(/* ev */) {
        var crtWidget = nbtracker.currentWidget;
        var notebook = crtWidget.content;
        var backupCell = notebook.widgets[0];
        console.log("backup cell", backupCell.model.metadata);
        this.model.set("value", backupCell.model.metadata.progressivis_backup);
        let markdown = [];
        notebook.widgets.forEach(function (cell) {
          if (cell.model.sharedModel.cell_type === "markdown") {
            markdown.push(cell.model.sharedModel.source);
          }
        });
        this.model.set("markdown", JSON.stringify(markdown));

        this.touch();
      }
    };

    const CellOutView = class extends DOMWidgetView {
      render() {
        this.el.innerHTML = "<div></div>";
        this.tag_changed();
        this.touch();
      }
      tag_changed() {
        let tag = this.model.get("tag");
        var crtWidget = nbtracker.currentWidget;
        var notebook = crtWidget.content;
        let imgSrc = notebook.model.metadata.progressivis_prev_outs[tag];
        this.el.innerHTML = "<img src='" + imgSrc + "'></img>";
        this.touch();
      }
    };

    let pwidgets_ = { ...pwidgets };
    pwidgets_.TalkerView = TalkerView;
    pwidgets_.BackupView = BackupView;
    pwidgets_.CellOutView = CellOutView;
    widgets.registerWidget({
      name: "jupyter-progressivis",
      version: "0.1.0",
      exports: pwidgets_,
    });
  }, // end activate
  autoStart: true,
};

export default progressivisPlugin;
