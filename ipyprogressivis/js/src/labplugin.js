import * as pwidgets from './index';
import * as cmds from './commands';
import {PageConfig} from "@jupyterlab/coreutils";
import {IJupyterWidgetRegistry, DOMWidgetView} from '@jupyter-widgets/base';
import { INotebookTracker, NotebookActions } from '@jupyterlab/notebook';
import {IFileBrowserFactory} from "@jupyterlab/filebrowser";
import {request} from "requests-helper";
export const progressivisPlugin = {
  id: 'jupyter-progressivis:plugin',
    requires: [IJupyterWidgetRegistry, INotebookTracker, IFileBrowserFactory],
    activate: function(app, widgets, nbtracker, browser) {
	window.ProgressiVis =  { frontend: () => {return app;}, nbtracker: () => { return nbtracker; },
				 nbactions: NotebookActions };
    request("get", `${PageConfig.getBaseUrl()}progressivis/template`).then((res) => {
	if (res.ok) {
	    const data = res.json();
	    app.commands.addCommand('progressivis:templ', {
		label: "New ProgressiBook",
		caption: "Initialize a notebook for progressivis chaining widgets usage",
		execute: () => { cmds.progressivisTemplate(app, res, data, browser);}
	    })
	}
    });
	app.commands.addCommand('progressivis:pass', {
	    label: "Progressivis pass",
	    caption: "Progressivis pass",
	    execute: () => {console.log("progressivis pass");}
	});
	app.commands.addCommand('progressivis:args', {
	    label: "Progressivis args",
	    caption: "Progressivis args",
	    execute: (args) => {console.log("progressivis args:", args);}
	});
	app.commands.addCommand('progressivis:cleanup', {
	    label: "Cleanup sidecars",
	    caption: "Cleanup orphan sidecars",
	    execute: () => {cmds.progressivisCleanup(app, nbtracker);}
	});
	app.commands.addCommand('progressivis:remove_tagged_cells', {
	    label: "Remove tagged cells",
	    caption: "Remove tagged cells",
	    execute: (tag) => {cmds.removeTaggedCells(nbtracker, tag);}
	});
	app.commands.addCommand('progressivis:create_stage_cells', {
	    label: "Create stage cells",
	    caption: "Create stage cells",
	    execute: (args) => {cmds.createStageCells(nbtracker,
						      args.tag, args.md, args.code);}
	});
	const TalkerView = class  extends DOMWidgetView {
	    render() {
		this.model.on('msg:custom', (ev)=>{
		    console.log("messs", ev);
		    let args = {...ev};
		    let cmd = args.cmd;
		    delete args.cmd;
		    console.log("exec cmd", cmd);
		    app.commands.execute(cmd, args);
		});
	    }
	}
	let pwidgets_ = {...pwidgets};
	pwidgets_.TalkerView = TalkerView;
      widgets.registerWidget({
          name: 'jupyter-progressivis',
          version: '0.1.0',
          exports: pwidgets_
      });

  }, // end activate
  autoStart: true
};

export default progressivisPlugin;
