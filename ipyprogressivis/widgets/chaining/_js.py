jslab_func_remove = """
(function(){{
  var NotebookActions = ProgressiVis.nbactions;
  var crtWidget = ProgressiVis.nbtracker().currentWidget;
  var notebook = crtWidget.content;
  let toDelete = [];
  notebook.widgets.forEach( function(cell) {{
      if(cell.model.metadata.progressivis_tag === "{tag}"){{
        cell.model.sharedModel.setMetadata("deletable", true);
        cell.model.sharedModel.setMetadata("editable", true);
        toDelete.push(cell);
      }}
  }});
  for(let c of toDelete){{
     let i = notebook.widgets.findIndex((x)=> x == c)
     notebook.model.sharedModel.transact(() => {{
     notebook.model.sharedModel.deleteCell(i);
     }});
  }}
}})();
"""

# https://github.com/jupyterlab/jupyterlab/issues/5660

jslab_func_cleanup = """
(function(){{
  var NotebookActions = ProgressiVis.nbactions;
  var crtWidget = ProgressiVis.nbtracker().currentWidget;
  var notebook = crtWidget.content;
  let toDelete = [];
  notebook.widgets.forEach( function(cell) {{
      console.log("meta", cell.model.sharedModel.metadata);
      console.log("ptag", cell.model.sharedModel.metadata.progressivis_tag);
      if(cell.model.sharedModel.metadata.progressivis_tag != undefined){{
        cell.model.sharedModel.setMetadata("deletable", true);
        cell.model.sharedModel.setMetadata("editable", true);
        toDelete.push(cell);
      }}
  }});
  console.log("toDelete", toDelete);
  for(let c of toDelete){{
     let i = notebook.widgets.findIndex((x)=> x == c)
     //notebook.model.sharedModel.transact(() => {{
     notebook.model.sharedModel.deleteCell(i);
     //}});
  }}
}})();
"""

jslab_func_toc = """
(function(){{
  var NotebookActions = ProgressiVis.nbactions;
  var crtWidget = ProgressiVis.nbtracker().currentWidget;
  var notebook = crtWidget.content;
  let i = -1;
  notebook.widgets.forEach( function(cell) {{
      if(cell.model.metadata.progressivis_tag === "{tag}"){{
        cell.model.sharedModel.setMetadata("deletable", true);
        cell.model.sharedModel.setMetadata("editable", true);
        let i = notebook.widgets.findIndex((x)=> x == cell)
      }}
  }});
  if(i<0){{
   i = notebook.widgets.length;
  }} else {{
    i = i+1;
  }}
  notebook.model.sharedModel.insertCell(i, {{
        "cell_type": "markdown",
        source: "{md}"
      }});
  notebook.activeCellIndex = i;
  var cell = notebook.widgets[i];
  NotebookActions.run(notebook, crtWidget.sessionContext);
  cell.model.sharedModel.setMetadata("trusted", true);
  cell.model.sharedModel.setMetadata("editable", false);
  cell.model.sharedModel.setMetadata("deletable", false);
  cell.model.sharedModel.setMetadata("progressivis_tag", "{tag}");
  notebook.model.sharedModel.insertCell(i+1, {{
        "cell_type": "code",
        source: "{code}"
      }});
  notebook.activeCellIndex = i+1;
  var cell = notebook.widgets[i+1];
  NotebookActions.run(notebook, crtWidget.sessionContext);
  cell.model.sharedModel.setMetadata("trusted", true);
  cell.model.sharedModel.setMetadata("editable", false);
  cell.model.sharedModel.setMetadata("deletable", false);
  cell.model.sharedModel.setMetadata("progressivis_tag", "{tag}");
}})();
"""

jslab_func_cell_index = """
(function(){{
  var NotebookActions = ProgressiVis.nbactions;
  var crtWidget = ProgressiVis.nbtracker().currentWidget;
  var notebook = crtWidget.content;
  notebook.model.sharedModel.insertCell({index}, {{
        "cell_type": "{kind}",
        source: "{text}"
      }});
  notebook.activeCellIndex = {index};
  var cell = notebook.widgets[{index}];
  cell.model.sharedModel.setMetadata("trusted", true);
  cell.model.sharedModel.setMetadata("editable", false);
  cell.model.sharedModel.setMetadata("deletable", false);
  cell.model.sharedModel.setMetadata("progressivis_tag", "{tag}");
  NotebookActions.run(notebook, crtWidget.sessionContext);
  }})();
"""
