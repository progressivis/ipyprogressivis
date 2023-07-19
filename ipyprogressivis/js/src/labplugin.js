import * as pwidgets from './index';
import {IJupyterWidgetRegistry} from '@jupyter-widgets/base';
import { INotebookTracker, NotebookActions } from '@jupyterlab/notebook';

export const progressivisPlugin = {
  id: 'jupyter-progressivis:plugin',
    requires: [IJupyterWidgetRegistry, INotebookTracker],
    activate: function(app, widgets, nbtracker) {
	window.ProgressiVis = { nbtracker: () => { return nbtracker; },
				nbactions: NotebookActions };
      widgets.registerWidget({
          name: 'jupyter-progressivis',
          version: '0.1.0',
          exports: pwidgets
      });
  },
  autoStart: true
};

export default progressivisPlugin;
