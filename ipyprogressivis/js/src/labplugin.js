import * as pwidgets from './index';
import {IJupyterWidgetRegistry} from '@jupyter-widgets/base';

export const progressivisPlugin = {
  id: 'jupyter-progressivis:plugin',
  requires: [IJupyterWidgetRegistry],
  activate: function(app, widgets) {
      widgets.registerWidget({
          name: 'jupyter-progressivis',
          version: '0.1.0',
          exports: pwidgets
      });
  },
  autoStart: true
};

export default progressivisPlugin;
