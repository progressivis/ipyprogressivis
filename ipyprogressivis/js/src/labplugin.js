const plugin = require('./index');
const base = require('@jupyter-widgets/base');

module.exports = {
  id: 'jupyter-progressivis',
  requires: [base.IJupyterWidgetRegistry],
  activate: (app, widgets) => {
    widgets.registerWidget({
      name: 'jupyter-progressivis',
      version: plugin.version,
      exports: plugin,
    });
  },
  autoStart: true,
};
