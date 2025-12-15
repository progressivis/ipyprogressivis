# How to write a chaining widget (alias CW)?


## The place of CW in ProgressiBook

When creating a ProgressiBook, you will find a simple interface that allows you to create a data loader. Currently, you can choose between a CSV, PARQUET, or Custom loader.

After creating and launching a loader (any one), the user will see a progress bar followed by a horizontal chaining bar containing:

* a "Next stage" dropdown list allowing you to choose a widget from a list
* an input field to give an alias to the element to be created
* a "Chain it" button to activate the chaining with another widget

Each new element consists of two parts:

* a specialized "guest" widget capable of performing the task selected in "Next stage". In the rest of this document, we will refer to it as chaining widget, or CW.
* a generic carrier widget, responsible for:
  * hosting the chaining widget (CW)
  * the chaining logic
  * displaying information common to all CWs

In concrete terms, the carrier widget is a vertical box (VBox) containing three elements:

* A header bar currently containing the red button for deleting the element
* The chaining widget (CW)
* A footer that can contain one or more horizontal bars:
  * a chaining bar
  * a progression bar
  * a quality bar
  * a dataviz management bar

The bars are displayed by default when they are technically relevant, but they can be disabled individually (via class decorators) when they are not semantically relevant.

## Focus on the CW

In the following, we refer to the `ipyprogressivis.widgets.chaining.utils` module as `.utils`.

A `CW` is a class that inherits from `.utils.VBoxTyped` (recommended) or from `.utils.VBox`. To make it visible in the toolkit UI, it must be decorated with `@utils.chaining_widget(label="A label")`.

If the superclass is `.utils.VBoxTyped`, then it must define a nested class called `Typed` that inherits from `.utils.TypedBase`. This class specifies the types of the first-level subwidgets of our CW via access attributes.

For example, the CW class AggregateW is defined as follows:

```python
@chaining_widget(label="Aggregate")
class AggregateW(VBoxTyped):
    class Typed(TypedBase):
        hidden_sel: ipw.SelectMultiple
        grid: ipw.GridBox
        start_btn: ipw.Button
```

and it will be visible in the interface with the label "Aggregate".

The attributes defined by the `Typed` class provide access to first-level sub-widgets with a dedicated syntax. For example, the "Start" button will be accessible on self via `self.child.start_btn` or via `self.c_.start_btn`, as desired.


### Inheritance

The `CW` class inherits three important instance attributes:

- `self.input_module: Module` representing the ProgressiVis module provided as input
- `self.input_slot: str` the name of the default slot
- `self.input_dtypes: dict[str, str]` or its alias `dtypes: dict[str, str]` provides, under certain conditions (see the initialize method below), a dictionary containing the column=>type mapping)

The scheduler is accessible via `self.input_module.scheduler`.

Tip: when `self.input_module` contains an instance of the `Sink` class, this means that the current module does not receive input data. Therefore the current CW class is a data loader or generator. However, `self.input_module` is still useful for providing access to the scheduler.

### Constructor

The CW class constructor has several special features:

- It is not intended to be called in the user's code.
- Instantiation is performed solely by the toolkit, without arguments, so every `CW` inherits a constructor without arguments.
- The CW class can define its own constructor(mainly to define and initialize attributes, never to create sub-widgets), but it must also be without arguments and must call `super().__init__()`.


### The initialize() method

- The CW class must always define an `initialize()` method that will be called by the toolkit after instantiation. This method is responsible for creating the sub-widgets that make up the initial composition of the CW and assigning them to the attributes declared by the Typed class using the syntax described above. This composition may evolve later as a result of interactions with the user.

- If the list of columns (with or without their types) from the input table (provided by `self.input_module.output[self.input_slot]`) is needed to display it, for example, in a select list, then `initialize()`  must be decorated with `@needs_dtypes`. This way, the list of columns will be available in `self.dtypes` and `self.input_dtypes`.

### The different tasks of a `CW` class

A `CW` class can perform several tasks:

- enrich the existing dataflow with new modules
- add callbacks to modules or the scheduler
- produce a dynamic visualization, most often animated by a callback associated with a module or the scheduler
- create virtual (calculated) columns on the output tables

### Operating modes

The above roles can be performed in two modes:

- Interactive mode: most often triggered by the callback of a button often called `start_btn`
- Replay mode: triggered in replay mode of a scenario previously recorded via a method called `run` and decorated with `@runner`.

Since both modes trigger the same processing, their common core must be located in a dedicated method.

This method is usually called `init_modules()`, but this is not mandatory.

If the method creates new modules (which is usually the case), it must be decorated with `@module_producer`, which records useful information in case the widget and underlying modules are deleted.



For example, the CW `AggregateW` has the following method:


```python
    @modules_producer
    def init_modules(self, compute: AnyType) -> Aggregate:
    	...
```


which creates the `Aggregate` module and adds it to the dataflow.
It is called in two places:

In the start_btn button callback for interactive mode:


```python
    @starter_callback
    def _start_btn_cb(self, btn: ipw.Button) -> None:
        compute = [
            ("" if col == ALL_COLS else col, fnc)
            for ((col, fnc), ck) in self.info_cbx.items()
            if fnc != "hide" and ck.value
        ]
	self.record = dict(compute=compute)
	self.output_module = self.init_modules(compute)
```

and in the `run()` method, decorated by `@runner` for replay mode:

```python
    @runner
    def run(self) -> AnyType:
        content = self.record
        self.output_module = self.init_modules(**content)
        self.output_slot = "result"
```

We can see that in interactive mode, the various parameters collected in the sub-widgets are grouped together in a common JSON structure that is saved for possible future use via `self.record` setting before being used to call init_modules().


In replay mode , the call parameters for `init_modules()` come from a previous recording and are retrieved via the same `self.record` attribute:

### CW typology

According to the tasks performed:

* module creator CW
* visualization creator CW
* both module+visualization creator CW

According to their place in the topology

* Node CW
* Leaf CW


#### CW module creator

Some CWs are designed to enrich the data flow with new modules and/or create calculated columns on tables (GroupBy, Aggregate, Join, etc.). Their UI is usually disabled in replay mode, except with the “step by step” option. These CWs are intended to produce data for other CWs with which they are linked.

For this reason, their `self.init_modules()` method should populate the `self.output_module` and `self.output_slot` attributes. Populating `self.output_dtypes` is optional because this information is often unknown at the time of creation and the toolkit is able to produce this information later, at the request of descendant CWs (using the `@needs_modules` decorator on the initialize method).

#### visualization creator CWs

Do not create modules, but probably a callback on the output module of the previous widget and a visualization that will be refreshed by this callback. The UI is divided into two parts:
1. Settings depending on the visualization capabilities may be partially frozen during replay.
2. Progressive data visualization.
Examples: `Dump table` and `AnyVega`.

#### CW module creator + data visualization

This is the case with Heatmap, which needs the Histogram2D module to produce a "density map" or heatmap visualization.


#### Node CW

This is a CW that accepts other CWs attached downstream. Logically, it necessarily creates at least one module. Its `init_modules()` method must provide the attributes.

#### Leaf CW

Unlike  the Node CW, the Leaf CW prevents the chaining of new widgets.

The typical case is that of a CW producing a visualization.

Since the chaining bar is displayed by default in the footer, a CW class must be decorated with `@is_leaf` to prevent the chaining bar from being displayed. For example:

```python
@is_leaf
# ...
@chaining_widget(label="Any Vega")
class AnyVegaW(VBoxTyped):
    ...
```

#### Other footer-related decorators

Additionnaly:

* `@no_progress_bar`  prevents the progress bar from being displayed when irrelevant
* `@no_quality_bar`  prevents the quality bar from being displayed when irrelevant

```python
@is_leaf
@no_progress_bar
@chaining_widget(label="Any Vega")
class AnyVegaW(VBoxTyped):
    ...
```

#### Implement a visualization creator CW

The basic way to integrate progressive visualization into a CW is described [here](https://progressivis.readthedocs.io/en/latest/userguide.html#communication-between-progressivis-and-the-notebook).

However, if you want to accompany the visualization with a horizontal bar integrated into the footer that allows you to manage the display frequency , proceed as follows:

1. Define a subclass of `utils.Coro`
2. Implement the asynchronous method `action()` on this subclass with this signature. For example:

```python
class AfterRun(Coro):
    async def action(self, m: Module, run_number: int) -> None:
    	...
```
3. Instantiate this subclass by passing the target module as an argument to the constructor. The module in question may or may not be produced by `init_modules()` (for example, `self.input_module` can be used even though it is not created by `init_modules()`)
4. Assign this instance to the `after_run` attribute of `CW`

In general, the right place to implement steps 2, 3, and 4 is the init_modules() method. For example:

```python
@is_leaf
@no_progress_bar
@chaining_widget(label="Heatmap")
class HeatmapW(VBoxTyped):
     ...
      @modules_producer
      def init_heatmap(self, ctx: dict[str, AnyType]) -> Heatmap:
         ...
         self.after_run = AfterRun(heatmap)
	 ...
```
#### The visual rendering of a ProgressiBook when loading and in replay mode

When loading a `progressibook`, the widgets are rendered using the standard mechanisms of `ipywidgets`.

Since the `progressibook` cells are created programmatically, the `progressibook` is not considered reliable by `Jupyterlab`, which only recognizes as reliable cells created and executed by human interactions.

For this reason, in order to benefit from the graphical visual rendering, the `progressibook` must be "signed" either in the `Jupyterlab` interface or with the command:

```sh
jupyter trust /path/to/my/file.ipynb
```

When running in replay mode, the rendering of widgets is static and similar to that obtained when opening the `progressibook`.

This is acceptable for widgets that only produce modules, but probably insufficient for widgets that produce progressive visualizations, which are by definition dynamic.

To address this type of situation, the CW must define a substitute, a “surrogate” widget, in other words, a widget that will be displayed instead of the standard rendering.

To do this, the CW should define a `provide_surrogate()` method that returns a widget that will be displayed instead of the default static rendering.

Most of the time, we will want to display the initial inactive widget (i.e., the settings part) along with the dynamic rendering of the progressive visualization.

```python
    def provide_surrogate(self, title: str) -> GuestWidget:
        disable_all(self)
        return self
```

**NB:** the `disable_all()` function is imported from `utils`