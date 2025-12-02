# How to write a chaining widget (alias CW)?

In the following, we refer to the `ipyprogressivis.widgets.chaining.utils` module as `.utils`.

A `CW` is a class that inherits from `.utils.VBoxTyped` (recommended) or from `.utils.VBox`. To make it visible in the toolkit UI, it must be decorated with `@utils.chaining_widget(label=“A label”)`.

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

- `input_module: Module` representing the ProgressiVis module provided as input
- `input_slot: str` the name of the default slot
- `input_dtypes: dict[str, str]` or its alias `dtypes: dict[str, str]` provides, under certain conditions (see the initialize method below), a dictionary containing the column=>type mapping)

The scheduler is accessible via `input_module.scheduler`.

Tip: when input_module contains an instance of the `Sink` class, this means that the current module does not receive input data. This means that the current CW class is a data loader or generator. However, `input_module` is still useful for providing access to the scheduler.

### Constructor

The CW class constructor has several special features:

- It is not intended to be called in the user's code.
- Instantiation is performed solely by the toolkit, without arguments, so every `CW` inherits a constructor without arguments.
- The CW class can define its own constructor(mainly to define and initialize attributes, never to create sub-widgets), but it must also be without arguments and must call `super().__init__()`.


### The initialize() method

- The CW class must always define an `initialize()` method that will be called by the toolkit after instantiation. This method is responsible for creating the sub-widgets that make up the initial composition of the CW and assigning them to the attributes declared by the Typed class using the syntax described above. This composition may evolve later as a result of interactions with the user.

- If the list of columns (with or without their types) from the input table (provided by `self.input_module.output[self.input_slot]`) is needed to display it, for example, in a select list, then my method must be decorated with `@needs_dtypes`. The list of columns will be available in `self.dtypes` and `self.input_dtypes`.

### The different tasks of a `CW` class

A `CW` class can perform several tasks:

- enrich the existing dataflow with new modules
- add callbacks to modules or the scheduler
- produce a dynamic visualization, most often animated by a callback associated with a module or the scheduler
- create virtual (calculated) columns on the output tables

### Operating modes

The above roles can be performed in two modes:

- Interactive mode: most often triggered by the callback of a button often called `start_btn`
- Batch mode: triggered in replay mode of a scenario previously recorded via a method called `run` and decorated with `@runner`.

Since both modes trigger the same processing, their common core must be located in a dedicated method.

This method is usually called `init_modules()`, but this is not mandatory.

If the method creates new modules (which is usually the case), it must be decorated with @module_producer, which records useful information in case the widget and underlying modules are deleted.



For example, the CW AggregateW has the following method:


```python
    @modules_producer
    def init_modules(self, compute: AnyType) -> Aggregate:
    	...
```


which creates the Aggregate module and adds it to the dataflow.
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
        if is_recording():
            amend_last_record({'frozen': dict(compute=compute)})
	self.output_module = self.init_modules(compute)
```

and in the `run()` method, decorated by `@runner`:

```python
    @runner
    def run(self) -> AnyType:
        content = self.frozen_kw
        self.output_module = self.init_modules(**content)
        self.output_slot = "result"
```

We can see that in interactive mode, the various parameters collected in the sub-widgets are grouped together in a common JSON structure that is saved for possible future use via amend_last_record() before being used to call init_modules().


In batch mode (replay), the call parameters for init_modules() come from a previous recording and are retrieved via the frozen_kw attribute:

### CW typology

According to the tasks performed:

* module creator (and virtual columns) only
* visualization creator only
* module and visualization creator

According to their place in the topology

* CW node
* CW leaf


#### CW module creator

Some CWs are designed to enrich the data flow with new modules and/or create calculated columns on tables (GroupBy, Aggregate, Join, etc.). Their UI is usually disabled in replay mode, except with the “step by step” option. These CWs are intended to produce data for other CWs with which they are linked.

For this reason, their init_modules method should populate the output_module and output_slot attributes. Populating output_dtypes is optional because this information is often unknown at the time of creation and the toolkit is able to produce this information later, at the request of descendant CWs (using the @needs_modules decorator on the initialize method).

#### Progressive visualization creator CWs


Do not create modules, but probably a callback on the output module of the previous widget and a visualization that will be refreshed by this callback. The UI is divided into two parts:
1. Settings depending on the visualization capabilities may be partially frozen during replay.
2. Progressive data visualization.
Examples: “Dump table” and AnyVega.

#### CW module creator + data visualization

This is the case with Heatmap, which needs the Histogram2D module to produce a “density map” or heatmap visualization.


#### CW Node

This is a CW that accepts other CWs attached downstream. Logically, it necessarily creates at least one module. Its init_modules() method must provide the attributes.
