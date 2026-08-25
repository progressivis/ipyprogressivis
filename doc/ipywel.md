# [ipy]WEL - an [ipy]Widgets Expression Language

(ipywel-intro)=

## Introduction

Interfaces created with [ipywidgets](https://ipywidgets.readthedocs.io/en/stable/) typically have a tree structure (boxes and other nested containers that, at the deepest level, contain "leaf" widgets that perform other functions). [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) structures are theoretically possible (when a widget is shared by two or more containers) but they are not currently present in [ipyprogressivis](https://progressivis.readthedocs.io/en/latest/notebooks.html#chaining-widgets) (and it would be possible to handle them if necessary with minimal effort).

### Why not use [ipywidgets](https://ipywidgets.readthedocs.io/en/stable/) directly?

In the context of [ipyprogressivis](https://progressivis.readthedocs.io/en/latest/notebooks.html#chaining-widgets) (but not limited to it), there are several reasons:


#### Problem #1: Code Expressiveness

Let's consider the following example:

```python
import ipywidgets as ipw

ui = ipw.VBox([
    ipw.HBox([ipw.Label("Check it:"), ipw.Checkbox()]),
    ipw.Button(description="Click!")
])
display(ui)
```
If you want to access the value of the checkbox, the solution would be to write:

```python
my_checkbox = ui.children[0].children[1]
```

This isn't very readable, and it becomes even less so as the widget tree gets deeper.

We could name the different indices to make the code more readable:

```python
NAME_1 = 0
NAME_2 = 1
my_checkbox = ui.children[NAME_1].children[NAME_2]
```

But the expression always indicates the position in the tree rather than the identity of the target widget.

If the tree structure changes, the way to access the widget changes as well.


Another solution would be to create the checkbox separately before inserting it into the tree:

```python
import ipywidgets as ipw

my_checkbox = ipw.Checkbox()
ui = ipw.VBox([
    ipw.HBox([ipw.Label("Check it:"), my_checkbox]),
    ipw.Button(description="Click!")
])
display(ui)
```

This makes it easier to access the widget directly but does not improve the overall readability. Readability and maintainability deteriorate as the tree becomes more complex.

#### Problem #2: Static Typing

Currently, the [ipywidgets](https://ipywidgets.readthedocs.io/en/stable/) package is not statically typed, and the elements of a container ({py:class}`~ipywidgets.widgets.widget_box.VBox`, {py:class}`~ipywidgets.widgets.widget_box.HBox`, {py:class}`~ipywidgets:ipywidgets.widgets.widget_selectioncontainer.Tab`, etc.) are represented as items in a tuple contained within the `children` attribute, which does not facilitate typing even when using stubs (for example, we cannot statically determine the type of the object returned by the expression `ui.children[0]`).

#### Problem #3: Persistence

In [ipyprogressivis](https://progressivis.readthedocs.io/en/latest/notebooks.html#chaining-widgets), widget persistence is essential for replaying recorded scenarios with the exact state of each widget at the time of recording.

For standard Python objects, this could be done using the `pickle` package, but [ipywidgets](https://ipywidgets.readthedocs.io/en/stable/) are complex objects that do not support (at least as of August 2026) pickling.

#### The [ipy]WEL Solution

[ipy]WEL aims to solve the three problems mentioned above using a simple expression language. The basic principles are as follows:

* To keep track of all changes made to [ipywidgets](https://ipywidgets.readthedocs.io/en/stable/) (so they can be saved later), each widget is wrapped in a proxy object as soon as it is created. This proxy acts as an intermediary between the application and the actual widget for any operation that involves a change in the widget’s state.

* Each widget is created with its underlying proxy through a function that returns the proxy. By convention, the function name is the widget name in lowercase. In accordance with PEP-8, CapitalizedWords have been converted to lower_case_with_underscores (e.g., SelectMultiple => select_multiple), except for initials (e.g., VBox => vbox).

* For container widget functions, the following convention has been adopted:
  * positional arguments represent child widgets
  * named arguments represent the assignment of attributes with the same name (e.g., selected_index=3 for {py:func}`tab() <ipyprogressivis.ipywel.tab>`)
  * For leaf widget functions:
    * Only a single positional argument is allowed, representing the name displayed in the interface (the `value` attribute for {py:func}`label() <ipyprogressivis.ipywel.label>`, and `description` for the others). This is a shorthand; you are free to use named arguments.
    * Named arguments are used in the same way as for containers

For illustration, the previous example would be written as follows:

```python
from ipyprogressivis.ipywel import vbox, hbox, checkbox, label, button
ui = vbox(
    hbox(
        label("Check it:"),
        checkbox()
    ),
    button("Click")
)
display(ui.widget)
```

## Tutorial

### About widget functions

These are the functions used to create the widgets mentioned earlier. All of these functions follow the calling conventions described above. Each function constructs an ipywidget, wraps it in a {py:class}`~ipyprogressivis.ipywel.Proxy` object, connects the widget and the proxy to their respective parents, and returns the proxy.

[The current list](#wel-api) corresponds to a subset of [ipywidgets](https://ipywidgets.readthedocs.io/en/stable/), specifically, those used in [ipyprogressivis](https://progressivis.readthedocs.io/en/latest/notebooks.html#chaining-widgets). This list could be expanded later as needed.

All of these functions can be used directly based on the previous explanations, with the exception of {py:func}`anybox() <ipyprogressivis.ipywel.anybox>`, which has a unique feature: it does not create its own container widget but instead takes an existing widget ({py:class}`~ipywidgets.widgets.widget_box.VBox`, {py:class}`~ipywidgets.widgets.widget_box.HBox`) as its first argument, which it then wraps. It is used in all [c-widgets](https://ipywidgets.readthedocs.io/en/stable/) to connect the `c-widget` with its sub-widgets managed by [ipy]WEL.

### Setting attributes of an ipywidget

We saw earlier that each ipywidget is encapsulated in a proxy that keeps track of all state changes to the widget. To prevent any loss of information, all settings must go through the proxy. Currently, this is done using the {py:meth}`attrs() <ipyprogressivis.ipywel.Proxy.attrs>` method. It must be called exclusively with named arguments, where the names correspond to the properties of the ipywidget to be set.

The {py:meth}`attrs() <ipyprogressivis.ipywel.Proxy.attrs>` method, like all the settings methods of the {py:class}`~ipyprogressivis.ipywel.Proxy` class, is a fluent method as defined [here](https://en.wikipedia.org/wiki/Fluent_interface), designed to facilitate method chaining. Specifically, methods that are supposed to return `None` will return `self` when called.


```{eval-rst}

.. note::
       All assignments made using named arguments when the object is created (as seen earlier) can optionally be performed using chained calls to the :py:meth:`attrs() <ipyprogressivis.ipywel.Proxy.attrs>`  method.
```

 For example, these two expressions are equivalent:

```python
from ipyprogressivis.ipywel import vbox, hbox, dropdown, label, button

# Direct settings

vbox(
    hbox(
        label("Choose a color:"),
        dropdown(options=["Red", "White", "Blue"], value="Red")
    ),
    button("Click")
).widget
```

```python
from ipyprogressivis.ipywel import vbox, hbox, dropdown, label, button

# Settings via attrs()

vbox(
    hbox(
        label("Choose a color:"),
        dropdown().attrs(options=["Red", "White", "Blue"], value="Red")
    ),
    button("Click")
).widget
```


### Reading attributes of an ipywidget

The {py:meth}`~ipyprogressivis.ipywel.Proxy.widget` property provides access to the ipywidget wrapped by the proxy. It is used for any non-modifying access to the widget, such as in the two previous examples where {py:meth}`~ipyprogressivis.ipywel.Proxy.widget` is used to return the target widget for display.

```{eval-rst}

.. note::
       The returned object is not write-protected. It is the user's responsibility to ensure that the object is not modified directly. Any direct modification of the object will be ignored when saving and could cause further inconsistencies in the data set.
```

### Naming and Referencing Widgets-The Trio of {py:meth}`uid() <ipyprogressivis.ipywel.Proxy.uid>`, {py:meth}`lookup() <ipyprogressivis.ipywel.Proxy.lookup>`, and {py:meth}`~ipyprogressivis.ipywel.Proxy.that`


Some widgets in the tree (but not necessarilly all) need to be accessed individually for specific processing. We illustrated this in the initial example with the checkbox. Naming is done using the {py:meth}`uid() <ipyprogressivis.ipywel.Proxy.uid>` method, and access by name is done either using the {py:meth}`lookup() <ipyprogressivis.ipywel.Proxy.lookup>` method or the {py:meth}`~ipyprogressivis.ipywel.Proxy.that` property.

```{eval-rst}

.. note::
       * The names assigned must be unique within the tree.
       * Any proxy object can identify another proxy in its tree by its unique identifier (uid).

```

To revisit the previous example:

```python
from ipyprogressivis.ipywel import vbox, hbox, checkbox, label, button
ui = vbox(
    hbox(
        label("Check it:"),
        checkbox().uid("my_checkbox")
    ),
    button("Click").uid("my-button")
)
print("my_checkbox value is:", ui.lookup("my_checkbox").widget.value)
print("my_checkbox value is:", ui.that.my_checkbox.widget.value)
print("my-button description is:", ui.lookup("my-button").widget.description)
```

Access via {py:meth}`lookup() <ipyprogressivis.ipywel.Proxy.lookup>` is always possible. Access via {py:meth}`~ipyprogressivis.ipywel.Proxy.that` is a convenience syntax that is acceptable only when:

* The name (uid) is known statically
* **And** the name has a syntax valid for an attribute.

The following expressions are incorrect:

```python
ui.that.my-button.widget.description  # incorrect (raise KeyError: 'my')
x = "my_check" + "box"
ui.that.x.widget.value  # incorrect (raise KeyError: 'x')
```

On the other hand, we can write:

```python
ui.lookup("my-button").widget.description  # correct
x = "my_check" + "box"
ui.lookup(x).widget.value  # correct
ui.lookup("my_check" + "box").widget.value  # correct
x = "box"
ui.lookup(f"my_check{x}").widget.value  # correct
```

## Widget events

Event handling at the proxy level follows the [approach implemented by ipywidgets](https://ipywidgets.readthedocs.io/en/latest/examples/Widget%20Events.html) via the {py:meth}`observe() <ipyprogressivis.ipywel.Proxy.observe>` and {py:meth}`on_click() <ipyprogressivis.ipywel.Proxy.on_click>`  methods. The main difference is the inclusion of the proxy in the callback signatures. Here is the previous example, expanded to include callbacks associated with the checkbox and button widgets:

```python

from ipyprogressivis.ipywel import vbox, hbox, checkbox, label, button, Proxy
from typing import Any

def checkbox_cb(proxy: Proxy, changes: dict[str, Any]) -> None:
    proxy.lookup("my-button").attrs(disabled=not changes["new"])

def btn_clicked(proxy: Proxy, btn: Any) -> None:
    print(f"The {proxy.widget.description} button was clicked!")

vbox(
    hbox(
        label("Check it:"),
        checkbox().uid("my_checkbox").observe(checkbox_cb)
    ),
    button("Click", disabled=True).uid("my-button").on_click(btn_clicked)
).widget
```

## Saving and restoring a UI

### Saving

One can back up the data using the {py:meth}`dump() <ipyprogressivis.ipywel.Proxy.dump>` method, which returns `JSON` data that can easily be stored in a file, a database, or transmitted over a network.
The dump does not contain Python code, so it must be restored in an execution context that includes all the Python functions and classes used to create and operate the interface.

For example, the previous UI can be dumped and saved this way:

```python
from ipyprogressivis.ipywel import vbox, hbox, checkbox, label, button, Proxy
from typing import Any

def checkbox_cb(proxy: Proxy, changes: dict[str, Any]) -> None:
    proxy.lookup("my-button").attrs(disabled=not changes["new"])

def btn_clicked(proxy: Proxy, btn: Any) -> None:
    print(f"The {proxy.widget.description} button was clicked!")

ui = vbox(
    hbox(
        label("Check it:"),
        checkbox().uid("my_checkbox").observe(checkbox_cb)
    ),
    button("Click", disabled=True).uid("my-button").on_click(btn_clicked)
)
content = ui.dump()
content
```

Where `content` is:

```python
{'classname': 'VBox',
 'children': [{'classname': 'HBox',
   'children': [{'classname': 'Label', 'updates': {'value': 'Check it:'}},
    {'classname': 'Checkbox',
     'uid': 'my_checkbox',
     'observe': 'checkbox_cb'}]},
  {'classname': 'Button',
   'uid': 'my-button',
   'updates': {'disabled': True, 'description': 'Click'},
   'on_click': 'btn_clicked'}]}
```
This content is json-serializable and you can see that widget classes (VBox, Checkbox etc.) and callback functions (checkbox_cb and btn_clicked) are only represented by their names and not by their codes.

You can save this content, for example in a file:

```python
import json
with open("/tmp/my_file.json", "w") as f:
    json.dump(content, f)
```

### Restoring

The restore is made via the {py:func}`restore() <ipyprogressivis.ipywel.restore>` function.

Usually one needs to restore the UI in another context or after restarting the notebook. In all cases the functions referenced in the dumped content must be present:

```python
from ipyprogressivis.ipywel import vbox, hbox, checkbox, label, button, Proxy, restore # import restore here
from typing import Any
import json

def checkbox_cb(proxy: Proxy, changes: dict[str, Any]) -> None:  # could be imported
    ...

def btn_clicked(proxy: Proxy, btn: Any) -> None:  # could be imported
    ...

with open("/tmp/my_file.json") as f:
    content = json.load(f)
ui = restore(content, ctx=globals())
ui.widget
```

In the previous snippet `ctx=globals()` argument bring the necessary code for `checkbox_cb()`, `btn_clicked()` function and all the other functions and classes imported from `ipywel`.


## Advanced concepts

### Backends

Sometimes you need to integrate pre-existing, non-graphical objects into a UI and be able to save and restore them in a way that is consistent and uniform with proxies.
This can be done using the {py:class}`~ipyprogressivis.ipywel.Backend` class as follows:

```python
from ipyprogressivis.ipywel import vbox, hbox, dropdown, label, button, Backend
import pandas as pd
from typing import Any

URL = "https://www.aviz.fr/nyc-taxi/nyc_weather_2015-clean.csv"

class MyCSVLoader:
    def __init__(self, url: str, **kw: Any) -> None:
        self.df = pd.read_csv(url, **kw)

csv_loader = Backend(MyCSVLoader, URL)


ui = vbox(
    hbox(
        label("Choose a column:"),
        dropdown(options=csv_loader().df.columns.tolist())
    ),
    button("Click")
).backend(csv_loader, name="loader")
content = ui.dump()
ui.widget
```

You can save this content just as you did before:

```python
import json
with open("/tmp/my_file.json", "w") as f:
    json.dump(content, f)
```

Then you can restore it this way:

```python
from ipyprogressivis.ipywel import vbox, hbox, dropdown, label, button, Backend, restore
import pandas as pd
from typing import Any

class MyCSVLoader:  # could be imported
    ...

with open("/tmp/my_file.json") as f:
    content = json.load(f)
ui = restore(content, ctx=globals())
ui.widget
```

### Using methods and lambda functions as widget callbacks

#### Using methods

Previously, we used functions as callbacks.
For various reasons, we use instead methods accessible through a class instance external to the UI, such as:

```python
from ipyprogressivis.ipywel import vbox, hbox, checkbox, label, button, Proxy
from typing import Any

class MyClass:
    def checkbox_cb(self, proxy: Proxy, changes: dict[str, Any]) -> None:
        proxy.lookup("my-button").attrs(disabled=not changes["new"])
    def btn_clicked(self, proxy: Proxy, btn: Any) -> None:
        print(f"The {proxy.widget.description} button was clicked!")

my_inst = MyClass()

ui = vbox(
    hbox(
        label("Check it:"),
        checkbox().uid("my_checkbox").observe(my_inst.checkbox_cb)
    ),
    button("Click", disabled=True).uid("my-button").on_click(my_inst.btn_clicked)
)
content = ui.dump()
ui.widget
```
Then save `content` as usual.

When restoring, you must provide an instance of the same class using the "obj" keyword. Currently, only one instance can be used:

```python
from ipyprogressivis.ipywel import vbox, hbox, checkbox, label, button, Proxy, restore
from typing import Any
import json

class MyClass:  # could be imported
    ...

my_inst = MyClass()

with open("/tmp/my_file.json") as f:
    content = json.load(f)
ui = restore(content, ctx=globals(), obj=my_inst)
ui.widget
```

#### Using lambda functions

Using lamba functions as widget calbacks is also possible:

```python
from ipyprogressivis.ipywel import vbox, hbox, checkbox, label, button, Proxy

def ui_func():
    return vbox(
            hbox(
                label("Check it:"),
                checkbox().uid("my_checkbox").observe(lambda proxy, changes: proxy.lookup("my-button").attrs(disabled=not changes["new"]))
            ),
            button("Click", disabled=True).uid("my-button").on_click(lambda proxy, btn: print(f"The {proxy.widget.description} button was clicked!"))
        )
ui = ui_func()
content = ui.dump()
ui.widget
```
The only requirement: every widget with a lambda callback must have an `uid`.

On the other hand, the restore is a little bit tricky ant will take place in two steps:

1. A blank (stateless) UI is build first. After that, widgets states are not restored yet but lambda function become available
2. The restore procedure take place using the lambda functions collected at the previous step

```python
from ipyprogressivis.ipywel import vbox, hbox, checkbox, label, button, restore

def ui_func():  # could be imported
    ...

stateless_ui = ui_func()
with open("/tmp/my_file.json") as f:
    content = json.load(f)
ui = restore(content, ctx=globals(), lambdas=stateless_ui._lambda)
ui.widget
```

### Wrapping custom widgets

To integrate custom ipywidgets, you need to write a wrapper function for your custom widget based on a similar example available in `ipywel.py`. For example:

```python
from ipyprogressivis.widgets.json_editor import JsonEditor

def json_editor(descr: str | None = None, **kw: AnyType) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    proxy = Proxy(JsonEditor())
    proxy.attrs(**kw, **kw2)
    return proxy

ui = vbox(
    json_editor(),
    button("Save JSON")
)
content = ui.dump()
ui.widget
```

When restoring the widget, you must provide the necessary information to the {py:func}`restore() <ipyprogressivis.ipywel.restore>` function via the `custom` parameter

```python
from ipyprogressivis.widgets.json_editor import JsonEditor

def json_editor(descr: str | None = None, **kw: AnyType) -> Proxy:
    ...

with open("/tmp/my_file.json") as f:
    content = json.load(f)
ui = restore(content, ctx=globals(), custom={"JsonEditor": json_editor})
ui.widget
```






Specific examples of how to use **[ipy]WEL** are available [here](https://github.com/progressivis/ipyprogressivis/tree/main/ipyprogressivis/widgets/chaining) (i.e. [ipyprogressivis chaining widgets](https://progressivis.readthedocs.io/en/latest/notebooks.html#chaining-widgets)). The advanced concepts discussed above are specifically used by the [CSV loader](https://github.com/progressivis/ipyprogressivis/blob/main/ipyprogressivis/widgets/chaining/csv_loader.py)


(wel-api)=

## [ipy]WEL API

### Container widget functions

```{eval-rst}
.. currentmodule:: ipyprogressivis.ipywel

.. autofunction:: box

.. autofunction:: vbox

.. autofunction:: hbox

.. autofunction:: gridbox

.. autofunction:: stack

.. autofunction:: tab

.. autofunction:: anybox

```

### Buttons functions

```{eval-rst}
.. currentmodule:: ipyprogressivis.ipywel

.. autofunction:: button

```

### Selection widget functions

```{eval-rst}

.. currentmodule:: ipyprogressivis.ipywel

.. autofunction:: radiobuttons

.. autofunction:: select

.. autofunction:: dropdown

.. autofunction:: select_multiple

```

### String valued widget functions

```{eval-rst}

.. currentmodule:: ipyprogressivis.ipywel

.. autofunction:: text

.. autofunction:: html

.. autofunction:: textarea

.. autofunction:: label


```

### Int valued widget functions

```{eval-rst}

.. autofunction:: int_text

.. autofunction:: bounded_int_text

.. autofunction:: int_slider
```

### Float valued widget functions

```{eval-rst}


.. autofunction:: bounded_float_text

```

### Other widget functions

```{eval-rst}

.. autofunction:: image

.. autofunction:: checkbox

.. autofunction:: file_upload

```

### Persistence management functions

```{eval-rst}

.. autofunction:: restore

.. autofunction:: restore_backends

```



## The `Proxy` class

```{eval-rst}

.. currentmodule:: ipyprogressivis.ipywel

.. autoclass:: Proxy
   :members:

.. :autoproperty: Proxy.widget

.. :autoproperty: Proxy.that
```

## The `Backend` class

```{eval-rst}

.. currentmodule:: ipyprogressivis.ipywel

.. autoclass:: Backend
   :members:
   :special-members: __call__
```
