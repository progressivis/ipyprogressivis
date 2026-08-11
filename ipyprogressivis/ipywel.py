import ipywidgets as ipw
from typing import Any, Callable, cast

_doc_building: bool | None = None

def copydoc(fnc: Callable[..., Any]) -> Callable[..., Any]:
    try:
        if fnc.__doc__ is None:
            return fnc
        fnc.__doc__ = fnc.__doc__.format(**fnc.__globals__)
    except Exception as e:
        print("cannot process copydoc", type(e), e.args)
    return fnc

DOC_HEADER = (
    """
    Creates an instance :py:class:`~ipywidgets:ipywidgets.widgets.widget_{widget}` and wraps it
    """
)

DOC_BOX_ARGS = (
    """

    Args:
        *args: children widgets
        **kw: settings for the wrapped widget properties

    Returns:
        the proxy containing the wrapped widget
    """
)

DOC_LEAF_ARGS = (
    """

    Args:
        descr: description property on the wrapped widget
        **kw: settings for the wrapped widget properties

    Returns:
        the proxy containing the wrapped widget
    """
)

SAME_ARGS = (
    """( same parameters as above )"""
)
_sphinx_list: list[tuple[str, str]] = []

FuncFunc = Callable[[Callable[..., "Proxy"]], Callable[..., "Proxy"]]
FuncProxy = Callable[..., "Proxy"]

corresp: dict[str, FuncProxy] = dict()

def _ipw_docstring(widget: str, wtype: str, verbose: bool = False) -> FuncFunc:
    """
    Implementation of the two meta-decorators below ``ipw_container`` and ``ipw_leaf``.
    It is responsible for automatically generating some of the docstrings for widget functions, as well as populating the ``corresp`` dictionary.

    Args:
        widget: take the form ``something.WidgetName`` (always contains a ``.``)
        wtype: ``box`` or ``leaf``
        verbose: when ``True`` it generate the full argument description (always the same for all leaf-widgets and container widgets)
    Returns:
        the required decorator
    """
    global _doc_building
    _, widget_base = widget.split(".")
    if _doc_building is None:
        import sys
        if "sphinx.config" in sys.modules:
            _doc_building = True
        else:
            _doc_building = False
    def void_decorator(fnc: FuncProxy) -> FuncProxy:
        # _sphinx_list.append((widget, fnc.__name__))  # uncomment only for using make_autodoc() and make_import()
        corresp[widget_base] = fnc
        return fnc
    if not _doc_building:
        return void_decorator
    template = DOC_HEADER
    if verbose:
        template += (DOC_BOX_ARGS if wtype == "box" else DOC_LEAF_ARGS)
    else:
        template += SAME_ARGS
    def decorator(fnc: FuncProxy) -> FuncProxy:
        #def decorator(fnc: T) -> T:
        try:
            init_doc = fnc.__doc__ or ""
            fnc.__doc__ = init_doc.strip() + "\n"+ template.format(widget=widget)
            corresp[widget_base] = fnc  # In principle, it's unnecessary, but it's safer and inexpensive
        except Exception as e:
            print("cannot process decorator", type(e), e.args)
        return fnc

    return decorator


def ipw_container(widget: str, verbose: bool = False) -> FuncFunc:
    """
    Partial instantiation of ``_ipw_docstring`` for container widgets
    """
    return _ipw_docstring(widget, "box", verbose)

def ipw_leaf(widget: str, verbose: bool = False) -> FuncFunc:
    """
    Partial instantiation of ``_ipw_docstring`` for leaf widgets
    """
    return _ipw_docstring(widget, "leaf", verbose)


def make_autodoc() -> str:
    """
    autodoc helper
    Requires uncommenting the 1st line in void_decorator() to work
    To be used with print(make_autodoc())
    """
    from io import StringIO
    from more_itertools import bucket  # type: ignore
    sio = StringIO()
    s = bucket(_sphinx_list, key=lambda x: x[0].split('.')[0])
    keys = list(s)
    for k in keys:
        sio.write(k)
        sio.write("\n")
        grp = s[k]
        for elt in grp:
            sio.write(f".. autofunction:: {elt[1]}\n")
    return sio.getvalue()

def make_import() -> str:
    """
    generate complete import to be included is
    user py files and adapted (delete the useless lines).
    The purpose is to avoid "blind" imports
    like "from ipyprogressivis.ipywel import *"
    To be used with print(make_import())
    """
    import io
    sio = io.StringIO("from ipyprogressivis.ipywel import (\n")
    sio.seek(0, io.SEEK_END)
    for _, fnc in sorted(_sphinx_list, key=lambda x: x[1]):
        sio.write(f"    {fnc},\n")
    sio.write(")\n")
    return sio.getvalue()

def small_dict(**kw: Any) -> dict[Any, Any]:
    """
    Removes from ``kw`` all superfluous void content

    Args:
        **kw: a json-serializale dict

    Returns:
        the cleanded up ``kw``
    """

    res = dict()
    for k, v in kw.items():
        if v:
            res[k] = v
    return res


def default_observer(
    proxy: "Proxy", custom: Callable[["Proxy", dict[str, Any]], Any] | None = None
) -> Callable[[dict[str, Any]], None]:
    """
    To manage persistence, a default observer is required to record the result of any
    event related to the widget in the proxy. This observer applies even when the widget
    does not require one. When the widget does require one (``custom`` variable), it is
    added to the default observer.

    Args:
        proxy: the proxy to be synchronized with the widget
        custom: the specialized callback (when it exists)
    Returns:
        The composed callback (default+custom). Remark : the returned callback contains the proxy
        object in its closure but not in its signature
    """
    def on_change_value(change: dict[str, Any]) -> None:
        proxy._updates[change["name"]] = change["new"]
        if custom is not None:
            custom(proxy, change)

    return on_change_value


def button_adapter(
    proxy: "Proxy", func: Callable[["Proxy", ipw.Button], None]
) -> Callable[[ipw.Button], None]:
    """
    As for observers, the button callback defined for the proxy contains the proxy
    argument which is not accepted by the ipywidgets buttton. This adapter returns
    a callback ipywidgets compatible containing the proxy in its closure.

    Args:
        proxy: the proxy to be synchronized with the widget
        func: the callback to be triggered by the button
    Returns:
        The "adapted" callback (i.e.  containing the proxy
        object in its closure but not in its signature
    """
    def on_click_func(btn: ipw.Button) -> None:
        func(proxy, btn)

    return on_click_func


class _Lookup:
    def __init__(self, proxy: "Proxy") -> None:  # TODO weakref
        self._proxy = proxy

    def __getattr__(self, attr: str) -> "Proxy":
        return self._proxy.lookup(attr)


class _Hint:
    def __init__(self, proxy: "Proxy") -> None:  # TODO weakref
        self._proxy = proxy

    def __getattr__(self, attr: str) -> Any:
        return self._proxy._hints[attr]


"""
class _Attr:
    def __init__(self, proxy: "Proxy") -> None:  # TODO weakref
        self._proxy = proxy

    def __getattr__(self, attr: str) -> str | int:
        return getattr(self._widget, attr)
"""


class Backend:
    """
    Backend is another kind of proxy object designed to hold a non-visual object instance
    that will be shared among the various widgets that make up an interface, in order to
    implement the interface's operational logic. We will refer to this hosted object as
    the "hosted backend" or simply "backend" when there is no ambiguity.

    **NB:** The hosted backend attributes must contain only json-serializable data.
    """
    def __init__(self, func: Callable[..., None], *args: Any, **kw: Any) -> None:
        """
        Initialize containing all info required to create (later)
        the guest object

        Args:
            func: the function object responsible for creating the hosted backend. It may be a constructor, but this is not required.
            args: positional args to be passed to ``func``
            kw: keyword  args to be passed to ``func``

        """
        self._obj: Any | None = None
        self._func = func  # may be a constructor
        self._args = args
        self._kw = kw

    def __call__(self) -> Any:
        """
        Creates the actual backend (hosted backend) by calling ``func(*args, **kw)``. It implements
        a "singleton" logic, i.e. it calls ``func`` only once then always returns
        the same host backend instance.

        Returns:
            the hosted backend
        """
        if self._obj is None:
            self._obj = self._func(*self._args, **self._kw)
        return self._obj

    def serialize(self) -> dict[Any, Any]:
        return small_dict(func=self._func.__name__, args=self._args, kw=self._kw)

    @staticmethod
    def deserialize(bulk: dict[Any, Any], ctx: dict[str, Any]) -> "Backend":
        fname = bulk["func"]
        return Backend(ctx[fname], *bulk.get("args", ()), **bulk.get("kw", dict()))


class Proxy:
    """
    The proxy acts as an intermediary between the application and the target widget. Its purpose is to:

    * enable access to the widget by name
    * log changes to enable replay

    """
    def __init__(self, widget: ipw.DOMWidget) -> None:
        """
        Proxy constructor

        .. warning::
           Do not call this class’s constructor directly, use the appropriate widget function instead.

        Args:
            widget: the wrapped widget
        """
        self._widget: ipw.DOMWidget = widget
        self._updates: dict[str, Any] = dict()
        self._hints: dict[str, Any] = dict()
        self._titles: list[str] | tuple[str, ...] = []
        self._parent: "Proxy" | None = None
        self._layout: dict[str, Any] = dict()
        self._registry: dict[str, "Proxy"] = dict()
        self._cache: dict[str, Any] = dict()
        self._children: list["Proxy"] | None = None
        self._uid: str | None = None
        self._is_container: bool = False
        self._observer: str | None = None
        self._on_click: str | None = None
        self._backends: dict[str, Backend] = dict()
        self._code: Callable[..., Any] | None = None
        self._lambda: dict[str, Callable[..., Any]] = dict()
        self._no_obs = False
        self._that = _Lookup(self)
        self.hint = _Hint(self)
        # self.attr = _Attr(self)

    @property
    def widget(self) -> ipw.DOMWidget:
        """
        Provides access to the ``ipywidget`` wrapped by the proxy. Used for any read-only access to the widget.

        .. warning::
           The returned object is not write-protected. It is the user's responsibility to ensure that the object
           is not modified directly if they want the changes to be persistent, as any direct modification to the
           object will be ignored when saving.
        """
        return self._widget

    @property
    def that(self) -> _Lookup:
        """
        Provides acces to a named proxy via its name used as an attribute
        """
        return self._that

    def attrs(self, **kw: Any) -> "Proxy":
        """
        This method is the only correct way to permanently modify the target widget.
        Any changes made directly to the target widget will be ignored by the backup.

        .. note::
               This method, as well as all setting methods on this class, are fluent (i.e., they return the ``self``). This allows for chained method calls.

        Args:
            **kw: key-value pairs corresponding to the properties of the widget to be updated  (such as ``value``, ``options``, etc.)  and their underlying values

        Returns:
            the ``self`` instance
        """
        for k, v in kw.items():
            setattr(self._widget, k, v)
            if k == "layout":
                continue
            self._updates[k] = v
        return self

    def hints(self, **kw: Any) -> "Proxy":
        """

        Setting named values as hints on a proxy object.
        Calling this method has no effect on the target widget but assigned hints are persistent.
        A hint assigned via this method (let's call it ``my_hint``) can be accessed using the syntax ``self.hint.my_hint``.

        Args:
            **kw: key-value pairs corresponding to hints assigned to the proxy

        Returns:
            the ``self`` instance
        """
        for k, v in kw.items():
            self._hints[k] = v
        return self

    def layout(self, **kw: Any) -> "Proxy":
        """
        Creates a :py:class:`~ipywidgets:ipywidgets.widgets.widget_layout.Layout`


        Args:
            **kw: key-value pairs corresponding to the layout properties

        Returns:
            the ``self`` instance
        """
        if not kw:
            return self
        self._layout = kw.copy()
        self.widget.layout = ipw.Layout(**kw)
        return self

    def titles(self, *args: str) -> "Proxy":
        """
        Assign titles (via ``set_title()`` method) on the target widget when it is a :py:class:`~ipywidgets:ipywidgets.widgets.widget_selectioncontainer.Tab`
        or an :py:class:`~ipywidgets:ipywidgets.widgets.widget_selectioncontainer.Accordion`. In other cases, it currently has no effect

        Args:
            **args: titles (must be ordered)
        Returns:
            the ``self`` instance
        """
        if not (isinstance(self.widget, ipw.Tab) or isinstance(self.widget, ipw.Accordion)):
            return self
        self._titles = tuple(args)
        for i, t in enumerate(args):
            self.widget.set_title(i, t)
        return self

    def uid(self, name: str) -> "Proxy":
        """
        This method sets an unique identifier.
        Naming is optional, but it is the only way to access a widget after it has been created.
        To find a named proxy (let’s call it ``my_named_proxy``) from another proxy (let’s call it ``another_proxy``),
        there are two ways to do this:

        * ``another_proxy.lookup("my_named_proxy")`` - always
        * ``another_proxy.that.my_named_proxy`` - if the name is known statically and if it is syntactically correct as an attribute name.

        Args:
            name: an unique name (uniqueness is not verified)
        Returns:
            the ``self`` instance
        """
        self._uid = name
        self._rec_lambda_if()
        return self

    def get_root(self) -> "Proxy":
        """
        This method provides access to the root of the current UI, i.e. the initial container.

        Returns:
            the ``root`` proxy
        """
        if self._parent is None:
            return self
        return self._parent.get_root()

    def _rec_lambda_if(self) -> None:
        if self._uid is None or self._code is None:
            return  # nothing to do (yet)
        root = self.get_root()
        root._lambda[self._uid] = self._code

    def observe(self, func: Callable[..., Any], names: str | list[str] = "value") -> "Proxy":
        """
        This is a wrapper for the `ipywidgets observe method <https://ipywidgets.readthedocs.io/en/stable/examples/Widget%20Events.html#traitlet-events>`_
        used to record a callback intended for handle changes on the target widget.
        The signature of the callback requested here (i.e. ``func``) is slightly different from
        `that of the ipywidget <https://ipywidgets.readthedocs.io/en/stable/examples/Widget%20Events.html#signatures>`_
        because it takes the current proxy as its first argument (i.e. ``handler(proxy: Proxy, change: dict[str, Any]) -> None``
        instead of ``handler(change: dict[str, Any]) -> None``

        Args:
            func: the callback (a.k.a. handler)
            names: the name of the widget properties for which the callback will apply
        Returns:
            the ``self`` instance
        """
        self._observer = func.__name__
        if func.__name__ == "<lambda>":
            self._code = func
            self._rec_lambda_if()
        self._widget.observe(default_observer(self, func), names=names)  # type: ignore
        return self

    def on_click(self, func: Callable[..., Any]) -> "Proxy":
        """
        This is a wrapper for the :py:class:`~ipywidgets:ipywidgets.widgets.widget_button.Button`
        `on_click() <https://ipywidgets.readthedocs.io/en/stable/examples/Widget%20Events.html#special-events>`_
        method used to record a callback to be called when the button is clicked.
        The signature of the callback requested here (i.e. ``func``) is slightly different from
        that of the :py:class:`~ipywidgets:ipywidgets.widgets.widget_button.Button` `callback <https://ipywidgets.readthedocs.io/en/stable/examples/Widget%20Events.html#example>`_
        because it takes the current proxy as its first argument (i.e. ``on_button_clicked(proxy: Proxy, btn: Button) -> None``
        instead of ``on_button_clicked(btn: Button) -> None``

        Args:
            func: the callback (a.k.a. handler)
        Returns:
            the ``self`` instance
        """
        self._on_click = func.__name__
        if func.__name__ == "<lambda>":
            self._code = func
            self._rec_lambda_if()
        assert isinstance(self._widget, ipw.Button)
        self._widget.on_click(button_adapter(self, func))
        return self

    def lookup(self, name: str) -> "Proxy":
        """
        Allows to find a proxy by its name.
        This assumes that the X has been previously named using the ``uid()`` method.

        Args:
            name: the name of the proxy to be found
        Returns:
            the found proxy
        """
        root = self.get_root()
        # if name not in root._registry:
        #     raise ValueError(f"{name} widget unknown")
        return root._registry[name]

    def backend(self, obj: Backend, *, name: str = "_") -> "Proxy":
        """
        Allows to register a Backend. The name is optional when an unique backend is used.

        .. note::
           Always call this method on the root proxy of your UI

        Args:
            obj: the backend to be registered
            name: the name of the backend (optional when the backend is unique)
        Returns:
            the ``self`` instance
        """
        self._backends[name] = obj
        return self

    def back(self, name: str = "_") -> Any:
        """
        Allows to instanciate a backend after restoring the backup that contains it's dumped image

        .. note::
           Always call this method on the root proxy of your UI

        Args:
            name: the name of the backend (optional when the backend is unique)
        Returns:
            the instanciated backend
        """
        root = self.get_root()
        assert name in root._backends
        return root._backends[name]()

    def proc(self, *args: Any) -> "Proxy":
        """
        A method with no action of its own; its sole purpose is to trigger the instantiation of its arguments.
        A convenient method useful for writing callbacks using lambda functions.

        Args:
            args: arbitrary arguments
        Returns:
            the ``self`` instance
        """
        return self

    def dump(self) -> dict[str, Any]:
        """
        Generates a serialization of the current proxy and its descendants in the form of a JSON-serializable Python object.
        Using this object, which can be saved and restored, you can reconstruct the exact UI from which the dump was generated
        via the :py:func:`restore` function.

        Returns:
            the serialization of the ``self`` instance and its descendants
        """
        classname = type(self._widget).__name__
        if not self._is_container:
            callback = (
                dict(on_click=self._on_click) if self._on_click is not None else dict()
            )
            if self._observer is not None:
                callback["observe"] = self._observer
            return small_dict(
                classname=classname,
                uid=self._uid,
                updates=dict() if isinstance(self.widget, ipw.FileUpload) else self._updates,
                # backends={bn: bk.serialize() for (bn, bk) in self._backends.items()},
                hints=self._hints,
                layout=self._layout,
                **callback,
            )
        assert self._children is not None
        return small_dict(
            classname=classname,
            uid=self._uid,
            updates=self._updates,
            backends={bn: bk.serialize() for (bn, bk) in self._backends.items()},
            hints=self._hints,
            titles=self._titles,
            layout=self._layout,
            children=[child.dump() for child in self._children],
        )


def _container_impl(proxy: Proxy, *args: Any, **kw: Any) -> Proxy:
    proxy._is_container = True
    proxy._children = []
    children = []
    for child in args:
        children.append(child._widget)
        proxy._children.append(child)
        if child._uid is not None:
            proxy._registry[child._uid] = child
        for k, v in child._registry.items():
            proxy._registry[k] = v
        child._registry = dict()
        for k, v in child._lambda.items():
            proxy._lambda[k] = v
        child._lambda = dict()
        child._cache = dict()
        child._parent = proxy
    assert hasattr(proxy.widget, "children")
    proxy.widget.children = children
    proxy.attrs(**kw)
    return proxy


def _container(box: ipw.Box, *args: Any, **kw: Any) -> Proxy:
    proxy = Proxy(box)
    return _container_impl(proxy, *args, **kw)


def merge_trees(
    new_root: Proxy, upper: Proxy, lower: Proxy
) -> None:  # TODO: check all cases
    """

    """
    for k, v in lower._registry.items():
        new_root._registry[k] = v
    lower._registry = dict()
    for k2, v2 in lower._backends.items():
        new_root._backends[k2] = v2
    lower._backends = dict()
    _container_impl(upper, lower)



# Container widget functions

@ipw_container(widget="box.Box", verbose=True)
def box(*args: "Proxy", **kw: Any) -> Proxy:
    return _container(ipw.Box(), *args, **kw)

@ipw_container(widget="box.VBox")
def vbox(*args: "Proxy", **kw: Any) -> Proxy:
    return _container(ipw.VBox(), *args, **kw)

@ipw_container(widget="box.HBox")
def hbox(*args: "Proxy", **kw: Any) -> Proxy:
    return _container(ipw.HBox(), *args, **kw)

@ipw_container(widget="box.GridBox")
def gridbox(*args: "Proxy", **kw: Any) -> Proxy:
    return _container(ipw.GridBox(), *args, **kw)

@ipw_container(widget="selectioncontainer.Stack")
def stack(*args: "Proxy", **kw: Any) -> Proxy:
    return _container(ipw.Stack(), *args, **kw)  # type: ignore

@ipw_container(widget="selectioncontainer.Tab")
def tab(*args: "Proxy", **kw: Any) -> Proxy:
    return _container(ipw.Tab(), *args, **kw)

@ipw_container(widget="selectioncontainer.Accordion")
def accordion(*args: "Proxy", **kw: Any) -> Proxy:
    return _container(ipw.Accordion(), *args, **kw)

def anybox(widget: ipw.Box, *args: "Proxy", **kw: Any) -> Proxy:
    """
    Wrapper for an already created Box instance

    Args:
        widget: the existing box to be wrapped
        *args: children widgets
        **kw: settings for the wrapped widget properties

    Returns:
        the proxy containing the wrapped widget
    """
    return _container(widget, *args, **kw)

# Buttons widget functions

@ipw_leaf(widget="button.Button", verbose=True)
def button(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    btn = ipw.Button()
    proxy = Proxy(btn)
    proxy.attrs(**kw, **kw2)
    return proxy

# Selection widget functions

@ipw_leaf(widget="selection.RadioButtons", verbose=True)
def radiobuttons(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    btn = ipw.RadioButtons()
    proxy = Proxy(btn)
    proxy.attrs(**kw, **kw2)
    return proxy


def _value_widget(widget: ipw.DOMWidget, **kw: Any) -> Proxy:
    proxy = Proxy(widget)
    widget.observe(default_observer(proxy), names="value")
    proxy.attrs(**kw)
    return proxy

@ipw_leaf(widget="selection.Select")
def select(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Select(), **kw, **kw2)

@ipw_leaf(widget="selection.Dropdown")
def dropdown(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Dropdown(), **kw, **kw2)

@ipw_leaf(widget="selection.SelectMultiple")
def select_multiple(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.SelectMultiple(), **kw, **kw2)

# String valued widget functions

@ipw_leaf(widget="string.Text", verbose=True)
def text(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Text(), **kw, **kw2)

@ipw_leaf(widget="string.HTML")
def html(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.HTML(), **kw, **kw2)


@ipw_leaf(widget="string.Textarea")
def textarea(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Textarea(), **kw, **kw2)

def _static_value_widget(widget: ipw.DOMWidget, **kw: Any) -> Proxy:
    proxy = Proxy(widget)
    proxy.attrs(**kw)
    return proxy

@ipw_leaf(widget="string.Label")
def label(value: str = '', **kw: Any) -> Proxy:
    return _static_value_widget(ipw.Label(), value=value, **kw)

# Int valued widget functions

@ipw_leaf(widget="int.IntText", verbose=True)
def int_text(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.IntText(), **kw, **kw2)

@ipw_leaf(widget="int.BoundedIntText")
def bounded_int_text(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.BoundedIntText(), **kw, **kw2)

@ipw_leaf(widget="int.IntSlider")
def int_slider(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.IntSlider(), **kw, **kw2)

# Float valued widget functions

@ipw_leaf(widget="float.BoundedFloatText", verbose=True)
def bounded_float_text(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.BoundedFloatText(), **kw, **kw2)

# Other widget functions

@ipw_leaf(widget="media.Image", verbose=True)
def image(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    proxy = Proxy(ipw.Image())
    proxy.attrs(**kw, **kw2)
    return proxy


@ipw_leaf(widget="bool.Checkbox")
def checkbox(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Checkbox(), **kw, **kw2)

@ipw_leaf(widget="upload.FileUpload")
def file_upload(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.FileUpload(), **kw, **kw2)


def restore_backends(bulk: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Backend]:
    """
    Restores the backends previously dumped in ``bulk``. The context is a dictionary
    that must contain all the ``function-name->function`` pairs for all the functions
    needed to recreate the backends.

    Args:
        bulk: the json dump
        ctx: the context containing the ``function-name->function`` pairs for all the functions
             needed to recreate the backends
    Returns:
        a dict of pairs ``name->backend``
    """
    assert "backends" in bulk
    return {bn: Backend.deserialize(bk, ctx) for (bn, bk) in bulk["backends"].items()}


def restore(
    bulk: dict[str, Any],
    ctx: dict[str, Any],
    obj: Any | None = None,
    lambdas: dict[str, Any] | None = None,
    custom: dict[str, Any] = dict()
) -> Proxy:
    """
    This function restores an UI from a previous dump (``bulk`` variable)

    Args:
        bulk: the dump json content to be restored
        ctx: dictionary of the UI dependencies (classes, functions).
             When the context in which the UI is created is the same as the context
             in which it is restored (for example, both are defined in the same module),
             ``ctx = globals()`` is usually the solution.
        obj: When all or some of the callbacks used by the interface are methods, ``obj``
             refers to an instance of the class that defines those methods. Currently,
             only a single instance can be used.
        lambdas: In some cases, UI dependencies can be lambda functions. In this case,
                `lambdas` is a dictionary of ``uid=>lambda-function`` pairs.
                See the user guide for more information.
        custom: Similar to X, but intended for widget functions and proprietary widgets
                that are not part of the language.
    Returns:
        the proxy of the UI root
    """
    def _restore_impl(bulk: dict[str, Any]) -> Proxy:
        assert isinstance(bulk, dict)
        classname = bulk["classname"]
        if classname in ctx:
            widget_cls = ctx[classname]
        elif classname in custom:
            widget_cls = custom[classname]
        else:
            widget_cls = ipw.__dict__[classname]
        if "children" in bulk:
            contn = _container(
                widget_cls(),
                *[_restore_impl(child) for child in bulk.get("children", [])],
            )
            contn.attrs(**bulk.get("updates", dict()))
            contn.hints(**bulk.get("hints", dict()))
            contn._backends = {
                bn: Backend.deserialize(bk, ctx)
                for (bn, bk) in bulk.get("backends", dict()).items()
            }
            contn.layout(**bulk.get("layout", dict()))
            contn.titles(*bulk.get("titles", dict()))
            if uid := bulk.get("uid"):
                contn._uid = uid
            return contn
        # leaf case
        all_corresp = dict(**corresp, **custom)
        widget_func = all_corresp[classname]
        assert callable(widget_func)
        proxy = cast(Proxy, widget_func())
        proxy.attrs(**bulk.get("updates", dict()))
        proxy.hints(**bulk.get("hints", dict()))
        proxy.layout(**bulk.get("layout", dict()))
        # proxy._backends = {bn: Backend.deserialize(bk, ctx)
        #                   for (bn, bk) in proxy._backends.items()}

        if uid := bulk.get("uid"):
            proxy._uid = uid
        if fname := bulk.get("on_click"):
            if hasattr(obj, fname):
                func = getattr(obj, fname)
            elif fname == "<lambda>":
                assert lambdas is not None
                assert proxy._uid is not None
                func = lambdas[proxy._uid]
            else:
                func = ctx[fname]
            proxy.on_click(func)
        if fname := bulk.get("observe"):
            if hasattr(obj, fname):
                func = getattr(obj, fname)
            elif fname == "<lambda>":
                assert lambdas is not None
                assert proxy._uid is not None
                func = lambdas[proxy._uid]
            else:
                func = ctx[fname]
            proxy.observe(func)

        return proxy

    return _restore_impl(bulk)
