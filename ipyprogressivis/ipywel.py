import ipywidgets as ipw
from typing import Any, Callable, cast

# Import template
"""
from ipyprogressivis.ipywel import (
    anybox,
    box,
    vbox,
    hbox,
    stack,
    tab,
    button,
    radiobuttons,
    text,
    int_text,
    bounded_int_text,
    html,
    textarea,
    checkbox,
    select,
    dropdown,
    select_multiple,
    label,
    file_upload,
    restore_backends,
    restore,
)
"""

def small_dict(**kw: Any) -> dict[Any, Any]:
    res = dict()
    for k, v in kw.items():
        if v:
            res[k] = v
    return res


def default_observer(
    proxy: "Proxy", custom: Callable[["Proxy", dict[str, Any]], Any] | None = None
) -> Callable[[dict[str, Any]], None]:
    def on_change_value(change: dict[str, Any]) -> None:
        proxy._updates[change["name"]] = change["new"]
        if custom is not None:
            custom(proxy, change)

    return on_change_value


def button_adapter(
    proxy: "Proxy", func: Callable[["Proxy", ipw.Button], None]
) -> Callable[[ipw.Button], None]:
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
    def __init__(self, func: Callable[..., None], *args: Any, **kw: Any) -> None:
        self._obj: Any | None = None
        self._func = func  # may be a constructor
        self._args = args
        self._kw = kw

    def __call__(self) -> Any:
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
    def __init__(self, widget: ipw.DOMWidget) -> None:
        self._widget: ipw.DOMWidget = widget
        self._updates: dict[str, Any] = dict()
        self._hints: dict[str, Any] = dict()
        self._titles: list[str] | tuple[str] = []
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
        self.that = _Lookup(self)
        self.hint = _Hint(self)
        # self.attr = _Attr(self)

    @property
    def widget(self) -> ipw.DOMWidget:
        return self._widget

    def attrs(self, **kw: Any) -> "Proxy":
        for k, v in kw.items():
            setattr(self._widget, k, v)
            if k == "layout":
                continue
            self._updates[k] = v
        return self

    def hints(self, **kw: Any) -> "Proxy":
        for k, v in kw.items():
            self._hints[k] = v
        return self

    def layout(self, **kw: Any) -> "Proxy":
        if not kw:
            return self
        self._layout = kw.copy()
        self.widget.layout = ipw.Layout(**kw)
        return self

    def titles(self, *args: Any) -> "Proxy":
        if not isinstance(self.widget, ipw.Tab):
            return self
        self._titles = args
        for i, t in enumerate(args):
            self.widget.set_title(i, t)
        return self

    def uid(self, name: str) -> "Proxy":
        self._uid = name
        self._rec_lambda_if()
        return self

    def get_root(self) -> "Proxy":
        if self._parent is None:
            return self
        return self._parent.get_root()

    def _rec_lambda_if(self) -> None:
        if self._uid is None or self._code is None:
            return  # nothing to do (yet)
        root = self.get_root()
        root._lambda[self._uid] = self._code

    def observe(self, func: Callable[..., Any]) -> "Proxy":
        self._observer = func.__name__
        if func.__name__ == "<lambda>":
            self._code = func
            self._rec_lambda_if()
        self._widget.observe(default_observer(self, func), names="value")
        return self

    def on_click(self, func: Callable[..., Any]) -> "Proxy":
        self._on_click = func.__name__
        if func.__name__ == "<lambda>":
            self._code = func
            self._rec_lambda_if()
        assert isinstance(self._widget, ipw.Button)
        self._widget.on_click(button_adapter(self, func))
        return self

    def lookup(self, name: str) -> "Proxy":
        root = self.get_root()
        if name not in root._registry:
            raise ValueError(f"{name} widget unknown")
        return root._registry[name]

    def backend(self, obj: Backend, *, name: str = "_") -> "Proxy":
        self._backends[name] = obj
        return self

    def back(self, name: str = "_") -> Any:
        root = self.get_root()
        assert name in root._backends
        return root._backends[name]()

    def proc(self, *args: Any) -> "Proxy":
        return self

    def dump(self) -> dict[str, Any]:
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
                updates=self._updates,
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
    for k, v in lower._registry.items():
        new_root._registry[k] = v
    lower._registry = dict()
    for k2, v2 in lower._backends.items():
        new_root._backends[k2] = v2
    lower._backends = dict()
    _container_impl(upper, lower)


def anybox(widget: ipw.Box, *args: Any, **kw: Any) -> Proxy:
    return _container(widget, *args, **kw)


def box(*args: Any, **kw: Any) -> Proxy:
    return _container(ipw.Box(), *args, **kw)


def vbox(*args: Any, **kw: Any) -> Proxy:
    return _container(ipw.VBox(), *args, **kw)


def hbox(*args: Any, **kw: Any) -> Proxy:
    return _container(ipw.HBox(), *args, **kw)


def stack(*args: Any, **kw: Any) -> Proxy:
    return _container(ipw.Stack(), *args, **kw)  # type: ignore


def tab(*args: Any, **kw: Any) -> Proxy:
    return _container(ipw.Tab(), *args, **kw)


def button(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    btn = ipw.Button()
    proxy = Proxy(btn)
    proxy.attrs(**kw, **kw2)
    return proxy


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


def text(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Text(), **kw, **kw2)


def int_text(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.IntText(), **kw, **kw2)


def bounded_int_text(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.BoundedIntText(), **kw, **kw2)


def html(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.HTML(), **kw, **kw2)


def textarea(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Textarea(), **kw, **kw2)


def checkbox(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Checkbox(), **kw, **kw2)


def select(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Select(), **kw, **kw2)


def dropdown(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Dropdown(), **kw, **kw2)


def select_multiple(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.SelectMultiple(), **kw, **kw2)


def _static_value_widget(widget: ipw.DOMWidget, **kw: Any) -> Proxy:
    proxy = Proxy(widget)
    proxy.attrs(**kw)
    return proxy


def label(*args: Any, **kw: Any) -> Proxy:
    return _static_value_widget(ipw.Label(*args), **kw)


def file_upload(descr: str | None = None, **kw: Any) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.FileUpload(), **kw, **kw2)


corresp = dict(
    Text=text,
    IntText=int_text,
    BoundedIntText=bounded_int_text,
    Button=button,
    Label=label,
    Select=select,
    HTML=html,
    RadioButtons=radiobuttons,
    Tab=tab,
    SelectMultiple=select_multiple,
    Stack=stack,
    Checkbox=checkbox,
    Textarea=textarea,
    VBox=vbox,
    HBox=hbox,
    Box=box,
    FileUpload=file_upload,
    Dropdown=dropdown,
)  # etc.


def restore_backends(bulk: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
    assert "backends" in bulk
    return {bn: Backend.deserialize(bk, ctx) for (bn, bk) in bulk["backends"].items()}


def restore(
    bulk: dict[str, Any],
    ctx: dict[str, Any],
    obj: Any | None = None,
    lambdas: dict[str, Any] | None = None,
) -> Proxy:
    def _restore_impl(bulk: dict[str, Any]) -> Proxy:
        assert isinstance(bulk, dict)
        classname = bulk["classname"]
        if classname in ctx:
            widget_cls = ctx[classname]
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
        widget_func = corresp[classname]
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
