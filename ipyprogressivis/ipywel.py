import ipywidgets as ipw

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


LOOKUP = dict()


def small_dict(**kw):
    res = dict()
    for k, v in kw.items():
        if v:
            res[k] = v
    return res


def default_observer(proxy, custom=None):
    def on_change_value(change):
        proxy._updates[change["name"]] = change["new"]
        if custom is not None:
            custom(proxy, change)

    return on_change_value


def button_adapter(proxy, func):
    def on_click_func(btn):
        func(proxy, btn)

    return on_click_func


class _Lookup:
    def __init__(self, proxy):  # TODO weakref
        self._proxy = proxy

    def __getattr__(self, attr):
        return self._proxy.lookup(attr)


class _Hint:
    def __init__(self, proxy):  # TODO weakref
        self._proxy = proxy

    def __getattr__(self, attr):
        return self._proxy._hints[attr]


class _Attr:
    def __init__(self, proxy):  # TODO weakref
        self._proxy = proxy

    def __getattr__(self, attr):
        return getattr(self._widget, attr)


class Backend:
    def __init__(self, func, *args, **kw):
        self._obj = None
        self._func = func  # may be a constructor
        self._args = args
        self._kw = kw

    def __call__(self):
        if self._obj is None:
            self._obj = self._func(*self._args, **self._kw)
        return self._obj

    def serialize(self):
        return small_dict(func=self._func.__name__, args=self._args, kw=self._kw)

    @staticmethod
    def deserialize(bulk, ctx):
        fname = bulk["func"]
        return Backend(ctx[fname], *bulk.get("args", ()), **bulk.get("kw", dict()))


class Proxy:
    def __init__(self, widget):
        self._widget = widget
        self._updates = dict()
        self._hints = dict()
        self._titles = []
        self._parent = None
        self._layout = dict()
        self._registry = dict()
        self._cache = dict()
        self._children = None
        self._uid = None
        self._is_container = False
        self._observer = None
        self._on_click = None
        self._backends = dict()
        self._code = None
        self._lambda = dict()
        self.that = _Lookup(self)
        self.hint = _Hint(self)
        self.attr = _Attr(self)

    @property
    def widget(self):
        return self._widget

    def attrs(self, **kw):
        for k, v in kw.items():
            setattr(self._widget, k, v)
            if k == "layout":
                continue
            self._updates[k] = v
        return self

    def hints(self, **kw):
        for k, v in kw.items():
            self._hints[k] = v
        return self

    def layout(self, **kw):
        if not kw:
            return self
        self._layout = kw.copy()
        self.widget.layout = ipw.Layout(**kw)
        return self

    def titles(self, *args):
        if not isinstance(self.widget, ipw.Tab):
            return self
        self._titles = args
        for i, t in enumerate(args):
            self.widget.set_title(i, t)
        return self

    def uid(self, name):
        self._uid = name
        self._rec_lambda_if()
        return self

    def get_root(self):
        if self._parent is None:
            return self
        return self._parent.get_root()

    def _rec_lambda_if(self):
        if self._uid is None or self._code is None:
            return  # nothing to do (yet)
        root = self.get_root()
        root._lambda[self._uid] = self._code

    def observe(self, func):
        self._observer = func.__name__
        if func.__name__ == "<lambda>":
            self._code = func
            self._rec_lambda_if()
        self._widget.observe(default_observer(self, func), names="value")
        return self

    def on_click(self, func):
        self._on_click = func.__name__
        if func.__name__ == "<lambda>":
            self._code = func
            self._rec_lambda_if()
        self._widget.on_click(button_adapter(self, func))
        return self

    def lookup(self, name):
        root = self.get_root()
        if name not in root._registry:
            raise ValueError(f"{name} widget unknown")
        return root._registry[name]

    def backend(self, obj, *, name="_"):
        self._backends[name] = obj
        return self

    def back(self, name="_"):
        root = self.get_root()
        assert name in root._backends
        return root._backends[name]()

    def proc(self, *args):
        return self

    def dump(self):
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


def _container_impl(proxy, *args, **kw):
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
    proxy.widget.children = children
    proxy.attrs(**kw)
    return proxy


def _container(box, *args, **kw):
    proxy = Proxy(box)
    return _container_impl(proxy, *args, **kw)


def merge_trees(new_root, upper, lower):  # TODO: check all cases
    upper_root = upper.get_root()
    for k, v in lower._registry.items():
        new_root._registry[k] = v
    lower._registry = dict()
    for k, v in lower._backends.items():
        new_root._backends[k] = v
    lower._backends = dict()
    _container_impl(upper, lower)


def anybox(widget, *args, **kw):
    return _container(widget, *args, **kw)


def box(*args, **kw):
    return _container(ipw.Box(), *args, **kw)


def vbox(*args, **kw):
    return _container(ipw.VBox(), *args, **kw)


def hbox(*args, **kw):
    return _container(ipw.HBox(), *args, **kw)


def stack(*args, **kw):
    return _container(ipw.Stack(), *args, **kw)


def tab(*args, **kw):
    return _container(ipw.Tab(), *args, **kw)


def button(descr=None, **kw):
    kw2 = dict() if descr is None else dict(description=descr)
    btn = ipw.Button()
    proxy = Proxy(btn)
    proxy.attrs(**kw, **kw2)
    return proxy


def radiobuttons(descr=None, **kw):
    kw2 = dict() if descr is None else dict(description=descr)
    btn = ipw.RadioButtons()
    proxy = Proxy(btn)
    proxy.attrs(**kw, **kw2)
    return proxy


def _value_widget(widget, **kw):
    proxy = Proxy(widget)
    widget.observe(default_observer(proxy), names="value")
    proxy.attrs(**kw)
    return proxy


def text(descr=None, **kw):
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Text(), **kw, **kw2)


def int_text(descr=None, **kw):
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.IntText(), **kw, **kw2)


def bounded_int_text(descr=None, **kw):
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.BoundedIntText(), **kw, **kw2)


def html(descr=None, **kw):
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.HTML(), **kw, **kw2)


def textarea(descr=None, **kw):
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Textarea(), **kw, **kw2)


def checkbox(descr=None, **kw):
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Checkbox(), **kw, **kw2)


def select(descr=None, **kw):
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Select(), **kw, **kw2)


def dropdown(descr=None, **kw):
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.Dropdown(), **kw, **kw2)


def select_multiple(descr=None, **kw):
    kw2 = dict() if descr is None else dict(description=descr)
    return _value_widget(ipw.SelectMultiple(), **kw, **kw2)


def _static_value_widget(widget, **kw):
    proxy = Proxy(widget)
    proxy.attrs(**kw)
    return proxy


def label(*args, **kw):
    return _static_value_widget(ipw.Label(*args), **kw)


def file_upload(descr=None, **kw):
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


def restore_backends(bulk, ctx):
    assert "backends" in bulk
    return {bn: Backend.deserialize(bk, ctx) for (bn, bk) in bulk["backends"].items()}


def restore(bulk, ctx, obj=None, lambdas=None):
    def _restore_impl(bulk):
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
        proxy = widget_func()
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
                func = lambdas[proxy._uid]
            else:
                func = ctx[fname]
            proxy.on_click(func)
        if fname := bulk.get("observe"):
            if hasattr(obj, fname):
                func = getattr(obj, fname)
            elif fname == "<lambda>":
                func = lambdas[proxy._uid]
            else:
                func = ctx[fname]
            proxy.observe(func)

        return proxy

    return _restore_impl(bulk)
