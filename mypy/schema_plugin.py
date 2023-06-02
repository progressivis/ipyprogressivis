from mypy.plugin import Plugin, ClassDefContext
from sqlmypy import add_var_to_class, CB  # type: ignore
from mypy.nodes import TypeInfo, NameExpr, StrExpr
from mypy.types import Instance
from typing import Any, Union, Optional

Expr = Union[NameExpr, StrExpr]


def get_full_name(name: str) -> Optional[str]:
    if "." in name:
        name = name.split(".")[-1]
    fullnames = dict(
        PTable="progressivis.table.table.PTable",
        BasePTable="progressivis.table.table_base.BasePTable",
        PTableSelectedView="progressivis.table.table_base.PTableSelectedView",
        PColumn="progressivis.table.table.PColumn",
        PDict="progressivis.utils.psdict.PDict",
        PIntSet="progressivis.core.pintset.PIntSet"
    )
    return fullnames.get(name)


def get_content(obj: Expr) -> str:
    if isinstance(obj, NameExpr):
        return obj.name
    return obj.value


def decl_schema_hook(ctx: ClassDefContext) -> None:
    fullname = ctx.cls.fullname
    chunks = fullname.split(".")
    assert chunks[-1] == "Schema"
    for sup in ctx.cls.info.mro:
        if sup.fullname.endswith(".utils.SchemaBase"):
            break
    else:
        print(f"{fullname} is not a Schema Box")
        return
    upper_fullname = ".".join(chunks[:-1])

    sym = ctx.api.lookup_fully_qualified_or_none(upper_fullname)
    if sym is None:
        print(f"{upper_fullname} not found")
        return
    if not isinstance(ctx.cls.info, TypeInfo):
        print(f"{ctx.cls.info} is not a TypeInfo")
        return
    typ = Instance(ctx.cls.info, [])
    add_var_to_class("child", typ, sym.node)
    add_var_to_class("c_", typ, sym.node)


class ModulePlugin(Plugin):
    def get_customize_class_mro_hook(self, fullname: str) -> CB[ClassDefContext]:
        if fullname.endswith(".Schema"):
            return decl_schema_hook


def plugin(version: str) -> Any:
    return ModulePlugin
