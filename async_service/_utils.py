import itertools
from typing import Any, Collection, Iterable, Mapping, Set, TypeVar

TItem = TypeVar("TItem")


def get_task_name(value: Any, explicit_name: str = None) -> str:
    # inline import to ensure `_utils` is always importable from the rest of
    # the module.
    from .abc import ManagerAPI, ServiceAPI  # noqa: F401

    if explicit_name is not None:
        # if an explicit name was provided, just return that.
        return explicit_name
    elif isinstance(value, ServiceAPI):
        # `Service` instance nameing rules:
        #
        # 1. __str__ **if** the class implements a custom __str__ method
        # 2. __repr__ **if** the class implements a custom __repr__ method
        # 3. The `Service` class name.
        value_cls = type(value)
        if value_cls.__str__ is not object.__str__:
            return str(value)
        if value_cls.__repr__ is not object.__repr__:
            return repr(value)
        else:
            return value.__class__.__name__
    else:
        try:
            # Prefer the name of the function if it has one
            return str(value.__name__)  # mypy doesn't know __name__ is a `str`
        except AttributeError:
            return repr(value)


def iter_dag(dag: Mapping[TItem, Collection[TItem]]) -> Iterable[TItem]:
    """
    Iterate over a DAG.

    The DAG is structured as a mapping of `parent -> children` such that each
    children is a set of elements that depend on `parent`.

    The iteration happens such that for any `parent` that is being yielded, all
    of the `children` for that parent will have been previosly yielded.
    """
    seen: Set[TItem] = set()
    all_items = set(dag.keys())
    all_children = set(itertools.chain.from_iterable(dag.values()))

    if not all_items.issuperset(all_children):
        # Ensure that
        raise ValueError("Parents not representative of all children")

    while seen != all_items:
        num_seen = len(seen)
        for item in dag.keys():
            if item in seen:
                # already yielded item
                continue
            elif seen.issuperset(dag[item]):
                # all children have been yielded so yield item
                yield item
                seen.add(item)
        if len(seen) == num_seen:
            raise Exception(
                "Unable to iterate over DAG. This can happen if the data "
                "structure is not actaully a dag and contains a cyclic "
                "dependency."
            )
