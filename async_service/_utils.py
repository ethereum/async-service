import itertools
from typing import Collection, Iterable, Mapping, Set, TypeVar

TItem = TypeVar("TItem")


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
