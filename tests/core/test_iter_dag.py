import random

from hypothesis import given, settings, strategies as st

from async_service._utils import iter_dag


def _take(count, iterable):
    for _ in range(count):
        try:
            yield next(iterable)
        except StopIteration:
            break


def _install_dag_children(dag, elements_iter, parent, depth, total_elements, max_depth):
    if depth >= max_depth:
        return

    # We want a random-ish number of children that tends towards a small number
    # but is able to be reasonably large.  Capped at 32 to keep the number of
    # children from being too large.
    stdev = min(32, total_elements / max(1, (4 * depth)))
    num_children = int(abs(random.normalvariate(0, stdev)))
    children = tuple(_take(num_children, elements_iter))
    dag[parent].extend(children)
    for child in children:
        _install_dag_children(
            dag, elements_iter, child, depth + 1, total_elements, max_depth
        )


def _construct_dag(num_elements, max_depth):
    elements = tuple(range(num_elements))
    elements_iter = iter(elements)
    dag = {i: [] for i in elements}

    while True:
        try:
            parent = next(elements_iter)
        except StopIteration:
            break

        _install_dag_children(dag, elements_iter, parent, 0, num_elements, max_depth)
    return dag


@settings(max_examples=1000)
@given(
    num_elements=st.integers(min_value=1, max_value=4096),
    max_depth=st.integers(min_value=1, max_value=10),
)
def test_iter_dag(num_elements, max_depth):
    dag = _construct_dag(num_elements, max_depth)

    seen = set()
    for value in iter_dag(dag):
        # We shouldn't see the same value twice
        assert value not in seen
        children = dag[value]
        # All of the value's children should have already been seen
        assert seen.issuperset(children)
        seen.add(value)

    assert len(seen) == len(dag)
