from pollination.point_in_time_grid.entry import PointInTimeGridEntryPoint
from queenbee.recipe.dag import DAG


def test_point_in_time_grid():
    recipe = PointInTimeGridEntryPoint().queenbee
    assert recipe.name == 'point-in-time-grid-entry-point'
    assert isinstance(recipe, DAG)
