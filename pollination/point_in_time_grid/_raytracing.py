"""Raytracing DAG for point-in-time grid-based."""

from pollination_dsl.dag import Inputs, DAG, task
from dataclasses import dataclass

from pollination.honeybee_radiance.grid import SplitGrid, MergeFiles
from pollination.honeybee_radiance.raytrace import RayTracingPointInTime


@dataclass
class PointInTimeGridRayTracing(DAG):
    """Point-in-time grid-based ray tracing."""
    # inputs

    sensor_count = Inputs.int(
        default=200,
        description='The maximum number of grid points per parallel execution',
        spec={'type': 'integer', 'minimum': 1}
    )

    radiance_parameters = Inputs.str(
        description='The radiance parameters for ray tracing',
        default='-ab 2 -aa 0.1 -ad 2048 -ar 64'
    )

    metric = Inputs.str(
        description='Text for the type of metric to be output from the calculation. '
        'Choose from: illuminance, irradiance, luminance, radiance.',
        default='illuminance', spec={'type': 'string',
        'enum': ['illuminance', 'irradiance', 'luminance', 'radiance']}
    )

    octree_file = Inputs.file(
        description='A Radiance octree file.',
        extensions=['oct']
    )

    grid_name = Inputs.str(
        description='Sensor grid file name. This is useful to rename the final result '
        'file to {grid_name}.res'
    )

    sensor_grid = Inputs.file(
        description='Sensor grid file.',
        extensions=['pts']
    )

    @task(template=SplitGrid)
    def split_grid(self, sensor_count=sensor_count, input_grid=sensor_grid):
        return [
            {'from': SplitGrid()._outputs.grids_list},
            {'from': SplitGrid()._outputs.output_folder, 'to': 'sub_grids'}
        ]

    @task(
        template=RayTracingPointInTime,
        needs=[split_grid],
        loop=split_grid._outputs.grids_list,
        sub_folder='results',
        sub_paths={'grid': '{{item.path}}'}
    )
    def ray_tracing(
        self, radiance_parameters=radiance_parameters, metric=metric,
        grid=split_grid._outputs.output_folder, scene_file=octree_file
    ):
        return [
            {
                'from': RayTracingPointInTime()._outputs.result,
                'to': '{{item.name}}.res'
            }
        ]

    @task(
        template=MergeFiles, needs=[ray_tracing]
    )
    def merge_results(self, name=grid_name, extension='.res', folder='results'):
        return [
            {
                'from': MergeFiles()._outputs.result_file,
                'to': '../../results/{{self.name}}.res'
            }
        ]
