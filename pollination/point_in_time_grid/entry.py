from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.sky import GenSky, AdjustSkyForMetric
from pollination.honeybee_radiance.octree import CreateOctreeWithSky
from pollination.honeybee_radiance.translate import CreateRadianceFolderGrid

# input/output alias
from pollination.alias.inputs.model import hbjson_model_input
from pollination.alias.inputs.pit import point_in_time_metric_input
from pollination.alias.inputs.radiancepar import rad_par_daylight_factor_input
from pollination.alias.inputs.grid import sensor_count_input, grid_filter_input
from pollination.alias.outputs.daylight import point_in_time_grid_results

from ._raytracing import PointInTimeGridRayTracing


@dataclass
class PointInTimeGridEntryPoint(DAG):
    """Point-in-time grid-based entry point."""

    # inputs
    model = Inputs.file(
        description='A Honeybee model in HBJSON file format.',
        extensions=['json', 'hbjson'],
        alias=hbjson_model_input
    )

    sky = Inputs.str(
        description='Sky string for any type of sky (cie, climate-based, irradiance, '
        'illuminance). This can be a minimal representation of the sky through '
        'altitude and azimuth (eg. "cie -alt 71.6 -az 185.2 -type 0"). Or it can be '
        'a detailed specification of time and location (eg. "climate-based 21 Jun 12:00 '
        '-lat 41.78 -lon -87.75 -tz 5 -dni 800 -dhi 120"). Both the altitude and '
        'azimuth must be specified for the minimal representation to be used. See the '
        'honeybee-radiance sky CLI group for a full list of options '
        '(https://www.ladybug.tools/honeybee-radiance/docs/cli/sky.html).'
    )

    metric = Inputs.str(
        description='Text for the type of metric to be output from the calculation. '
        'Choose from: illuminance, irradiance, luminance, radiance.',
        default='illuminance', alias=point_in_time_metric_input,
        spec={'type': 'string',
              'enum': ['illuminance', 'irradiance', 'luminance', 'radiance']},
    )

    grid_filter = Inputs.str(
        description='Text for a grid identifer or a pattern to filter the sensor grids '
        'of the model that are simulated. For instance, first_floor_* will simulate '
        'only the sensor grids that have an identifier that starts with '
        'first_floor_. By default, all grids in the model will be simulated.',
        default='*',
        alias=grid_filter_input
    )

    sensor_count = Inputs.int(
        default=200,
        description='The maximum number of grid points per parallel execution',
        spec={'type': 'integer', 'minimum': 1},
        alias=sensor_count_input
    )

    radiance_parameters = Inputs.str(
        description='The radiance parameters for ray tracing',
        default='-ab 2 -aa 0.1 -ad 2048 -ar 64',
        alias=rad_par_daylight_factor_input
    )

    @task(template=GenSky)
    def generate_sky(self, sky_string=sky):
        return [
            {
                'from': GenSky()._outputs.sky,
                'to': 'resources/weather.sky'
            }
        ]

    @task(
        template=AdjustSkyForMetric,
        needs=[generate_sky]
    )
    def adjust_sky(self, sky=generate_sky._outputs.sky, metric=metric):
        return [
            {
                'from': AdjustSkyForMetric()._outputs.adjusted_sky,
                'to': 'resources/weather.sky'
            }
        ]

    @task(template=CreateRadianceFolderGrid)
    def create_rad_folder(
        self, input_model=model, grid_filter=grid_filter
            ):
        """Translate the input model to a radiance folder."""
        return [
            {'from': CreateRadianceFolderGrid()._outputs.model_folder, 'to': 'model'},
            {
                'from': CreateRadianceFolderGrid()._outputs.model_sensor_grids_file,
                'to': 'results/grids_info.json'
            },
            {
                'from': CreateRadianceFolderGrid()._outputs.sensor_grids,
                'description': 'Sensor grids information.'
            }
        ]

    @task(
        template=CreateOctreeWithSky, needs=[adjust_sky, create_rad_folder]
    )
    def create_octree(
        self, model=create_rad_folder._outputs.model_folder,
        sky=adjust_sky._outputs.adjusted_sky
    ):
        """Create octree from radiance folder and sky."""
        return [
            {
                'from': CreateOctreeWithSky()._outputs.scene_file,
                'to': 'resources/scene.oct'
            }
        ]

    @task(
        template=PointInTimeGridRayTracing,
        needs=[create_rad_folder, create_octree],
        loop=create_rad_folder._outputs.sensor_grids,
        sub_folder='initial_results/{{item.name}}',  # create a subfolder for each grid
        sub_paths={'sensor_grid': 'grid/{{item.full_id}}.pts'}  # subpath for sensor_grid
    )
    def point_in_time_grid_ray_tracing(
        self,
        sensor_count=sensor_count,
        radiance_parameters=radiance_parameters,
        metric=metric,
        octree_file=create_octree._outputs.scene_file,
        grid_name='{{item.full_id}}',
        sensor_grid=create_rad_folder._outputs.model_folder
    ):
        # this task doesn't return a file for each loop.
        # instead we access the results folder directly as an output
        pass

    results = Outputs.folder(
        source='results', description='Folder with raw result files (.res) that contain '
        'numerical values for each sensor. Values are in standard SI units of the input '
        'metric (lux, W/m2, cd/m2, W/m2-sr).', alias=point_in_time_grid_results
    )
