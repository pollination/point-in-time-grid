from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.raytrace import RayTracingPointInTime

# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.pit import point_in_time_metric_input
from pollination.alias.inputs.radiancepar import rad_par_daylight_factor_input
from pollination.alias.inputs.grid import grid_filter_input, cpu_count
from pollination.alias.outputs.daylight import point_in_time_grid_results

from ._prepare_folder import PointInTimeGridPrepareFolder
from ._postprocess_results import PointInTimeGridPostProcess

@dataclass
class PointInTimeGridEntryPoint(DAG):
    """Point-in-time grid-based entry point."""

    # inputs
    model = Inputs.file(
        description='A Honeybee model in HBJSON file format.',
        extensions=['json', 'hbjson', 'pkl', 'hbpkl', 'zip'],
        alias=hbjson_model_grid_input
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
        description='Text for a grid identifier or a pattern to filter the sensor grids '
        'of the model that are simulated. For instance, first_floor_* will simulate '
        'only the sensor grids that have an identifier that starts with '
        'first_floor_. By default, all grids in the model will be simulated.',
        default='*',
        alias=grid_filter_input
    )

    cpu_count = Inputs.int(
        default=50,
        description='The maximum number of CPUs for parallel execution. This will be '
        'used to determine the number of sensors run by each worker.',
        spec={'type': 'integer', 'minimum': 1},
        alias=cpu_count
    )

    min_sensor_count = Inputs.int(
        description='The minimum number of sensors in each sensor grid after '
        'redistributing the sensors based on cpu_count. This value takes '
        'precedence over the cpu_count and can be used to ensure that '
        'the parallelization does not result in generating unnecessarily small '
        'sensor grids.',
        default=1000, default_local=500,
        spec={'type': 'integer', 'minimum': 1}
    )

    radiance_parameters = Inputs.str(
        description='The radiance parameters for ray tracing',
        default='-ab 2 -aa 0.1 -ad 2048 -ar 64',
        alias=rad_par_daylight_factor_input
    )

    @task(template=PointInTimeGridPrepareFolder)
    def prepare_folder_point_in_time_grid(
        self, model=model, sky=sky, metric=metric, grid_filter=grid_filter,
        cpu_count=cpu_count, min_sensor_count=min_sensor_count
    ):
        return [
            {
                'from': PointInTimeGridPrepareFolder()._outputs.model_folder,
                'to': 'model'
            },
            {
                'from': PointInTimeGridPrepareFolder()._outputs.resources,
                'to': 'resources'
            },
            {
                'from': PointInTimeGridPrepareFolder()._outputs.initial_results,
                'to': 'initial_results'
            },
            {
                'from': PointInTimeGridPrepareFolder()._outputs.sensor_grids,
                'description': 'Grid information.'
            }
        ]

    @task(
        template=RayTracingPointInTime,
        needs=[prepare_folder_point_in_time_grid],
        loop=prepare_folder_point_in_time_grid._outputs.sensor_grids,
        sub_folder='initial_results/{{item.full_id}}',  # subfolder for each grid
        sub_paths={
            'grid': 'grid/{{item.full_id}}.pts',
            'scene_file': 'scene.oct',
            'bsdf_folder': 'bsdf',
            'ies_folder': 'ies'
            }
    )
    def point_in_time_grid_ray_tracing(
        self,
        radiance_parameters=radiance_parameters,
        metric=metric,
        scene_file=prepare_folder_point_in_time_grid._outputs.resources,
        grid=prepare_folder_point_in_time_grid._outputs.resources,
        bsdf_folder=prepare_folder_point_in_time_grid._outputs.model_folder,
        ies_folder=prepare_folder_point_in_time_grid._outputs.model_folder
    ):
        return [
            {
                'from': RayTracingPointInTime()._outputs.result,
                'to': '../{{item.name}}.res'
            }
        ]

    @task(
        template=PointInTimeGridPostProcess,
        needs=[point_in_time_grid_ray_tracing]
    )
    def post_process_point_in_time_grid(
        self, results_folder='initial_results',
        grids_info='resources/grids_info.json'
    ):
        return [
            {
                'from': PointInTimeGridPostProcess()._outputs.results,
                'to': 'results'
            }
        ]

    results = Outputs.folder(
        source='results/pit', description='Folder with raw result files (.res) that '
        'contain numerical values for each sensor. Values are in standard SI units of '
        'the input metric (lux, W/m2, cd/m2, W/m2-sr).', alias=point_in_time_grid_results
    )
