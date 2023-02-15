"""Post-process DAG for point-in-time Grid-based."""
from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from dataclasses import dataclass
from pollination.path.copy import CopyFile
from pollination.honeybee_radiance.grid import MergeFolderData
from pollination.honeybee_radiance.post_process import PointInTimeVisMetadata
from pollination.honeybee_display.translate import ModelToVis


@dataclass
class PointInTimeGridPostProcess(GroupedDAG):
    """Post-process for point-in-time-grid."""

    # inputs
    model = Inputs.file(
        description='A Honeybee Model in either JSON or Pkl format. This can also '
        'be a zipped honeybee-radiance folder.',
        extensions=['json', 'hbjson', 'pkl', 'hbpkl', 'zip']
    )

    results_folder = Inputs.folder(
        description='Daylight factor results input folder.'
    )

    grids_info = Inputs.file(
        description='Grids information from the original model.'
    )

    @task(template=MergeFolderData, annotations={'main_task': True})
    def restructure_results(self, input_folder=results_folder, extension='res'):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'results/pit'
            }
        ]

    @task(template=CopyFile, needs=[restructure_results])
    def copy_grid_info(self, src=grids_info):
        return [
            {
                'from': CopyFile()._outputs.dst,
                'to': 'results/pit/grids_info.json'
            }
        ]

    @task(
        template=PointInTimeVisMetadata,
        needs=[restructure_results]
    )
    def create_vis_metadata(self):
        return [
            {
                'from': PointInTimeVisMetadata()._outputs.cfg_file,
                'to': 'results/pit/vis_metadata.json'
            }
        ]

    @task(
        template=ModelToVis,
        needs=[create_vis_metadata, copy_grid_info]
    )
    def create_vsf(
        self, model=model, grid_data='results', output_format='vsf'
    ):
        return [
            {
                'from': ModelToVis()._outputs.output_file,
                'to': 'visualization.vsf'
            }
        ]

    visualization = Outputs.file(
        source='visualization.vsf',
        description='Result visualization in VisualizationSet format.'
    )

    results = Outputs.folder(
        source='results', description='Point-in-time-grid results.'
    )
