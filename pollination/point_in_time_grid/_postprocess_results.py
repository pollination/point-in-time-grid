"""Post-process DAG for point-in-time Grid-based."""
from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from dataclasses import dataclass
from pollination.path.copy import CopyFile
from pollination.honeybee_radiance.grid import MergeFolderData


@dataclass
class PointInTimeGridPostProcess(GroupedDAG):
    """Post-process for point-in-time-grid."""

    # inputs
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

    results = Outputs.folder(
        source='results', description='Point-in-time-grid results.'
    )
