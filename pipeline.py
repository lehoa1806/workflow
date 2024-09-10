from typing import Dict, Iterable, Iterator, List

from .base_stage import BaseStage


class Pipeline(BaseStage):
    """
    A class that represents a sequence of processing stages forming a data
    pipeline. Each stage in the pipeline is responsible for specific
    processing tasks and can be composed to perform complex data
    transformations.
    """
    def __init__(
        self,
        stage,
        logged_columns: List[str] = None,
        name: str = None,
    ) -> None:
        """
        Initializes the Pipeline with a single starting stage.

        :param stage: The first processing stage to be added to the pipeline.
        :param logged_columns: Optional columns to be logged during processing.
        :param name: Optional name for the pipeline.
        """
        name = name or f'Pipeline:{stage.name}'
        super().__init__(name)
        self.stages = [{
            'stage': stage,
            'logged_columns': logged_columns or [],
        }]

    def setup(self, item: Dict) -> Iterator:
        pass

    def process(self, item: Dict) -> Iterator:
        pass

    def teardown(self, item: Dict) -> Iterator:
        pass

    def run(self, source: Iterable = None, **kwargs) -> Iterator:
        source = source or []
        for stage_info in self.stages:
            stage = stage_info.get('stage')
            if not isinstance(stage, BaseStage):
                continue
            logged_columns = stage_info.get('logged_columns') or []
            source = stage.run(source, logged_columns=logged_columns)
        yield from source

    def add_stage(
        self,
        stage: BaseStage,
        logged_columns: List[str] = None,
        name: str = None,
    ) -> 'Pipeline':
        """
        Adds a new stage to the pipeline for processing items in sequence.

        :param stage: The stage to be added.
        :param logged_columns: Optional columns to be logged by this stage.
        :param name: Optional name for the stage.
        :return: The pipeline instance to allow for method chaining.
        """
        # TODO: Doesn't work if isinstace(stage, Pipeline). Correct this!!!
        name = name or stage.name
        self.name = f'{self.name}:{name}'
        self.stages.append({
            'stage': stage,
            'logged_columns': logged_columns or [],
        })
        return self
