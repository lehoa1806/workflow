from typing import Callable, Dict, Iterable, List

from workflow.base_stage import BaseStage
from workflow.consumer import Consumer
from workflow.pipeline import Pipeline


class MultiPathConsumer(Consumer):
    def __init__(
        self,
        consumers: Dict[bool | str, Consumer],
        route: Callable,
        pipeline: Pipeline = None
    ) -> None:
        self.consumers = consumers
        self.route = route
        self.pipeline = pipeline

    def setup(self, item: Dict) -> None:
        pass

    def process(self, item: Dict) -> None:
        pass

    def teardown(self, item: Dict) -> None:
        pass

    def consume(self, source: Iterable[Dict]) -> None:
        source = self.pipeline.run(source) if self.pipeline else source
        for item in source:
            route_key = self.route(item)
            if route_key in self.consumers:
                self.consumers[route_key].consume([item])

    def add_stage(
        self,
        stage: BaseStage,
        logged_columns: List[str] = None,
        name: str = None,
    ) -> 'MultiPathConsumer':
        if self.pipeline:
            self.pipeline = self.pipeline.add_stage(
                stage=stage,
                logged_columns=logged_columns,
                name=name,
            )
        else:
            self.pipeline = Pipeline(
                stage=stage,
                logged_columns=logged_columns,
                name=name,
            )
        return self

    def add_consumer(
        self,
        route_key: str,
        consumer: Consumer
    ) -> 'MultiPathConsumer':
        self.consumers[route_key] = consumer
        return self
