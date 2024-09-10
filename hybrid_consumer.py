from typing import Dict, Iterable, List

from .base_stage import BaseStage
from .consumer import Consumer
from .pipeline import Pipeline


class HybridConsumer(Consumer):
    """
    A consumer that allows for the composition of multiple consumers and
    optionally a pipeline to process items in a hybrid, flexible manner. It
    can direct items through different pathways for processing.
    """
    def __init__(
        self,
        consumers: List[Consumer],
        pipeline: Pipeline = None,
    ) -> None:
        """
        Initializes the HybridConsumer with a list of consumers and an
        optional pipeline.

        :param consumers: A list of Consumer instances to be used for item
                          consumption.
        :param pipeline: Optional; a Pipeline instance to pre-process items
                         before consumption.
        """
        self.consumers = consumers
        self.pipeline = pipeline

    def setup(self, item: Dict) -> None:
        pass

    def process(self, item: Dict) -> None:
        raise NotImplementedError

    def teardown(self, item: Dict) -> None:
        pass

    def consume(self, source: Iterable[Dict]) -> None:
        """
        Consumes items from the provided iterable source, optionally
        processing them through a pipeline before distributing them to the
        configured list of consumers. Each item is passed to every consumer
        in the list, allowing for parallel processing or multiple output
        pathways.

        If a pipeline is configured, it processes each item before the
        consumption phase. This allows for pre-processing steps like
        filtering, transformation, or aggregation to be applied.

        :param source: An iterable source of dictionaries representing the
                       data items to be consumed.
        """
        source = self.pipeline.run(source) if self.pipeline else source
        for item in source:
            for consumer in self.consumers:
                consumer.consume([item])

    def add_stage(
        self,
        stage: BaseStage,
        logged_columns: List[str] = None,
        name: str = None,
    ) -> 'HybridConsumer':
        """
        Additionally configures the internal pipeline with a processing stage.

        :param stage: A processing stage to be added to the pipeline.
        :param logged_columns: Columns that should be logged by the added
                               stage.
        :param name: Optional name for the processing stage.
        :return: The HybridConsumer instance to allow for chaining.
        """
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

    def add_consumer(self, consumer: Consumer) -> 'HybridConsumer':
        """
        Adds a consumer to the list of consumers in the HybridConsumer.

        :param consumer: The Consumer instance to be added.
        :return: The HybridConsumer instance to allow for chaining.
        """
        self.consumers.append(consumer)
        return self
