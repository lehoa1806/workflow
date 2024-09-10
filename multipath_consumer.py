from typing import Callable, Dict, Iterable, List

from workflow.base_stage import BaseStage
from workflow.consumer import Consumer
from workflow.pipeline import Pipeline


class MultiPathConsumer(Consumer):
    """
    A Consumer capable of directing items to different consumers based on a
    routing function. This allows for dynamic processing pathways within a
    data processing workflow.
    """
    def __init__(
        self,
        consumers: Dict[bool | str, Consumer],
        route: Callable,
        pipeline: Pipeline = None
    ) -> None:
        """
        Initializes the MultiPathConsumer with a set of consumers, a routing
        function, and an optional pipeline.

        :param consumers: A dictionary mapping route keys to Consumer
                          instances.
        :param route: A function that takes an item and returns a  route key
                      to direct the item to a consumer.
        :param pipeline: Optional; a Pipeline instance for pre-processing
                      items before routing.
        """
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
        """
        Processes items from the given source through an (optional) pipeline
        and then routes each item to one of the configured consumers based
        on the specified routing function. This approach enables a dynamic
        consumption pathway, where items can be directed to different
        consumers based on their content or other criteria.

        The routing mechanism allows for versatile processing strategies,
        where each consumer can handle items that meet specific conditions,
        supporting scenarios like conditional processing, categorization,
        or multiplexing within a data workflow.

        :param source: An iterable source of dictionaries representing the
                       data items to be consumed.
        """
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
        """
        Adds a processing stage to the internal pipeline, configuring the
        processing flow of items.

        :param stage: A processing stage to be added.
        :param logged_columns: Columns to be logged by the added stage.
        :param name: Optional name for the processing stage.
        :return: The MultiPathConsumer instance to allow for chaining.
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

    def add_consumer(
        self,
        route_key: str,
        consumer: Consumer
    ) -> 'MultiPathConsumer':
        """
        Adds a consumer to the routing map, enabling the conditional
        processing of items.

        :param route_key: The key used to route items to the corresponding
                          consumer.
        :param consumer: The Consumer instance to be added for the given route
                         key.
        :return: The MultiPathConsumer instance to allow for chaining.
        """
        self.consumers[route_key] = consumer
        return self
