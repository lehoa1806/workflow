from typing import Dict, Iterable, Iterator

from .base_stage import BaseStage, Start, Stop


class Stage(BaseStage):
    """
    A concrete implementation of a processing stage within a data pipeline.
    This class provides a framework for implementing the setup, process,
    and teardown phases of data handling, offering a structured approach to
    data transformation or analysis.
    """
    def setup(self, item: Dict) -> Iterator:
        """
        Sets up any necessary resources or state before starting the
        processing of data items. By default, this method emits a Start
        signal to indicate the beginning of a stage's processing.

        :param item: A dictionary potentially containing the initial
                     configuration.
        :return: An iterator that should yield a Start signal.
        """
        yield Start()

    def process(self, item: Dict) -> Iterator:
        """
        Processes a single item. This method is intended to be overridden by
        subclasses to implement specific data processing logic.

        :param item: The item to be processed, as a dictionary.
        :return: An iterator over processed items.
        """
        raise NotImplementedError

    def teardown(self, item: Dict) -> Iterator:
        """
        Cleans up any resources or state after the processing of data items
        has completed. By default, this method emits a Stop signal to
        indicate the end of a stage's processing.

        :param item: A dictionary potentially containing the final
                     configuration.
        :return: An iterator that should yield a Stop signal.
        """
        yield Stop()

    def run(self, source: Iterable = None, **kwargs) -> Iterator:
        """
        Executes the stage by running the setup process for each input item
        and then the teardown process. It is designed to integrate
        seamlessly into a data processing pipeline, automatically handling
        start and stop signals.

        :param source: An iterable of source data items for processing.
        :param kwargs: Additional keyword arguments that may be required for
                       processing.
        :return: An iterator over all processed and potentially transformed
                 data items.
        """
        source = source or []
        logged_columns = kwargs.get('logged_columns') or []
        for in_data in source:
            item = self.get_input_item(in_data)
            logged = {k: in_data.get(k) for k in logged_columns}
            if isinstance(item, Start):
                for out_data in self.setup(item):
                    yield self.get_output_item(out_data, logged_data=logged)
            elif isinstance(item, Stop):
                for out_data in self.teardown(item):
                    yield self.get_output_item(out_data, logged_data=logged)
            else:
                for out_data in self.process(item):
                    yield self.get_output_item(out_data, logged_data=logged)
