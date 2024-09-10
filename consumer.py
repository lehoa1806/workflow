from typing import Dict, Iterable, Set

from .common import Start, Stop


class Consumer:
    """
    A base class representing a consumer in a data processing pipeline. This
    class is designed to consume items from a data source, process them,
    and possibly forward them to the next stage or store the results.
    """
    @property
    def required_columns(self) -> Set:
        """
        Specifies the set of columns required by this consumer for
        processing items.

        :return: A set of required column names.
        """
        return set()

    def setup(self, item: Dict) -> None:
        """
        Sets up resources or configurations needed before beginning to
        consume items.

        :param item: Initial setup data or configurations.
        """
        pass

    def process(self, item: Dict) -> None:
        """
        Processes a single item. This method must be implemented by subclasses.

        :param item: The item to be processed.
        """
        raise NotImplementedError

    def teardown(self, item: Dict) -> None:
        """
        Cleans up resources or configurations after all items have been
        consumed.

        :param item: Final teardown data or configurations.
        """
        pass

    def consume(self, source: Iterable[Dict]) -> None:
        """
        Consumes items from the provided source, processing each in turn.

        :param source: An iterable source of items to consume.
        """
        for item in source:
            if isinstance(item, Start):
                self.setup(item)
            elif isinstance(item, Stop):
                self.teardown(item)
            else:
                if (
                    self.required_columns
                    and not self.required_columns.issubset(set(item.keys()))
                ):
                    raise ValueError(
                        f'Invalid data {item}. '
                        f'Required columns: {self.required_columns}.'
                    )
                self.process(item)
