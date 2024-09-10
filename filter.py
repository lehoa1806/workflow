from typing import Dict, Iterator, List

from .stage import Stage


class Filter(Stage):
    """
    A concrete implementation of a Stage that filters data items based on
    certain criteria. It can modify the data stream by selectively passing
    items through.
    """
    def __init__(
        self,
        output_columns: List[str] = None,
        name: str = None,
    ) -> None:
        """
        Initializes the Filter stage with optional output columns and name.

        :param output_columns: A list of column names to output, filtering
                               out other columns.
        :param name: Optional; the name of the filter stage.
        """
        super().__init__(name)
        self._output_columns = output_columns or []

    @property
    def output_columns(self) -> List[str]:
        return self._output_columns

    def process(self, item: Dict) -> Iterator:
        yield item
