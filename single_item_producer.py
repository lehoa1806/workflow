from typing import Any, Dict, Iterator

from .producer import Producer


class SingleItemProducer(Producer):
    """
    A producer that generates a stream consisting of a single item, useful
    for processing tasks that only need to process one item at a time.
    """
    def __init__(self, item: Dict[str, Any]) -> None:
        """
        Initializes the SingleItemProducer with the item to be produced.

        :param item: The single item to be processed.
        """
        self.item = item

    def to_stream(self) -> Iterator[Dict]:
        yield self.item
