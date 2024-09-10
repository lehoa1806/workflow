from typing import Dict, Iterator

from .producer import Producer


class SerialProducer(Producer):
    """
    A producer that generates items from a serial data source, such as a
    list or file, for processing in a pipeline.
    """
    def __init__(self, source) -> None:
        """
        Initializes the SerialProducer with a data source.

        :param source: The data source providing items to be processed.
        """
        self.source = source

    def to_stream(self) -> Iterator[Dict]:
        yield from self.source
