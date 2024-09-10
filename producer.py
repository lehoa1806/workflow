from typing import Dict, Iterator

from .common import Start, Stop


class Producer:
    """
    A base class for producing items to be processed in a data pipeline.
    This class allows for the generation of a stream of items, including
    initiation and termination signals.
    """
    @property
    def stream(
        self,
    ) -> Iterator[Dict]:
        """
        A generator that produces a stream of items to be processed,
        starting with a Start signal and ending with a Stop signal.

        :return: An iterator generating a sequence of data items.
        """
        yield Start()
        yield from self.to_stream()
        yield Stop()

    def to_stream(self) -> Iterator[Dict]:
        """
        Generates the data items to be included in the stream. This method
        must be implemented by subclasses to yield actual data items.

        :return: An iterator generating the stream data items.
        """
        raise NotImplementedError
