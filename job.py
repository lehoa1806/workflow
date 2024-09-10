import logging
from argparse import Namespace

from .consumer import Consumer
from .pipeline import Pipeline
from .producer import Producer


class Job:
    """
    A class representing a complete job encompassing setup, execution,
    and teardown of a data processing task. This could involve setting up
    producers, consumers, and pipelines to process streamed data.
    """
    def __init__(self) -> None:
        """
        Initializes the job, parsing any arguments and setting up required
        configurations.
        """
        self.args = self.parse_args()

    def parse_args(self) -> Namespace:
        """
        Parses command-line arguments or configurations required for the job.

        :return: A namespace of parsed arguments.
        """
        raise NotImplementedError

    @property
    def pipeline(self) -> Pipeline:
        raise NotImplementedError

    @property
    def consumer(self) -> Consumer:
        raise NotImplementedError

    @property
    def producer(self) -> Producer:
        raise NotImplementedError

    def setup(self) -> None:
        """
        Sets up any required resources or initial state before beginning the
        job.
        """
        pass

    def teardown(self) -> None:
        """
        Cleans up resources or state after the job has completed.
        """
        pass

    def main(self) -> None:
        """
        The main execution method for the job, which typically involves
        initializing the data pipeline, passing data through it,
        and handling any exceptions or cleanup.
        """
        try:
            logging.info('Start')
            self.setup()
            self.consumer.consume(self.pipeline.run(self.producer.stream))
        except Exception as ex:
            logging.warning('Failed')
            logging.exception(str(ex))
            raise SystemExit(1)
        finally:
            self.teardown()
            logging.info('Stop')
