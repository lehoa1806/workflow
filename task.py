import logging
import traceback
import warnings

from .consumer import Consumer
from .pipeline import Pipeline
from .producer import Producer


class Task:
    """
    A high-level class designed to orchestrate the execution of a data
    processing task, integrating producers, pipelines, and consumers in a
    cohesive workflow.
    """
    def __init__(self, **kwargs) -> None:
        """
        Initializes the Task with optional configuration parameters.

        :param kwargs: Configuration parameters for the task.
        """
        self.name = kwargs.get('name') or self.__class__.__name__

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
        pass

    def teardown(self) -> None:
        pass

    def main(self) -> None:
        try:
            warnings.warn(f'{self.name} started ...')
            logging.info(f'{self.name} started ...')
            self.setup()
            self.consumer.consume(self.pipeline.run(self.producer.stream))
        except Exception as ex:
            warnings.warn(f'{self.name} failed: {traceback.format_exc()}')
            logging.warning(f'{self.name} failed ...')
            logging.exception(str(ex))
        finally:
            self.teardown()
            warnings.warn(f'{self.name} stopped ...')
            logging.info(f'{self.name} stopped ...')

    @classmethod
    def process_task(cls, **kwargs) -> None:
        """
        Class method to instantiate and execute the task with provided
        parameters.

        :param kwargs: Configuration parameters for creating and running the
                       task instance.
        """
        cls(**kwargs).main()
