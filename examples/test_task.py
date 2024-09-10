import logging

from workflow.examples.data import (DictListConsumer, DictStage, ListStage,
                                    MultiplyConsumer, MultiplyStage,
                                    SumConsumer, SumStage, get_stream)
from workflow.hybrid_consumer import HybridConsumer
from workflow.pipeline import Pipeline
from workflow.serial_producer import SerialProducer
from workflow.task import Task


class SimpleTask(Task):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.length = kwargs.get('length', 10)

    @property
    def pipeline(self) -> Pipeline:
        return Pipeline(
            stage=SumStage(),
            logged_columns=['operand1', 'operand2'],
        ).add_stage(
            stage=MultiplyStage(),
            logged_columns=['operand1', 'operand2', 'sum'],
        )

    @property
    def consumer(self):
        list_consumer = HybridConsumer(
            consumers=[DictListConsumer()],
            pipeline=Pipeline(
                DictStage(),
                logged_columns=['operand1', 'operand2', 'sum', 'multiply'],
            )
        ).add_stage(
            stage=ListStage(),
            logged_columns=['dict'],
        )

        return HybridConsumer(
            consumers=[list_consumer]
        ).add_consumer(
            consumer=SumConsumer()
        ).add_consumer(
            consumer=MultiplyConsumer(),
        )

    @property
    def producer(self):
        return SerialProducer(get_stream(self.length))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    # Call Task
    logging.info('=== Process SimpleTask ===')
    SimpleTask().process_task(length=5)
    # 1 5 9 13 17
