import argparse
import logging

from workflow.examples.data import (DictListConsumer, DictStage, ListStage,
                                    MultiplyConsumer, MultiplyStage,
                                    SumConsumer, SumStage, get_stream)
from workflow.hybrid_consumer import HybridConsumer
from workflow.job import Job
from workflow.pipeline import Pipeline
from workflow.serial_producer import SerialProducer


class SimpleJob(Job):
    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
           '--length',
           type=int,
           required=True,
           help='Length of the testing stream',
        )
        return parser.parse_args(namespace=argparse.Namespace())

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
        return SerialProducer(get_stream(self.args.length))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    # Call Job
    logging.info('=== Process SimpleJOB ===')
    SimpleJob().main()
    # 1 5 9 13 17 21 25 29 33 37
