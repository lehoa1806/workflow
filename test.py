import argparse
import logging

from workflow.consumer import Consumer
from workflow.pipeline import Pipeline
from workflow.serial_producer import SerialProducer
from workflow.stage import Stage
from workflow.worker import Worker


def get_stream(x: int = 10):
    for i in range(x):
        yield {
            'operand1': 2 * i,
            'operand2': 2 * i + 1,
            'operand3': 'unnecessary data',
        }


class SumStage(Stage):
    @property
    def input_columns(self):
        return ['operand1', 'operand2']

    def process(self, item):
        logging.info(
            f'SumStage: operand1 = {item["operand1"]}, operand2 = {item["operand2"]}')
        yield {'sum': item['operand1'] + item['operand2']}


class MultiplyStage(Stage):
    @property
    def input_columns(self):
        return ['operand1', 'operand2']

    def process(self, item):
        logging.info(
            f'MultiplyStage: operand1 = {item["operand1"]}, operand2 = {item["operand2"]}')
        yield {'multiply': item['operand1'] * item['operand2']}


class SimpleConsumer(Consumer):
    def process(self, item):
        logging.info(item.get('sum'))


class SimpleWorker(Worker):
    def parse_args(self):
        parser = argparse.ArgumentParser()
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
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
            logged_columns=['sum'],
        )

    @property
    def consumer(self):
        return SimpleConsumer()

    @property
    def producer(self):
        return SerialProducer(get_stream(self.args.length))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    # Step by step
    # stream = SerialProducer(get_stream()).stream
    # result = SimpleStage().run(stream)
    # SimpleConsumer().consume(result)
    # 1 5 9 13 17 21 25 29 33 37

    # Call the worker
    SimpleWorker().main()
    # 1 5 9 13 17 21 25 29 33 37
