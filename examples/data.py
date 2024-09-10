import logging

from workflow.consumer import Consumer
from workflow.stage import Stage


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
            f'SumStage: operand1 = {item["operand1"]}, '
            f'operand2 = {item["operand2"]}')
        yield {'sum': item['operand1'] + item['operand2']}


class MultiplyStage(Stage):
    @property
    def input_columns(self):
        return ['operand1', 'operand2']

    def process(self, item):
        logging.info(
            f'MultiplyStage: operand1 = {item["operand1"]}, '
            f'operand2 = {item["operand2"]}'
        )
        yield {'multiply': item['operand1'] * item['operand2']}


class DictStage(Stage):
    def process(self, item):
        logging.info(
            f'DictStage: operand1 = {item["operand1"]}, '
            f'operand2 = {item["operand2"]}, '
            f'sum = {item["sum"]}, multiply = {item["multiply"]}'
        )
        output = {
            'operand1': item['operand1'],
            'operand2': item['operand2'],
            'sum': item['operand1'] + item['operand2'],
            'multiply': item['operand1'] * item['operand2'],
        }
        yield {'dict': output}


class ListStage(Stage):
    def process(self, item):
        logging.info(
            f'ListStage: operand1 = {item["operand1"]}, '
            f'operand2 = {item["operand2"]}, '
            f'sum = {item["sum"]}, multiply = {item["multiply"]}'
        )
        output = [
            item['operand1'],
            item['operand2'],
            item['operand1'] + item['operand2'],
            item['operand1'] * item['operand2'],
        ]
        yield {'list': output}


class SumConsumer(Consumer):
    def process(self, item):
        logging.info(
            f'SumConsumer subscribed for: Sum = {item["sum"]}')


class MultiplyConsumer(Consumer):
    def process(self, item):
        logging.info(
            f'MultiplyConsumer subscribed for: Multiply = {item["multiply"]}')


class DictListConsumer(Consumer):
    def process(self, item):
        logging.info(
            f'DictListConsumer wants a list of the two operands, '
            f'sum and multiply results: = {item["list"]}')
        logging.info(
            f'DictListConsumer also wants a dict of the two operands, '
            f'sum and multiply results: = {item["dict"]}')


class Mode3Consumer(Consumer):
    def process(self, item):
        logging.info(
            f'Mode3Consumer wants a list of the two operands, '
            f'sum and multiply results: = {item} '
            f'where operand1 is divisible by 3'
        )
