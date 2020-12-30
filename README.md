# workflow
A simple data processing workflow

# Example
```python
from workflow.consumer import Consumer
from workflow.pipeline import Pipeline
from workflow.serial_producer import SerialProducer
from workflow.stage import Stage
from workflow.worker import Worker


def get_stream():
    for i in range(10):
        yield {
            'operand1': 2 * i,
            'operand2': 2 * i +1,
            'operand3': 'unnecessary data',
        }


class SimpleStage(Stage):
    @property
    def input_columns(self):
        return ['operand1', 'operand2']

    def process(self, item):
        yield {'sum': item['operand1'] + item['operand2']}


class SimpleConsumer(Consumer):
    def process(self, item):
        print(item.get('sum'))


class SimpleWorker(Worker):
    @property
    def args(self):
        pass

    @property
    def pipeline(self) -> Pipeline:
        return Pipeline(
            stage=SimpleStage(),
        )

    @property
    def consumer(self):
        return SimpleConsumer()

    @property
    def producer(self):
        return SerialProducer(get_stream())


if __name__ == "__main__":
    # Step by step
    stream = SerialProducer(get_stream()).stream
    result = SimpleStage().run(stream)
    SimpleConsumer().consume(result)
    # 1 5 9 13 17 21 25 29 33 37

    # Call the worker
    SimpleWorker().main()
    # 1 5 9 13 17 21 25 29 33 37
```