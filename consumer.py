from typing import Dict, Iterable

from .common import Start, Stop


class Consumer:
    def setup(self, item: Dict) -> None:
        pass

    def process(self, item: Dict) -> None:
        raise NotImplementedError

    def teardown(self, item: Dict) -> None:
        pass

    def consume(self, source: Iterable[Dict]) -> None:
        for item in source:
            if isinstance(item, Start):
                self.setup(item)
            elif isinstance(item, Stop):
                self.teardown(item)
            else:
                self.process(item)
