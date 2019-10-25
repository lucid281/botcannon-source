import typing

from walrus.streams import Message


class Collector:
    def read(self) -> typing.Generator[dict, None, None]:
        pass

    def task(self, message: Message, **kwargs) -> dict:
        pass

    def taskback(self, message: Message) -> None:
        pass

