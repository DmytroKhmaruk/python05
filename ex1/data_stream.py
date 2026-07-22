import typing
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[str] = []
        self._rank: int = 0
        self._total_processed: int = 0

    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if len(self._data) == 0:
            raise IndexError("No data available")

        value = self._data[0]
        del self._data[0]

        result = (self._rank, value)
        self._rank += 1
        return result

    def get_total_processed(self) -> int:
        return self._rank + len(self._data)

    def get_data_count(self) -> int:
        return len(self._data)


class NumericProcessor(DataProcessor):
    def validate(self, data: int | float | list[int | float]) -> bool:
        if isinstance(data, bool):
            return False
        if isinstance(data, int) or isinstance(data, float):
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, int) and not isinstance(item, float):
                    return False
            return True
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise TypeError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._data.append(str(item))
                self._total_processed += 1
        else:
            self._data.append(str(data))
            self._total_processed += 1


class TextProcessor(DataProcessor):
    def validate(self, data: str | list[str]) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, str):
                    return False
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise TypeError("Invalid data")
        if isinstance(data, list):
            for item in data:
                self._data.append(item)
                self._total_processed += 1
        else:
            self._data.append(data)
            self._total_processed += 1


class LogProcessor(DataProcessor):
    def _validate_dictionary(self, data: dict[str, str]) -> bool:
        for key in data:
            if not isinstance(key, str):
                return False
            if not isinstance(data[key], str):
                return False
        return True

    def validate(self, data: dict[str, str] | list[dict[str, str]]) -> bool:
        if isinstance(data, dict):
            return self._validate_dictionary(data)
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    return False
                if not self._validate_dictionary(item):
                    return False
            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise TypeError("Invalid data")
        if isinstance(data, list):
            for item in data:
                log_string = (f"{item['log_level']}: "
                              f"{item['log_message']}")
                self._data.append(log_string)
        else:
            log_string = (f"{data['log_level']}: "
                          f"{data['log_message']}")
            self._data.append(str(data))


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            processed = False
            for processor in self._processors:
                if processor.validate(item):
                    processor.ingest(item)
                    processed = True
                    break
            if not processed:
                print(f"DataStream error - Can't process element in "
                      f"stream: {item}")

    def print_processors_stats(self) -> None:
        if len(self._processors) == 0:
            print("No processor found, no data")
            return
        for processor in self._processors:
            name = processor.__class__.__name__
            total = processor.get_total_processed()
            remininig = processor.get_data_count()
            print(f"{name}: total {total} items processed, remaining "
                  f"{remininig} on processor")


def main() -> None:
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")
    data_stream = DataStream()
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()

    print("\nRegistering Numeric Processor\n")
    numeric_processor = NumericProcessor()
    data_stream.register_processor(numeric_processor)
    first_batch: list[typing.Any] = [
        "Hello world", [3.14, -1, 2.71],
        [{
            "log_level": "WARNING",
            "log_message": "Telnet access! Use ssh instead"
            },
            {
            "log_level": 'INFO',
            "log_message": "User wil isconnected"
        }], 42, ["Hi", "five"]
    ]

    print(f"Send first batch of data on stream: {first_batch}")
    data_stream.process_stream(first_batch)
    print("== DataStream statistics ==")
    data_stream.print_processors_stats()
    print("\nRegistering other data processors")

    text_processor = TextProcessor()
    log_processor = LogProcessor()
    data_stream.register_processor(text_processor)
    data_stream.register_processor(log_processor)

    print("Send the same batch again")
    print("== DataStream statistics ==")
    data_stream.process_stream(first_batch)
    data_stream.print_processors_stats()

    print("\nConsume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    print("== DataStream statistics ==")
    numeric_processor.output()
    numeric_processor.output()
    numeric_processor.output()
    text_processor.output()
    text_processor.output()
    log_processor.output()

    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
