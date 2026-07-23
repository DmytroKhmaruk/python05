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
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, bool):
            return False
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            for item in data:
                if isinstance(item, bool):
                    return False
                if not isinstance(item, (int, float)):
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
    def validate(self, data: typing.Any) -> bool:
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
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            if "log_level" not in data or "log_message" not in data:
                return False
            for key in data:
                if not isinstance(key, str):
                    return False
                if not isinstance(data[key], str):
                    return False
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    return False
                if "log_level" not in item or "log_message" not in item:
                    return False
                for key in item:
                    if not isinstance(key, str):
                        return False
                    if not isinstance(item[key], str):
                        return False
            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise TypeError("Invalid data")
        if isinstance(data, list):
            for item in data:
                log_string = (f"{item['log_level']}: {item['log_message']}")
                self._data.append(log_string)
        else:
            log_string = (f"{data['log_level']}: {data['log_message']}")
            self._data.append(log_string)


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        values: list[str] = []
        for _, value in data:
            values.append(value)
        print(",".join(values))


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        items: list[str] = []
        for rank, value in data:
            escaped_value = value.replace("\\", "\\\\")
            escaped_value = escaped_value.replace('"', '\\"')
            item = f'"item_{rank}": "{escaped_value}"'
            items.append(item)
        json_output = "{" + ", ".join(items) + "}"
        print(json_output)


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
            name = name.replace("Processor", " Processor")
            total = processor.get_total_processed()
            remininig = processor.get_data_count()
            print(f"{name}: total {total} items processed, remaining "
                  f"{remininig} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        output_data: list[tuple[int, str]] = []

        for processor in self._processors:
            output_data = []
            for _ in range(nb):
                try:
                    result = processor.output()
                    output_data.append(result)
                except IndexError:
                    break
            plugin.process_output(output_data)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===\n")
    print("Initialize Data Stream...\n")
    print("== DataStream statistics ==")
    data_stream = DataStream()
    data_stream.print_processors_stats()

    print("\nRegistering Processor\n")
    numeric_processor = NumericProcessor()
    text_processor = TextProcessor()
    log_processor = LogProcessor()
    data_stream.register_processor(numeric_processor)
    data_stream.register_processor(text_processor)
    data_stream.register_processor(log_processor)
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
    print("\n== DataStream statistics ==")
    data_stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExportPlugin()
    data_stream.output_pipeline(3, csv_plugin)
    print("\n== DataStream statistics ==")
    data_stream.print_processors_stats()

    second_batch: list[typing.Any] = [
        21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR', 'log_message': '500 server crash'},
         {'log_level': 'NOTICE',
         'log_message': 'Certificate expires in 10 days'}],
        [32, 42, 64, 84, 128, 168], 'World hello']

    print(f"\nSend another batch of data: {second_batch}")
    data_stream.process_stream(second_batch)
    print("\n== DataStream statistics ==")
    data_stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExportPlugin()
    data_stream.output_pipeline(5, json_plugin)

    print("\n== DataStream statistics ==")
    data_stream.print_processors_stats()


if __name__ == "__main__":
    main()
