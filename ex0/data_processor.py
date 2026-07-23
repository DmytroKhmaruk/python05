import typing
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[str] = []
        self._rank: int = 0

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


class NumericProcessor(DataProcessor):
    def validate(self, data: int | float | list[int | float]) -> bool:
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
        else:
            self._data.append(str(data))


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
        else:
            self._data.append(data)


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


def main() -> None:
    print("=== Code Nexus - Data Processor ===")
    numeric = NumericProcessor()

    print("\nTesting Numeric Processor...")
    print(f" Trying to validate input '42': {numeric.validate(42)}")
    print(f" Trying to validate input 'Hello': {numeric.validate('Hello')}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        numeric.ingest("foo")
    except TypeError as e:
        print(f" Got exception: {e}")

    print(" Processing data: [1, 2, 3, 4, 5]")
    numeric.ingest([1, 2, 3, 4, 5])
    print(" Extracting 3 values...")
    print(f" Numeric value 0: {numeric.output()[1]}")
    print(f" Numeric value 1: {numeric.output()[1]}")
    print(f" Numeric value 2: {numeric.output()[1]}")

    text = TextProcessor()

    print("\nTesting Text Processor...")
    print(f" Trying to validate input '42': {text.validate(42)}")

    print(" Processing data: ['Hello', 'Nexus', 'World']")
    text.ingest(["Hello", "Nexus", "World"])
    print(" Extracting 1 values...")
    print(f" Text value 0: {text.output()[1]}")

    log = LogProcessor()

    print("\nTesting Log Processor...")
    print(f" Trying to validate input 'Hello': {log.validate('Hello')}")

    print(
        " Processing data: [{'log_level': 'NOTICE', 'log_message':"
        " 'Connection to server'}, {'log_level': 'ERROR'"
        ", 'log_message': 'Unauthorized access!!'}]")
    log.ingest(
        [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
            {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}])
    print(" Extracting 2 values...")
    print(f" Log entry 0: {log.output()[1]}")
    print(f" Log entry 1: {log.output()[1]}")


if __name__ == "__main__":
    main()
