import json
import logging
from collections import defaultdict
from datetime import datetime

from destination_databricks.client import DatabricksSqlClient


class DatabricksSqlWriter:
    """
    Base class for shared writer logic.
    """

    flush_interval = 1000

    def __init__(self, client: DatabricksSqlClient) -> None:
        """
        :param client: Databricks SDK connection class with established connection
            to the database.
        """
        self.client = client
        self._buffer = defaultdict(list)
        self._values = 0

    def delete_table(self, name: str) -> None:
        """
        Delete the resulting table.
        Primarily used in Overwrite strategy to clean up previous data.

        :param name: table name to delete.
        """
        cursor = self.client.open()
        cursor.execute(f"DROP TABLE IF EXISTS _airbyte_raw_{name}")
        cursor.close()

    def create_raw_table(self, name: str):
        """
        Create the resulting _airbyte_raw table.

        :param name: table name to create.
        """
        query = f"""
        CREATE TABLE IF NOT EXISTS _airbyte_raw_{name} (
            _airbyte_ab_id STRING,
            _airbyte_emitted_at TIMESTAMP,
            _airbyte_data STRING
        )
        """
        cursor = self.client.open()
        cursor.execute(query)
        cursor.close()

    def queue_write_data(self, stream_name: str, id: str, time: datetime, record: str) -> None:
        """
        Queue up data in a buffer in memory before writing to the database.
        When flush_interval is reached data is persisted.

        :param stream_name: name of the stream for which the data corresponds.
        :param id: unique identifier of this data row.
        :param time: time of writing.
        :param record: string representation of the json data payload.
        """
        self._buffer[stream_name].append((id, time, record))
        self._values += 1
        if self._values == self.flush_interval:
            self._flush()

    def _flush(self):
        """
        Stub for the intermediate data flush that's triggered during the
        buffering operation.
        """
        raise NotImplementedError()

    def flush(self):
        """
        Stub for the data flush at the end of writing operation.
        """
        raise NotImplementedError()


class DatabricksSqlWriterImpl(DatabricksSqlWriter):
    """
    Data writer using the SQL writing strategy. Data is buffered in memory
    and flushed using INSERT INTO SQL statement.
    """

    flush_interval = 1000

    def __init__(self, client: DatabricksSqlClient) -> None:
        """
        :param client: Databricks SDK connection class with established connection
            to the databse.
        """
        super().__init__(client)

    @staticmethod
    def quote_value(value):
        if isinstance(value, str):
            res = value.replace('\'', '\'\'')
            return f"'{res}'"  # Escape single quotes in strings
        elif isinstance(value, dict):
            res = json.dumps(value).replace('\'', '\'\'')
            return f"'{res}'"  # Convert dict to JSON and escape
        elif value is None:
            return "NULL"  # Handle NULLs
        elif isinstance(value, datetime):
            return f"'{str(value)}'"
        else:
            return str(value)  # For numbers and other types

    def _flush(self) -> None:
        """
        Intermediate data flush that's triggered during the
        buffering operation. Writes data stored in memory via SQL commands.
        Databricks connector insert into table using stage
        """
        cursor = self.client.open()
        # id, written_at, data
        for table, data in self._buffer.items():
            values = ",".join([
                f"({self.quote_value(x)}, {self.quote_value(y)}, {self.quote_value(z)})"
                for (x, y, z) in data
            ])
            cursor.execute(
                f"INSERT INTO _airbyte_raw_{table} (_airbyte_ab_id,_airbyte_emitted_at,_airbyte_data) VALUES {values}",
            )
        self._buffer.clear()
        self._values = 0
        cursor.close()

    def flush(self) -> None:
        """
        Final data flush after all data has been written to memory.
        """
        self._flush()


def create_databricks_writer(client: DatabricksSqlClient, logger: logging.Logger) -> DatabricksSqlWriter:
    logger.info("Using the SQL writing strategy")
    writer = DatabricksSqlWriterImpl(client)
    return writer
