from typing import Iterable, List, Optional
from bytewax.inputs import StatefulSourcePartition, FixedPartitionedSource
from binascii import hexlify
import MySQLdb
from MySQLdb.cursors import SSCursor


class SingleStoreSource(FixedPartitionedSource[dict, dict[int, Optional[bytes]]]):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        table: str,
        event_types: List[str] = [
            "Insert",
            "Update",
            "Delete",
            "BeginTransaction",
            "CommitTransaction",
            "BeginSnapshot",
            "CommitSnapshot",
        ],
    ):
        self.connection_params = {
            "host": host,
            "port": port,
            "user": user,
            "passwd": password,
            "db": database,
        }
        self.table = table
        self.event_types = event_types

    def list_parts(self) -> List[str]:
        return [self.table]

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[dict[int, Optional[bytes]]],
    ) -> StatefulSourcePartition[dict, dict[int, Optional[bytes]]]:
        return _SingleStoreSourcePartition(
            self.connection_params, for_part, resume_state, self.event_types
        )


class _SingleStoreSourcePartition(
    StatefulSourcePartition[dict, dict[int, Optional[bytes]]]
):
    def __init__(
        self,
        connection_params: dict,
        table: str,
        resume_state: Optional[dict[int, Optional[bytes]]],
        event_types: List[str],
    ):
        self.connection_params = connection_params
        self.table = table
        self.event_types = event_types
        self.last_offsets = {
            i: resume_state[i] if resume_state and i in resume_state else None
            for i in range(self._get_partition_count())
        }
        self.connection, self.cursor = self._start_observe_query()

    def next_batch(self) -> Iterable[dict]:
        row = self.cursor.fetchone()
        if row is None:
            return []
        self.last_offsets[row[1]] = row[0]
        if row[2] in self.event_types:
            yield {"type": row[2], "data": row[7:]}

    def snapshot(self) -> dict[int, Optional[bytes]]:
        return self.last_offsets

    def close(self):
        self.connection.close()
        self.cursor.close()

    def _start_observe_query(self):
        connection = MySQLdb.connect(**self.connection_params)
        cursor = connection.cursor(SSCursor)
        query = f"OBSERVE * FROM {self.table}"
        offsets = self._restore_offsets()
        if offsets:
            query += f" BEGIN AT ({','.join(offsets)})"
        cursor.execute(query)
        return connection, cursor

    def _restore_offsets(self):
        ordered_offsets = sorted(self.last_offsets.items(), key=lambda x: x[0])
        return [
            f"'{hexlify(offset).decode('utf-8')}'" if offset is not None else "NULL"
            for _, offset in ordered_offsets
        ]

    def _get_partition_count(self):
        with MySQLdb.connect(**self.connection_params) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SHOW PARTITIONS")
                partitions = cursor.fetchall()
                return len(partitions) if partitions is not None else 1
