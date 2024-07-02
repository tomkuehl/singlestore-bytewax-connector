from typing import Iterable, List, Optional
from bytewax.inputs import StatefulSourcePartition, FixedPartitionedSource, batch_async
from binascii import hexlify
from asyncmy import connect
from asyncmy.cursors import SSCursor
from datetime import timedelta
import asyncio
from prometheus_client import Gauge


OFFSET_LAG_GAUGE = Gauge(
    "singlestore_partition_offset_lag",
    "The number of events (Inserts, Updates, Deletes) that the connefctor lags behind.",
    ["step_id", "table", "partition_id"],
)

snapshot_and_transaction_type = [
    "BeginTransaction",
    "CommitTransaction",
    "BeginSnapshot",
    "CommitSnapshot",
]


class SingleStoreSource(
    FixedPartitionedSource[dict, dict[int, dict[int, Optional[bytes]]]]
):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        table: str,
        include_snapshot_and_transaction_events: bool = False,
        batch_size: int = 100,
        batch_timeout: timedelta = timedelta(seconds=1),
    ):
        self.connection_params = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
        }
        self.table = table
        self.include_snapshot_and_transaction_events = (
            include_snapshot_and_transaction_events
        )
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout

    def list_parts(self) -> List[str]:
        return [self.table]

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[dict[int, dict[int, Optional[bytes]]]],
    ) -> StatefulSourcePartition[dict, dict[int, dict[int, Optional[bytes]]]]:
        metric_labels = {"step_id": step_id, "table": for_part}
        return _SingleStoreSourcePartition(
            self.connection_params,
            for_part,
            resume_state,
            metric_labels,
            self.include_snapshot_and_transaction_events,
            self.batch_size,
            self.batch_timeout,
        )


class _SingleStoreSourcePartition(
    StatefulSourcePartition[dict, dict[int, dict[int, Optional[bytes]]]]
):
    def __init__(
        self,
        connection_params: dict,
        table: str,
        resume_state: Optional[dict[int, dict[int, Optional[bytes]]]],
        metric_labels: dict,
        include_snapshot_and_transaction_events: bool,
        batch_size: int,
        batch_timeout: timedelta,
    ):
        self.connection_params = connection_params
        self.table = table
        self.metric_labels = metric_labels
        self.include_snapshot_and_transaction_events = (
            include_snapshot_and_transaction_events
        )
        self.previous_batch = []
        self.loop = asyncio.new_event_loop()
        self.connection = self.loop.run_until_complete(
            connect(**self.connection_params)
        )
        self.last_offsets = {
            i: resume_state[i]
            if resume_state and i in resume_state
            else {"count": 0, "offset": None}
            for i in range(self.loop.run_until_complete(self._get_partition_count()))
        }
        self.columns = self.loop.run_until_complete(self._describe_table())
        self.partitions_offsets = self.loop.run_until_complete(
            self._get_row_change_counts()
        )
        self.batcher = batch_async(
            self._observe(),
            timeout=batch_timeout,
            batch_size=batch_size,
            loop=self.loop,
        )

    def next_batch(self) -> Iterable[dict]:
        batch = next(self.batcher)
        self._commit_previous_batch()
        self._update_metrics()
        # TODO: Why doesn't this work:
        # self.loop.run_until_complete(self._get_row_change_counts())
        self.previous_batch = batch
        return batch

    def snapshot(self) -> dict[int, Optional[bytes]]:
        return self.last_offsets

    def close(self):
        async def cleanup():
            if self.cursor:
                try:
                    async with connect(**self.connection_params) as kill_connection:
                        async with kill_connection.cursor() as kill_cursor:
                            await kill_cursor.execute(
                                f"KILL QUERY {self.connection_id}"
                            )
                except Exception:
                    print(f"Failed to kill query {self.connection_id}")
                await self.cursor.close()
            if self.connection:
                await self.connection.close()

        self.loop.run_until_complete(cleanup())
        self.loop.close()

    async def _observe(self):
        query = f"OBSERVE * FROM {self.table}"
        offsets = self._stringify_offsets()
        if offsets:
            query += f" BEGIN AT ({','.join(offsets)})"
        async with self.connection.cursor() as id_cursor:
            await id_cursor.execute("SELECT CONNECTION_ID()")
            self.connection_id = (await id_cursor.fetchone())[0]

        self.cursor = self.connection.cursor(SSCursor)
        await self.cursor.execute(query)
        while True:
            row = await self.cursor.fetchone()
            data = row[7:]
            event = {
                "type": row[2],
                "partition_id": row[1],
                "offset": row[0],
                "data": {self.columns[i]["name"]: data[i] for i in range(len(data))},
            }
            if self.include_snapshot_and_transaction_events:
                yield event
            elif row[2] in ["Insert", "Update", "Delete"]:
                yield event

    def _stringify_offsets(self):
        ordered_offsets = sorted(self.last_offsets.items(), key=lambda x: x[0])
        return [
            f"'{hexlify(partition['offset']).decode('utf-8')}'"
            if partition["offset"] is not None
            else "NULL"
            for _, partition in ordered_offsets
        ]

    async def _get_partition_count(self):
        async with self.connection.cursor() as cursor:
            await cursor.execute("SHOW PARTITIONS")
            partitions = await cursor.fetchall()
            return len(partitions) if partitions is not None else 1

    async def _describe_table(self):
        async with self.connection.cursor() as cursor:
            await cursor.execute(f"DESCRIBE {self.table}")
            result = await cursor.fetchall()
            return [
                {"name": row[0], "is_primary_key": row[3] == "PRI"} for row in result
            ]

    async def _get_row_change_counts(self):
        async with self.connection.cursor(SSCursor) as cursor:
            await cursor.execute(
                f"SELECT partition, total_changes FROM information_schema.mv_collected_row_change_counts WHERE table_name = '{self.table}'"
            )
            rows = await cursor.fetchall()
            return [(row[0], row[1]) for row in rows]

    def _update_metrics(self):
        for partition_id, offset in self.partitions_offsets:
            processed = self.last_offsets[partition_id]["count"]
            lag = offset - processed
            OFFSET_LAG_GAUGE.labels(
                **self.metric_labels, partition_id=partition_id
            ).set(lag)

    def _commit_previous_batch(self):
        for row in self.previous_batch:
            self.last_offsets[row["partition_id"]]["offset"] = row["offset"]
            if row["type"] in ["Insert", "Update", "Delete"]:
                self.last_offsets[row["partition_id"]]["count"] += 1
