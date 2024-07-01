from typing import Iterable, List, Optional
from bytewax.inputs import StatefulSourcePartition, FixedPartitionedSource, batch_async
from binascii import hexlify
from asyncmy import connect
from asyncmy.cursors import SSCursor
from datetime import timedelta
import asyncio


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
        self.event_types = event_types
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout

    def list_parts(self) -> List[str]:
        return [self.table]

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[dict[int, Optional[bytes]]],
    ) -> StatefulSourcePartition[dict, dict[int, Optional[bytes]]]:
        return _SingleStoreSourcePartition(
            self.connection_params,
            for_part,
            resume_state,
            self.event_types,
            self.batch_size,
            self.batch_timeout,
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
        batch_size: int,
        batch_timeout: timedelta,
    ):
        self.connection_params = connection_params
        self.table = table
        self.event_types = event_types
        self.loop = asyncio.new_event_loop()
        self.last_offsets = {
            i: resume_state[i] if resume_state and i in resume_state else None
            for i in range(self.loop.run_until_complete(self._get_partition_count()))
        }
        self.columns = self.loop.run_until_complete(self._describe_table())
        self.batcher = batch_async(
            self._observe(),
            timeout=batch_timeout,
            batch_size=batch_size,
            loop=self.loop,
        )

    def next_batch(self) -> Iterable[dict]:
        return next(self.batcher)

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
        self.connection = await connect(**self.connection_params)
        async with self.connection.cursor() as id_cursor:
            await id_cursor.execute("SELECT CONNECTION_ID()")
            self.connection_id = (await id_cursor.fetchone())[0]

        self.cursor = self.connection.cursor(SSCursor)
        await self.cursor.execute(query)
        while True:
            row = await self.cursor.fetchone()
            self.last_offsets[row[1]] = row[0]
            if row[2] in self.event_types:
                data = row[7:]
                yield {
                    "type": row[2],
                    "data": {
                        self.columns[i]["name"]: data[i] for i in range(len(data))
                    },
                }

    def _stringify_offsets(self):
        ordered_offsets = sorted(self.last_offsets.items(), key=lambda x: x[0])
        return [
            f"'{hexlify(offset).decode('utf-8')}'" if offset is not None else "NULL"
            for _, offset in ordered_offsets
        ]

    async def _get_partition_count(self):
        async with connect(**self.connection_params) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SHOW PARTITIONS")
                partitions = await cursor.fetchall()
                return len(partitions) if partitions is not None else 1

    async def _describe_table(self):
        async with connect(**self.connection_params) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(f"DESCRIBE {self.table}")
                result = await cursor.fetchall()
                return [
                    {"name": row[0], "is_primary_key": row[3] == "PRI"}
                    for row in result
                ]
