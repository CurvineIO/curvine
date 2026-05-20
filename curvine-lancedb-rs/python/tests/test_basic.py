"""Basic tests for curvine-lancedb Python SDK."""
import pyarrow as pa
import pytest

import curvine_lancedb


@pytest.fixture
async def conn(tmp_path):
    uri = str(tmp_path / "test-db")
    return await curvine_lancedb.connect(uri).execute()


@pytest.fixture
async def table(conn):
    data = pa.record_batch({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [95.5, 87.0, 92.3],
    })
    return await conn.create_table("users", data)


class TestConnection:
    @pytest.mark.asyncio
    async def test_connect(self, conn):
        assert conn is not None

    @pytest.mark.asyncio
    async def test_table_names_empty(self, conn):
        names = await conn.table_names()
        assert names == []

    @pytest.mark.asyncio
    async def test_create_table(self, conn):
        data = pa.record_batch({"id": [1, 2]})
        table = await conn.create_table("t", data)
        assert table is not None

    @pytest.mark.asyncio
    async def test_table_names(self, conn):
        data = pa.record_batch({"id": [1]})
        await conn.create_table("t", data)
        names = await conn.table_names()
        assert "t" in names

    @pytest.mark.asyncio
    async def test_drop_table(self, conn):
        data = pa.record_batch({"id": [1]})
        await conn.create_table("t", data)
        await conn.drop_table("t")
        names = await conn.table_names()
        assert "t" not in names

    @pytest.mark.asyncio
    async def test_open_table(self, conn):
        data = pa.record_batch({"id": [1, 2, 3]})
        await conn.create_table("t", data)
        table = await conn.open_table("t")
        count = await table.count_rows()
        assert count == 3


class TestTable:
    @pytest.mark.asyncio
    async def test_count_rows(self, table):
        assert await table.count_rows() == 3

    @pytest.mark.asyncio
    async def test_add(self, table):
        more = pa.record_batch({"id": [4, 5], "name": ["David", "Eve"], "score": [88.1, 99.9]})
        await table.add(more)
        assert await table.count_rows() == 5

    @pytest.mark.asyncio
    async def test_delete(self, table):
        await table.delete("id = 1")
        assert await table.count_rows() == 2

    @pytest.mark.asyncio
    async def test_query_all(self, table):
        result = await table.search().to_arrow()
        assert result.num_rows == 3
        assert result.column_names == ["id", "name", "score"]

    @pytest.mark.asyncio
    async def test_query_filter(self, table):
        result = await table.search().where("score > 90").to_arrow()
        df = result.to_pandas()
        assert len(df) == 2

    @pytest.mark.asyncio
    async def test_query_limit(self, table):
        result = await table.search().limit(2).to_arrow()
        assert result.num_rows == 2

    @pytest.mark.asyncio
    async def test_query_select(self, table):
        result = await table.search().select(["name", "score"]).to_arrow()
        assert result.column_names == ["name", "score"]

    @pytest.mark.asyncio
    async def test_query_select_and_limit(self, table):
        result = await table.search().select(["name", "score"]).limit(2).to_arrow()
        df = result.to_pandas()
        assert len(df) == 2
        assert list(df.columns) == ["name", "score"]
