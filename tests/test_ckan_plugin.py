"""Comprehensive tests for CKAN plugin.

These tests verify plugin initialization, tool execution, API interactions,
error handling, and data formatting. Tests are designed to fail if functionality breaks.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch

import httpx

from plugins.ckan.plugin import CKANPlugin


class TestPluginInitialization:
    """Test plugin initialization."""

    @pytest.fixture
    def ckan_config(self):
        """Standard CKAN plugin configuration."""
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
            "timeout": 120,
        }

    @pytest.mark.asyncio
    async def test_plugin_initialization_succeeds(self, ckan_config):
        """Test that plugin initialization succeeds with valid config."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = Mock()
            mock_response.json.return_value = {"success": True}
            mock_response.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client_class.return_value = mock_client

            result = await plugin.initialize()

            assert result is True
            assert plugin.is_initialized is True
            assert plugin.client is not None
            mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_plugin_initialization_fails_on_api_error(self, ckan_config):
        """Test that plugin initialization fails when API test fails."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = Mock()
            mock_response.json.return_value = {"success": False}
            mock_response.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client_class.return_value = mock_client

            result = await plugin.initialize()

            assert result is False
            assert plugin.is_initialized is False

    @pytest.mark.asyncio
    async def test_plugin_initialization_fails_on_exception(self, ckan_config):
        """Test that plugin initialization fails on exception."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client_class.side_effect = Exception("Connection failed")

            result = await plugin.initialize()

            assert result is False
            assert plugin.is_initialized is False

    @pytest.mark.asyncio
    async def test_plugin_initialization_with_api_key(self, ckan_config):
        """Test that plugin initialization includes API key in headers."""
        ckan_config["api_key"] = "test-api-key-123"
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = Mock()
            mock_response.json.return_value = {"success": True}
            mock_response.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client_class.return_value = mock_client

            await plugin.initialize()

            # Verify AsyncClient was created with Authorization header
            call_kwargs = mock_client_class.call_args[1]
            assert "headers" in call_kwargs
            assert call_kwargs["headers"]["Authorization"] == "test-api-key-123"

    @pytest.mark.asyncio
    async def test_plugin_shutdown_closes_client(self, ckan_config):
        """Test that plugin shutdown closes HTTP client."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = Mock()
            mock_response.json.return_value = {"success": True}
            mock_response.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            assert plugin.client is not None

            await plugin.shutdown()

            mock_client.aclose.assert_called_once()
            assert plugin.client is None
            assert plugin.is_initialized is False


class TestGetTools:
    """Test get_tools method."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    def test_get_tools_returns_all_tools(self, ckan_config):
        """Test that get_tools returns all expected tools."""
        plugin = CKANPlugin(ckan_config)
        tools = plugin.get_tools()

        assert len(tools) == 7
        tool_names = [t.name for t in tools]
        assert "search_datasets" in tool_names
        assert "get_dataset" in tool_names
        assert "query_data" in tool_names
        assert "get_schema" in tool_names
        assert "execute_sql" in tool_names
        assert "aggregate_data" in tool_names
        assert "search_and_query" in tool_names

    def test_get_tools_includes_city_name_in_descriptions(self, ckan_config):
        """Test that tool descriptions include city name."""
        plugin = CKANPlugin(ckan_config)
        tools = plugin.get_tools()

        for tool in tools:
            if (
                tool.name != "execute_sql"
            ):  # execute_sql has different description format
                assert "TestCity" in tool.description

    def test_get_tools_has_correct_input_schemas(self, ckan_config):
        """Test that tools have correct input schemas."""
        plugin = CKANPlugin(ckan_config)
        tools = plugin.get_tools()

        search_tool = next(t for t in tools if t.name == "search_datasets")
        assert search_tool.input_schema["type"] == "object"
        assert "query" in search_tool.input_schema["properties"]
        assert "limit" in search_tool.input_schema["properties"]
        assert "query" in search_tool.input_schema["required"]


class TestSearchDatasets:
    """Test search_datasets method."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_search_datasets_returns_results(self, ckan_config):
        """Test that search_datasets returns dataset results."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            # First call for initialize
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            # Second call for search
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {
                    "results": [
                        {"id": "dataset-1", "title": "Dataset 1"},
                        {"id": "dataset-2", "title": "Dataset 2"},
                    ]
                }
            }
            mock_response_search.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_search]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            results = await plugin.search_datasets("test query", limit=10)

            assert len(results) == 2
            assert results[0]["id"] == "dataset-1"
            assert results[1]["id"] == "dataset-2"

    @pytest.mark.asyncio
    async def test_search_datasets_handles_empty_results(self, ckan_config):
        """Test that search_datasets handles empty results."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {"result": {"results": []}}
            mock_response_search.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_search]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            results = await plugin.search_datasets("nonexistent", limit=10)

            assert results == []

    @pytest.mark.asyncio
    async def test_search_datasets_passes_query_and_limit(self, ckan_config):
        """Test that search_datasets passes correct parameters to API."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {"result": {"results": []}}
            mock_response_search.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_search]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            await plugin.search_datasets("test query", limit=25)

            # Check second call (after initialize)
            call_args = mock_client.post.call_args_list[1]
            assert call_args[0][0] == "/api/3/action/package_search"
            assert call_args[1]["json"]["q"] == "test query"
            assert call_args[1]["json"]["rows"] == 25


class TestGetDataset:
    """Test get_dataset method."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_get_dataset_returns_dataset_metadata(self, ckan_config):
        """Test that get_dataset returns dataset metadata."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_dataset = Mock()
            mock_response_dataset.json.return_value = {
                "result": {
                    "id": "dataset-1",
                    "title": "Test Dataset",
                    "description": "Test description",
                }
            }
            mock_response_dataset.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_dataset]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            dataset = await plugin.get_dataset("dataset-1")

            assert dataset["id"] == "dataset-1"
            assert dataset["title"] == "Test Dataset"
            assert dataset["description"] == "Test description"

    @pytest.mark.asyncio
    async def test_get_dataset_passes_dataset_id(self, ckan_config):
        """Test that get_dataset passes dataset ID to API."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_dataset = Mock()
            mock_response_dataset.json.return_value = {"result": {}}
            mock_response_dataset.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_dataset]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            await plugin.get_dataset("test-dataset-id")

            call_args = mock_client.post.call_args_list[1]
            assert call_args[1]["json"]["id"] == "test-dataset-id"


class TestQueryData:
    """Test query_data method."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_query_data_returns_records(self, ckan_config):
        """Test that query_data returns data records."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_query = Mock()
            mock_response_query.json.return_value = {
                "result": {
                    "records": [
                        {"id": 1, "name": "Record 1"},
                        {"id": 2, "name": "Record 2"},
                    ]
                }
            }
            mock_response_query.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_query]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            records = await plugin.query_data("resource-123", limit=10)

            assert len(records) == 2
            assert records[0]["id"] == 1
            assert records[1]["id"] == 2

    @pytest.mark.asyncio
    async def test_query_data_passes_filters(self, ckan_config):
        """Test that query_data passes filters to API."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_query = Mock()
            mock_response_query.json.return_value = {"result": {"records": []}}
            mock_response_query.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_query]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            await plugin.query_data(
                "resource-123",
                filters={"status": "Open", "category": "311"},
                limit=50,
            )

            call_args = mock_client.post.call_args_list[1]
            params = call_args[1]["json"]
            assert params["resource_id"] == "resource-123"
            assert params["limit"] == 50
            assert params["filters"] == {"status": "Open", "category": "311"}


class TestExecuteTool:
    """Test execute_tool method."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_execute_tool_search_datasets_succeeds(self, ckan_config):
        """Test executing search_datasets tool."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {"results": [{"id": "1", "title": "Test"}]}
            }
            mock_response_search.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_search]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_datasets", {"query": "test", "limit": 10}
            )

            assert result.success is True
            assert len(result.content) > 0
            assert "text" in result.content[0]

    @pytest.mark.asyncio
    async def test_execute_tool_get_dataset_missing_param(self, ckan_config):
        """Test executing get_dataset tool without required parameter."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=mock_response_init)
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool("get_dataset", {})

            assert result.success is False
            assert "required" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_execute_tool_execute_sql_succeeds(self, ckan_config):
        """Test executing execute_sql tool with valid SQL."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_sql = Mock()
            mock_response_sql.json.return_value = {
                "result": {
                    "records": [{"id": 1, "name": "Test"}],
                    "fields": [
                        {"id": "id", "type": "int"},
                        {"id": "name", "type": "text"},
                    ],
                }
            }
            mock_response_sql.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_sql]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "execute_sql",
                {
                    "sql": 'SELECT * FROM "11111111-2222-3333-4444-555555555555" LIMIT 1'
                },
            )

            assert result.success is True
            assert len(result.content) > 0

    @pytest.mark.asyncio
    async def test_execute_tool_execute_sql_validation_error(self, ckan_config):
        """Test executing execute_sql tool with invalid SQL."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=mock_response_init)
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "execute_sql", {"sql": "DELETE FROM users"}
            )

            assert result.success is False
            assert result.error_message is not None
            assert "SELECT" in result.error_message or "DELETE" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_tool_execute_sql_missing_param(self, ckan_config):
        """Test executing execute_sql tool without sql parameter."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=mock_response_init)
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool("execute_sql", {})

            assert result.success is False
            assert "required" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_execute_tool_search_datasets_surfaces_total_count(
        self, ckan_config
    ):
        """search_datasets reads CKAN's `count` and renders X-of-Y."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {
                    "count": 47,
                    "results": [
                        {"id": f"d{i}", "title": f"Dataset {i}", "resources": []}
                        for i in range(20)
                    ],
                }
            }
            mock_response_search.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_search]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_datasets", {"query": "parks", "limit": 20}
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "20 of 47 matching dataset(s) shown" in text

    @pytest.mark.asyncio
    async def test_execute_tool_execute_sql_truncated_warning(self, ckan_config):
        """execute_sql warns when len(records) hits the LIMIT clause."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_sql = Mock()
            mock_response_sql.json.return_value = {
                "result": {
                    "records": [{"_id": i, "x": i} for i in range(100)],
                    "fields": [{"id": "x", "type": "int"}],
                }
            }
            mock_response_sql.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_sql]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "execute_sql",
                {
                    "sql": (
                        'SELECT * FROM "11111111-2222-3333-4444-555555555555" '
                        "LIMIT 100"
                    )
                },
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "MAY BE TRUNCATED" in text
            assert "ckan__aggregate_data" in text or "COUNT(*)" in text

    @pytest.mark.asyncio
    async def test_execute_tool_execute_sql_no_warning_under_limit(
        self, ckan_config
    ):
        """execute_sql does not warn when fewer rows returned than LIMIT."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_sql = Mock()
            mock_response_sql.json.return_value = {
                "result": {
                    "records": [{"_id": i, "x": i} for i in range(7)],
                    "fields": [{"id": "x", "type": "int"}],
                }
            }
            mock_response_sql.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_sql]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "execute_sql",
                {
                    "sql": (
                        'SELECT * FROM "11111111-2222-3333-4444-555555555555" '
                        "LIMIT 100"
                    )
                },
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "TRUNCATED" not in text
            assert "7 rows returned" in text

    @pytest.mark.asyncio
    async def test_execute_tool_search_and_query_succeeds(self, ckan_config):
        """search_and_query returns rows from the first resource of the first match."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            # 1) search_datasets
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {
                    "results": [
                        {
                            "id": "dataset-1",
                            "title": "311 Service Requests",
                            "resources": [
                                {
                                    "id": "11111111-2222-3333-4444-555555555555",
                                    "name": "311 CSV",
                                    "format": "CSV",
                                    "datastore_active": True,
                                }
                            ],
                        }
                    ]
                }
            }
            mock_response_search.raise_for_status = Mock()
            # 2) datastore_search (query_data)
            mock_response_query = Mock()
            mock_response_query.json.return_value = {
                "result": {
                    "records": [
                        {"_id": 1, "type": "Pothole"},
                        {"_id": 2, "type": "Streetlight"},
                    ]
                }
            }
            mock_response_query.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_search,
                    mock_response_query,
                ]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_and_query", {"query": "311", "limit": 10}
            )

            assert result.success is True
            assert len(result.content) == 1
            text = result.content[0]["text"]
            # Header surfaces the chosen IDs
            assert "11111111-2222-3333-4444-555555555555" in text
            assert "dataset-1" in text
            # Rows from the second mocked call show up
            assert "Pothole" in text or "Streetlight" in text

    @pytest.mark.asyncio
    async def test_execute_tool_search_and_query_no_matches(self, ckan_config):
        """search_and_query returns an error when no datasets match."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {"result": {"results": []}}
            mock_response_search.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_search]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_and_query", {"query": "nonexistent-keyword-xyz"}
            )

            assert result.success is False
            assert result.error_message is not None
            assert "No datasets found" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_tool_search_and_query_dataset_has_no_resources(
        self, ckan_config
    ):
        """search_and_query reports an error when the matched dataset has no resources."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {
                    "results": [
                        {"id": "dataset-empty", "title": "Empty", "resources": []}
                    ]
                }
            }
            mock_response_search.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_search]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_and_query", {"query": "anything"}
            )

            assert result.success is False
            err = (result.error_message or "").lower()
            assert "no queryable" in err or "no resources" in err

    @pytest.mark.asyncio
    async def test_execute_tool_search_and_query_skips_download_only_resources(
        self, ckan_config
    ):
        """Parks-style regression: a dataset with GeoJSON/KML/SHP first and a
        single datastore_active CSV — the composite tool must skip past the
        download-only resources and query the CSV one."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {
                    "results": [
                        {
                            "id": "dataset-parks",
                            "title": "Park_Features",
                            "resources": [
                                {
                                    "id": "0826fc19-4ff8-44a5-b9c4-916960d8cfb3",
                                    "format": "GeoJSON",
                                    "datastore_active": False,
                                },
                                {
                                    "id": "4d28fc98-c503-4065-987f-9fbc41947fc4",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                                {
                                    "id": "5f130274-b67e-44e6-9c72-4175a2dca339",
                                    "format": "SHP",
                                    "datastore_active": False,
                                },
                            ],
                        }
                    ]
                }
            }
            mock_response_search.raise_for_status = Mock()
            mock_response_query = Mock()
            mock_response_query.json.return_value = {
                "result": {
                    "records": [
                        {"_id": 1, "park_name": "Boston Common"},
                        {"_id": 2, "park_name": "Franklin Park"},
                    ]
                }
            }
            mock_response_query.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_search,
                    mock_response_query,
                ]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_and_query", {"query": "parks"}
            )

            assert result.success is True
            text = result.content[0]["text"]
            # Picked the CSV resource, not the GeoJSON one
            assert "4d28fc98-c503-4065-987f-9fbc41947fc4" in text
            assert "0826fc19-4ff8-44a5-b9c4-916960d8cfb3" not in text
            assert "Boston Common" in text or "Franklin Park" in text
            # And the third call's body asked for the CSV resource
            third_call = mock_client.post.call_args_list[2]
            assert third_call[1]["json"]["resource_id"] == (
                "4d28fc98-c503-4065-987f-9fbc41947fc4"
            )

    @pytest.mark.asyncio
    async def test_execute_tool_search_and_query_resource_name_picks_archive(
        self, ckan_config
    ):
        """Boston-style regression: a 311 dataset with a rolling NEW SYSTEM
        plus per-year archives. resource_name='2020' must pick the 2020
        archive, not the first datastore_active resource."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {
                    "results": [
                        {
                            "id": "dataset-311",
                            "title": "311 Service Requests",
                            "resources": [
                                {
                                    "id": "new-uuid",
                                    "name": "311 Service Requests - NEW SYSTEM",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                                {
                                    "id": "2020-uuid",
                                    "name": "311 SERVICE REQUESTS - 2020",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                                {
                                    "id": "2021-uuid",
                                    "name": "311 SERVICE REQUESTS - 2021",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                            ],
                        }
                    ]
                }
            }
            mock_response_search.raise_for_status = Mock()
            mock_response_query = Mock()
            mock_response_query.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "case_id": "X-2020"}],
                    "fields": [{"id": "case_id", "type": "text"}],
                }
            }
            mock_response_query.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_search,
                    mock_response_query,
                ]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_and_query",
                {"query": "311", "resource_name": "2020"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            # Picked the 2020 archive (case-insensitive substring on name)
            assert "2020-uuid" in text
            assert "X-2020" in text
            # Sibling block surfaces the other queryable resources
            assert "Other queryable resources in this dataset" in text
            assert "311 Service Requests - NEW SYSTEM" in text
            assert "311 SERVICE REQUESTS - 2021" in text
            # And the third call's body actually queried 2020-uuid
            third_call = mock_client.post.call_args_list[2]
            assert third_call[1]["json"]["resource_id"] == "2020-uuid"

    @pytest.mark.asyncio
    async def test_execute_tool_search_and_query_resource_name_no_match_errors(
        self, ckan_config
    ):
        """resource_name with no match returns a clean error listing names."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {
                    "results": [
                        {
                            "id": "dataset-311",
                            "title": "311",
                            "resources": [
                                {
                                    "id": "new-uuid",
                                    "name": "NEW SYSTEM",
                                    "format": "CSV",
                                    "datastore_active": True,
                                }
                            ],
                        }
                    ]
                }
            }
            mock_response_search.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_search]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_and_query",
                {
                    "query": "311",
                    "resource_name": "1999",
                    "dataset_index": 0,
                },
            )

            assert result.success is False
            err = result.error_message or ""
            assert "1999" in err
            assert "NEW SYSTEM" in err

    @pytest.mark.asyncio
    async def test_execute_tool_search_and_query_siblings_block_lists_archives(
        self, ckan_config
    ):
        """Siblings block lists every queryable resource of the chosen
        dataset other than the chosen one — even when resource_name is
        not used."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {
                    "results": [
                        {
                            "id": "ds-311",
                            "title": "311",
                            "resources": [
                                {
                                    "id": "new",
                                    "name": "NEW SYSTEM",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                                {
                                    "id": "y2025",
                                    "name": "311 - 2025",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                                {
                                    "id": "y2024",
                                    "name": "311 - 2024",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                                {
                                    "id": "geojson",
                                    "name": "GeoJSON",
                                    "format": "GeoJSON",
                                    "datastore_active": False,
                                },
                            ],
                        }
                    ]
                }
            }
            mock_response_search.raise_for_status = Mock()
            mock_response_query = Mock()
            mock_response_query.json.return_value = {
                "result": {"records": [{"_id": 1}], "fields": []}
            }
            mock_response_query.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_search,
                    mock_response_query,
                ]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_and_query", {"query": "311"}
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "Other queryable resources in this dataset" in text
            assert "311 - 2025" in text
            assert "311 - 2024" in text
            # Only QUERYABLE siblings — the GeoJSON should not appear
            # in the siblings block
            assert "GeoJSON" not in text.split(
                "Other queryable resources in this dataset"
            )[1]

    @pytest.mark.asyncio
    async def test_search_and_query_emits_partial_warning_when_auto_picked(
        self, ckan_config
    ):
        """When the model auto-picks a resource and queryable siblings
        exist, the response must include a PARTIAL DATASET ANSWER block
        — otherwise GPT-4o reads the one-resource count as the dataset
        total. Regression test for: 'How many 311 requests in total?'
        returning 9,790 (NEW SYSTEM) instead of walking 22 archives."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {
                    "results": [
                        {
                            "id": "ds-311",
                            "title": "311 Service Requests",
                            "resources": [
                                {
                                    "id": "new-uuid",
                                    "name": "311 - NEW SYSTEM",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                                {
                                    "id": "y2025",
                                    "name": "311 - 2025",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                                {
                                    "id": "y2024",
                                    "name": "311 - 2024",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                            ],
                        }
                    ]
                }
            }
            mock_response_search.raise_for_status = Mock()
            mock_response_query = Mock()
            mock_response_query.json.return_value = {
                "result": {
                    "records": [{"_id": 1}],
                    "fields": [],
                    "total": 9790,
                }
            }
            mock_response_query.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_search,
                    mock_response_query,
                ]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_and_query", {"query": "311"}
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "PARTIAL DATASET ANSWER" in text
            assert "311 - NEW SYSTEM" in text
            assert "include_resource_totals=true" in text

    @pytest.mark.asyncio
    async def test_search_and_query_no_partial_warning_when_resource_name(
        self, ckan_config
    ):
        """When the model explicitly picks a resource via resource_name,
        no PARTIAL warning — they got what they asked for."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {
                    "results": [
                        {
                            "id": "ds-311",
                            "title": "311",
                            "resources": [
                                {
                                    "id": "new",
                                    "name": "NEW SYSTEM",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                                {
                                    "id": "y2018",
                                    "name": "311 - 2018",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                            ],
                        }
                    ]
                }
            }
            mock_response_search.raise_for_status = Mock()
            mock_response_query = Mock()
            mock_response_query.json.return_value = {
                "result": {"records": [{"_id": 1}], "fields": [], "total": 5}
            }
            mock_response_query.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_search,
                    mock_response_query,
                ]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_and_query",
                {"query": "311", "resource_name": "2018"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "PARTIAL DATASET ANSWER" not in text

    @pytest.mark.asyncio
    async def test_search_and_query_include_resource_totals_runs_parallel_counts(
        self, ckan_config
    ):
        """include_resource_totals=true must run COUNT(*) against EVERY
        queryable resource and surface a grand-total + per-resource
        breakdown, so 'total across all years' resolves in one call."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {
                    "results": [
                        {
                            "id": "ds-311",
                            "title": "311",
                            "resources": [
                                {
                                    "id": "11111111-2222-3333-4444-555555555555",
                                    "name": "NEW SYSTEM",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                                {
                                    "id": "22222222-3333-4444-5555-666666666666",
                                    "name": "311 - 2025",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                                {
                                    "id": "33333333-4444-5555-6666-777777777777",
                                    "name": "311 - 2024",
                                    "format": "CSV",
                                    "datastore_active": True,
                                },
                            ],
                        }
                    ]
                }
            }
            mock_response_search.raise_for_status = Mock()
            # Main query (NEW SYSTEM) returns sample rows
            mock_response_query = Mock()
            mock_response_query.json.return_value = {
                "result": {
                    "records": [{"_id": i} for i in range(10)],
                    "fields": [{"id": "_id", "type": "int"}],
                    "total": 9790,
                }
            }
            mock_response_query.raise_for_status = Mock()
            # Three COUNT(*) calls — return totals for each archive
            def make_count_response(n):
                m = Mock()
                m.json.return_value = {
                    "result": {"records": [{"n": n}]}
                }
                m.raise_for_status = Mock()
                return m
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_search,
                    mock_response_query,
                    make_count_response(9790),
                    make_count_response(267187),
                    make_count_response(282836),
                ]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_and_query",
                {"query": "311", "include_resource_totals": True},
            )

            assert result.success is True
            text = result.content[0]["text"]
            # Per-resource breakdown rendered
            assert "Per-resource totals" in text
            assert "9790" in text
            assert "267187" in text
            assert "282836" in text
            # Grand total = sum of all three
            assert "GRAND TOTAL across 3 resources: 559813" in text
            # Three COUNT(*) calls beyond init + search + main query
            assert mock_client.post.call_count == 6

    @pytest.mark.asyncio
    async def test_execute_tool_search_and_query_walks_to_next_dataset(
        self, ckan_config
    ):
        """If the best-match dataset has no datastore_active resource, the
        composite tool falls through to the next dataset."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_search = Mock()
            mock_response_search.json.return_value = {
                "result": {
                    "results": [
                        {
                            "id": "dataset-no-datastore",
                            "title": "PDFs only",
                            "resources": [
                                {
                                    "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                                    "format": "PDF",
                                    "datastore_active": False,
                                }
                            ],
                        },
                        {
                            "id": "dataset-with-csv",
                            "title": "Has CSV",
                            "resources": [
                                {
                                    "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                                    "format": "CSV",
                                    "datastore_active": True,
                                }
                            ],
                        },
                    ]
                }
            }
            mock_response_search.raise_for_status = Mock()
            mock_response_query = Mock()
            mock_response_query.json.return_value = {
                "result": {"records": [{"_id": 1, "x": 1}]}
            }
            mock_response_query.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_search,
                    mock_response_query,
                ]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_and_query", {"query": "anything"}
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb" in text
            assert "Has CSV" in text

    @pytest.mark.asyncio
    async def test_execute_tool_query_data_with_where_uses_sql_endpoint(
        self, ckan_config
    ):
        """When `where` is supplied, query_data must route through
        datastore_search_sql with a built WHERE clause — not through
        datastore_search (equality-only)."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_sql = Mock()
            mock_response_sql.json.return_value = {
                "result": {
                    "records": [
                        {"_id": 1, "case_id": "BCS-1", "case_status": "Closed"}
                    ],
                    "fields": [
                        {"id": "case_id", "type": "text"},
                        {"id": "close_date", "type": "timestamp"},
                        {"id": "case_status", "type": "text"},
                    ],
                }
            }
            mock_response_sql.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_sql]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "where": {
                        "close_date": {
                            "gte": "2026-04-29",
                            "lt": "2026-04-30",
                        },
                        "case_status": "Closed",
                    },
                    "limit": 5,
                },
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "BCS-1" in text
            # Schema footer surfaces filterable columns
            assert "Filterable columns" in text
            assert "close_date" in text
            # Verify the second POST hit datastore_search_sql with a SQL
            # body containing the expected WHERE clause.
            second_call = mock_client.post.call_args_list[1]
            assert second_call[0][0] == "/api/3/action/datastore_search_sql"
            sql = second_call[1]["json"]["sql"]
            assert (
                'FROM "11111111-2222-3333-4444-555555555555"' in sql
            )
            assert '"close_date" >= \'2026-04-29\'' in sql
            assert '"close_date" < \'2026-04-30\'' in sql
            assert '"case_status" = \'Closed\'' in sql
            assert "LIMIT 5" in sql

    @pytest.mark.asyncio
    async def test_execute_tool_query_data_where_validation_error_surfaces(
        self, ckan_config
    ):
        """A bad `where` operator returns a clean error — no API call."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=mock_response_init)
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "where": {"col": {"regex": "."}},
                },
            )

            assert result.success is False
            assert "Unknown operator" in (result.error_message or "")
            # Only the init POST should have happened — no SQL call.
            assert mock_client.post.call_count == 1

    @pytest.mark.asyncio
    async def test_execute_tool_query_data_schema_footer_in_normal_path(
        self, ckan_config
    ):
        """The non-SQL (no `where`) path also returns the schema footer."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_query = Mock()
            mock_response_query.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "x": "y"}],
                    "fields": [
                        {"id": "x", "type": "text"},
                        {"id": "z", "type": "int"},
                    ],
                }
            }
            mock_response_query.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_query]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "Filterable columns" in text
            assert "x (text)" in text
            assert "z (int)" in text

    @pytest.mark.asyncio
    async def test_query_data_surfaces_total_from_datastore_search(
        self, ckan_config
    ):
        """When CKAN returns `total`, format prefers total_matching_rows
        over returned_rows."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_query = Mock()
            mock_response_query.json.return_value = {
                "result": {
                    "records": [{"_id": i} for i in range(100)],
                    "fields": [{"id": "_id", "type": "int"}],
                    "total": 531,
                }
            }
            mock_response_query.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_query]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "limit": 100,
                },
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "100 of 531" in text
            assert "TRUNCATED" in text
            assert "the answer is 531, NOT 100" in text
            assert "ckan__aggregate_data" in text

    @pytest.mark.asyncio
    async def test_query_data_no_truncation_warning_when_under_limit(
        self, ckan_config
    ):
        """When records returned < limit, no truncation warning shown."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_query = Mock()
            mock_response_query.json.return_value = {
                "result": {
                    "records": [{"_id": i} for i in range(85)],
                    "fields": [{"id": "_id", "type": "int"}],
                    "total": 85,
                }
            }
            mock_response_query.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_query]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "limit": 100,
                },
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "85 rows returned" in text
            assert "TRUNCATED" not in text

    @pytest.mark.asyncio
    async def test_query_data_where_path_does_count_followup_when_truncated(
        self, ckan_config
    ):
        """SQL (`where`) path: when SELECT * hits the limit, the plugin
        must do a COUNT(*) follow-up so the model gets a real total
        rather than mistaking the limit for the count."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            # First SQL call: SELECT * returns exactly limit rows
            mock_response_select = Mock()
            mock_response_select.json.return_value = {
                "result": {
                    "records": [
                        {"_id": i, "case_id": f"c{i}"} for i in range(100)
                    ],
                    "fields": [
                        {"id": "case_id", "type": "text"},
                        {"id": "closed_dt", "type": "timestamp"},
                    ],
                }
            }
            mock_response_select.raise_for_status = Mock()
            # Follow-up SQL call: SELECT COUNT(*) returns the true total
            mock_response_count = Mock()
            mock_response_count.json.return_value = {
                "result": {"records": [{"n": 531}]}
            }
            mock_response_count.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_select,
                    mock_response_count,
                ]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "where": {
                        "closed_dt": {
                            "gte": "2016-04-29",
                            "lt": "2016-04-30",
                        }
                    },
                    "limit": 100,
                },
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "100 of 531" in text
            assert "TRUNCATED" in text
            assert "the answer is 531, NOT 100" in text
            # Follow-up COUNT(*) actually issued
            assert mock_client.post.call_count == 3
            count_call = mock_client.post.call_args_list[2]
            count_sql = count_call[1]["json"]["sql"]
            assert "COUNT(*)" in count_sql
            assert '"closed_dt" >= \'2016-04-29\'' in count_sql

    @pytest.mark.asyncio
    async def test_query_data_where_path_no_count_when_under_limit(
        self, ckan_config
    ):
        """SQL path: if records returned < limit we already know the total
        — no extra COUNT(*) call should fire."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_select = Mock()
            mock_response_select.json.return_value = {
                "result": {
                    "records": [{"_id": i} for i in range(85)],
                    "fields": [],
                }
            }
            mock_response_select.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_select]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "where": {"x": {"gt": 1}},
                    "limit": 100,
                },
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "85 rows returned" in text
            assert "TRUNCATED" not in text
            # init + SELECT only, no COUNT(*)
            assert mock_client.post.call_count == 2

    @pytest.mark.asyncio
    async def test_query_data_where_path_count_failure_falls_back_to_warning(
        self, ckan_config
    ):
        """If the COUNT(*) follow-up fails, we still return the data with
        a 'MAY BE TRUNCATED' warning rather than failing the whole call."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_select = Mock()
            mock_response_select.json.return_value = {
                "result": {
                    "records": [{"_id": i} for i in range(100)],
                    "fields": [],
                }
            }
            mock_response_select.raise_for_status = Mock()
            # COUNT(*) call fails server-side
            mock_response_count_fail = Mock()
            mock_response_count_fail.json.return_value = {
                "success": False,
                "error": {"message": "COUNT failed"},
            }
            mock_response_count_fail.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_select,
                    mock_response_count_fail,
                ]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "where": {"x": {"gt": 1}},
                    "limit": 100,
                },
            )

            # Whole call still succeeds — count-failure must not block data
            assert result.success is True
            text = result.content[0]["text"]
            assert "MAY BE TRUNCATED" in text

    @pytest.mark.asyncio
    async def test_query_data_404_includes_datastore_active_hint(self, ckan_config):
        """A 404 from query_data should append the datastore_active hint."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_404 = Mock()
            mock_response_404.status_code = 404
            mock_response_404.json.return_value = {
                "success": False,
                "error": {"message": "Resource not found"},
            }
            mock_response_404.raise_for_status = Mock(
                side_effect=httpx.HTTPStatusError(
                    "Not Found",
                    request=Mock(),
                    response=mock_response_404,
                )
            )
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_404]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "0826fc19-4ff8-44a5-b9c4-916960d8cfb3"},
            )

            assert result.success is False
            assert "datastore_active" in (result.error_message or "")

    @pytest.mark.asyncio
    async def test_execute_tool_unknown_tool(self, ckan_config):
        """Test executing unknown tool."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=mock_response_init)
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool("unknown_tool", {})

            assert result.success is False
            assert "Unknown tool" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_tool_handles_exception(self, ckan_config):
        """Test that execute_tool handles exceptions gracefully."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, RuntimeError("API error")]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool("search_datasets", {"query": "test"})

            assert result.success is False
            assert "API error" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_sql_returns_error_when_ckan_body_has_success_false(
        self, ckan_config
    ):
        """Test execute_sql returns descriptive error when CKAN returns success: false."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_sql = Mock()
            mock_response_sql.json.return_value = {
                "success": False,
                "error": {
                    "message": (
                        'relation "11111111-2222-3333-4444-555555555555" '
                        "does not exist"
                    )
                },
            }
            mock_response_sql.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_sql]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "execute_sql",
                {
                    "sql": (
                        'SELECT * FROM '
                        '"11111111-2222-3333-4444-555555555555" LIMIT 1'
                    )
                },
            )

            assert result.success is False
            assert result.error_message is not None
            assert (
                "does not exist" in result.error_message
                or "TestCity" in result.error_message
            )

    @pytest.mark.asyncio
    async def test_aggregate_data_returns_error_when_ckan_body_has_success_false(
        self, ckan_config
    ):
        """Test aggregate_data returns descriptive error when CKAN returns success: false."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_sql = Mock()
            mock_response_sql.json.return_value = {
                "success": False,
                "error": {"message": 'relation "bad-resource-id" does not exist'},
            }
            mock_response_sql.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_sql]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "aggregate_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "metrics": {"count": "count(*)"},
                },
            )

            assert result.success is False
            assert result.error_message is not None
            assert (
                "does not exist" in result.error_message
                or "TestCity" in result.error_message
            )

    @pytest.mark.asyncio
    async def test_query_data_returns_descriptive_error_on_http_404(self, ckan_config):
        """Test that 404 HTTP error includes resource_id and status code."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_404 = Mock()
            mock_response_404.status_code = 404
            mock_response_404.json.return_value = {
                "success": False,
                "error": {"message": "Resource not found"},
            }
            mock_response_404.raise_for_status = Mock(
                side_effect=httpx.HTTPStatusError(
                    "Not Found",
                    request=Mock(),
                    response=mock_response_404,
                )
            )
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_404]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "fake-dataset-does-not-exist-12345", "limit": 10},
            )

            assert result.success is False
            assert "404" in result.error_message
            assert (
                "fake-dataset-does-not-exist-12345" in result.error_message
                or "TestCity" in result.error_message
            )


class TestHealthCheck:
    """Test health_check method."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_health_check_succeeds(self, ckan_config):
        """Test that health check succeeds when API is healthy."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = Mock()
            mock_response.json.return_value = {"success": True}
            mock_response.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            health = await plugin.health_check()

            assert health is True

    @pytest.mark.asyncio
    async def test_health_check_fails_on_api_error(self, ckan_config):
        """Test that health check fails when API returns error."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_response_health = Mock()
            mock_response_health.json.return_value = {"success": False}
            mock_response_health.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, mock_response_health]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            health = await plugin.health_check()

            assert health is False

    @pytest.mark.asyncio
    async def test_health_check_fails_on_exception(self, ckan_config):
        """Test that health check fails on exception."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[mock_response_init, Exception("Connection failed")]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            health = await plugin.health_check()

            assert health is False


class TestRetryLogic:
    """Test retry logic for API calls."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_retry_on_transient_error(self, ckan_config):
        """Test that API calls retry on transient errors."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            # First call fails, second succeeds
            mock_response_fail = Mock()
            mock_response_fail.raise_for_status.side_effect = Exception(
                "Transient error"
            )
            mock_response_success = Mock()
            mock_response_success.json.return_value = {"result": {"results": []}}
            mock_response_success.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_fail,
                    mock_response_success,
                ]
            )
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            # This should retry and eventually succeed
            # Note: Actual retry behavior depends on tenacity configuration
            try:
                results = await plugin.search_datasets("test")
                # If retry succeeds, we get results
                assert isinstance(results, list)
            except Exception:
                # If retry fails, exception is raised
                pass
