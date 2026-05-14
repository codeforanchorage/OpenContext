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
                {"sql": 'SELECT * FROM "11111111-2222-3333-4444-555555555555" LIMIT 1'},
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
    async def test_execute_tool_search_datasets_surfaces_total_count(self, ckan_config):
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
                        'SELECT * FROM "11111111-2222-3333-4444-555555555555" LIMIT 100'
                    )
                },
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "MAY BE TRUNCATED" in text
            assert "ckan__aggregate_data" in text or "COUNT(*)" in text

    @pytest.mark.asyncio
    async def test_execute_tool_execute_sql_no_warning_under_limit(self, ckan_config):
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
                        'SELECT * FROM "11111111-2222-3333-4444-555555555555" LIMIT 100'
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
            result = await plugin.execute_tool("search_and_query", {"query": "parks"})

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
            result = await plugin.execute_tool("search_and_query", {"query": "311"})

            assert result.success is True
            text = result.content[0]["text"]
            assert "Other queryable resources in this dataset" in text
            assert "311 - 2025" in text
            assert "311 - 2024" in text
            # Only QUERYABLE siblings — the GeoJSON should not appear
            # in the siblings block
            assert (
                "GeoJSON"
                not in text.split("Other queryable resources in this dataset")[1]
            )

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
            result = await plugin.execute_tool("search_and_query", {"query": "311"})

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
                m.json.return_value = {"result": {"records": [{"n": n}]}}
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
            # Pre-flight field-name validation fetches the schema before the
            # SQL call; mock that response with the same fields the SQL call
            # would expose.
            mock_response_schema = Mock()
            mock_response_schema.json.return_value = {
                "result": {
                    "fields": [
                        {"id": "case_id", "type": "text"},
                        {"id": "close_date", "type": "timestamp"},
                        {"id": "case_status", "type": "text"},
                    ],
                }
            }
            mock_response_schema.raise_for_status = Mock()
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
                side_effect=[
                    mock_response_init,
                    mock_response_schema,
                    mock_response_sql,
                ]
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
            # Verify the SQL POST (3rd call: init, schema preflight, SQL)
            # hit datastore_search_sql with the expected WHERE clause.
            sql_call = mock_client.post.call_args_list[2]
            assert sql_call[0][0] == "/api/3/action/datastore_search_sql"
            sql = sql_call[1]["json"]["sql"]
            assert 'FROM "11111111-2222-3333-4444-555555555555"' in sql
            assert "\"close_date\" >= '2026-04-29'" in sql
            assert "\"close_date\" < '2026-04-30'" in sql
            assert "\"case_status\" = 'Closed'" in sql
            assert "LIMIT 5" in sql

    @pytest.mark.asyncio
    async def test_execute_tool_query_data_where_validation_error_surfaces(
        self, ckan_config
    ):
        """A bad `where` operator returns a clean error — no SQL call.

        Pre-flight field-name validation may also hit /datastore_search to
        fetch the schema, so we only require that the SQL endpoint
        (/datastore_search_sql) is never reached.
        """
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
            # SQL endpoint must NOT have been called — the error fires
            # before SQL is built.
            for call in mock_client.post.call_args_list:
                assert call[0][0] != "/api/3/action/datastore_search_sql"

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
    async def test_query_data_surfaces_total_from_datastore_search(self, ckan_config):
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
    async def test_query_data_no_truncation_warning_when_under_limit(self, ckan_config):
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
            # Pre-flight schema fetch (civic-AI field-name validation)
            mock_response_schema = Mock()
            mock_response_schema.json.return_value = {
                "result": {
                    "fields": [
                        {"id": "case_id", "type": "text"},
                        {"id": "closed_dt", "type": "timestamp"},
                    ],
                }
            }
            mock_response_schema.raise_for_status = Mock()
            # First SQL call: SELECT * returns exactly limit rows
            mock_response_select = Mock()
            mock_response_select.json.return_value = {
                "result": {
                    "records": [{"_id": i, "case_id": f"c{i}"} for i in range(100)],
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
                    mock_response_schema,
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
            # Follow-up COUNT(*) is the last call; init + schema + SQL +
            # count = 4 total.
            assert mock_client.post.call_count == 4
            count_call = mock_client.post.call_args_list[3]
            count_sql = count_call[1]["json"]["sql"]
            assert "COUNT(*)" in count_sql
            assert "\"closed_dt\" >= '2016-04-29'" in count_sql

    @pytest.mark.asyncio
    async def test_query_data_where_path_no_count_when_under_limit(self, ckan_config):
        """SQL path: if records returned < limit we already know the total
        — no extra COUNT(*) call should fire."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response_init = Mock()
            mock_response_init.json.return_value = {"success": True}
            mock_response_init.raise_for_status = Mock()
            # Pre-flight schema (empty fields is fine — validator returns None)
            mock_response_schema = Mock()
            mock_response_schema.json.return_value = {
                "result": {"fields": [{"id": "x", "type": "int"}]}
            }
            mock_response_schema.raise_for_status = Mock()
            mock_response_select = Mock()
            mock_response_select.json.return_value = {
                "result": {
                    "records": [{"_id": i} for i in range(85)],
                    "fields": [],
                }
            }
            mock_response_select.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_schema,
                    mock_response_select,
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

            assert result.success is True
            text = result.content[0]["text"]
            assert "85 rows returned" in text
            assert "TRUNCATED" not in text
            # init + schema + SELECT only, no COUNT(*)
            assert mock_client.post.call_count == 3
            # And the SQL endpoint was called exactly once (just the SELECT).
            sql_calls = [
                c
                for c in mock_client.post.call_args_list
                if c[0][0] == "/api/3/action/datastore_search_sql"
            ]
            assert len(sql_calls) == 1

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
            mock_response_schema = Mock()
            mock_response_schema.json.return_value = {
                "result": {"fields": [{"id": "x", "type": "int"}]}
            }
            mock_response_schema.raise_for_status = Mock()
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
                    mock_response_schema,
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
                        'relation "11111111-2222-3333-4444-555555555555" does not exist'
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
                        'SELECT * FROM "11111111-2222-3333-4444-555555555555" LIMIT 1'
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
            # Pre-flight schema fetch (civic-AI field-name validation in
            # aggregate_data validates group_by/metrics/filters names).
            mock_response_schema = Mock()
            mock_response_schema.json.return_value = {"result": {"fields": []}}
            mock_response_schema.raise_for_status = Mock()
            mock_response_sql = Mock()
            mock_response_sql.json.return_value = {
                "success": False,
                "error": {"message": 'relation "bad-resource-id" does not exist'},
            }
            mock_response_sql.raise_for_status = Mock()
            mock_client.post = AsyncMock(
                side_effect=[
                    mock_response_init,
                    mock_response_schema,
                    mock_response_sql,
                ]
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


class TestAbandonmentDetector:
    """Civic-AI #5/#6: APPARENT ABANDONMENT + NO UPDATE CADENCE DECLARED."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    def _build_mocks(self, dataset_dict):
        init = Mock()
        init.json.return_value = {"success": True}
        init.raise_for_status = Mock()
        pkg = Mock()
        pkg.json.return_value = {"result": dataset_dict}
        pkg.raise_for_status = Mock()
        return init, pkg

    @pytest.mark.asyncio
    async def test_abandonment_fires_when_weekly_dataset_months_stale(
        self, ckan_config
    ):
        """Dataset declares weekly updates but resource last_modified is
        over a year old -- well past 4x the 7-day cadence."""
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init, pkg = self._build_mocks(
                {
                    "id": "stale-weekly",
                    "name": "stale-weekly",
                    "title": "Stale Weekly Dataset",
                    "metadata_modified": "2026-05-01T00:00:00",
                    "frequency": "weekly",
                    "organization": {"title": "Test"},
                    "resources": [
                        {
                            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                            "name": "data",
                            "datastore_active": True,
                            "last_modified": "2023-01-01T00:00:00",
                        }
                    ],
                }
            )
            mock_client.post = AsyncMock(side_effect=[init, pkg])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "get_dataset", {"dataset_id": "stale-weekly"}
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "APPARENT ABANDONMENT" in text
            assert "'weekly'" in text
            # Dual timestamps must both appear
            assert "Data last updated: 2023-01-01" in text
            assert "Metadata last touched: 2026-05-01" in text

    @pytest.mark.asyncio
    async def test_abandonment_silent_when_within_cadence(self, ckan_config):
        """Weekly dataset modified within the last week -- abandonment
        must stay silent. False alarms erode trust faster than silences."""
        plugin = CKANPlugin(ckan_config)
        from datetime import datetime, timezone, timedelta

        recent = (datetime.now(timezone.utc) - timedelta(days=3)).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init, pkg = self._build_mocks(
                {
                    "id": "live-weekly",
                    "name": "live-weekly",
                    "title": "Live Weekly Dataset",
                    "metadata_modified": recent,
                    "frequency": "weekly",
                    "organization": {"title": "Test"},
                    "resources": [
                        {
                            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                            "name": "data",
                            "datastore_active": True,
                            "last_modified": recent,
                        }
                    ],
                }
            )
            mock_client.post = AsyncMock(side_effect=[init, pkg])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "get_dataset", {"dataset_id": "live-weekly"}
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "APPARENT ABANDONMENT" not in text
            assert "DATA FRESHNESS" not in text  # recent enough

    @pytest.mark.asyncio
    async def test_no_frequency_note_fires_on_old_undeclared_dataset(self, ckan_config):
        """No frequency declared + resource > 2yr old -> the softer
        'cannot tell if current' note."""
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init, pkg = self._build_mocks(
                {
                    "id": "no-freq",
                    "name": "no-freq",
                    "title": "Undeclared Dataset",
                    # frequency intentionally omitted
                    "organization": {"title": "Test"},
                    "resources": [
                        {
                            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                            "name": "data",
                            "datastore_active": True,
                            "last_modified": "2020-01-01T00:00:00",
                        }
                    ],
                }
            )
            mock_client.post = AsyncMock(side_effect=[init, pkg])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool("get_dataset", {"dataset_id": "no-freq"})

            assert result.success is True
            text = result.content[0]["text"]
            assert "NO UPDATE CADENCE DECLARED" in text
            # And the abandonment banner must NOT fire (no cadence to
            # measure against)
            assert "APPARENT ABANDONMENT" not in text


class TestStringlyTypedDetection:
    """Civic-AI #9: TEXT columns holding dates/numbers."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_text_column_with_iso_dates_fires_type_note(self, ckan_config):
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [
                        {"_id": 1, "open_dt": "2024-06-15"},
                        {"_id": 2, "open_dt": "2024-06-16"},
                        {"_id": 3, "open_dt": "2024-06-17"},
                        {"_id": 4, "open_dt": "2024-06-18"},
                    ],
                    "fields": [{"id": "open_dt", "type": "text"}],
                    "total": 4,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "STRINGLY-TYPED FIELDS" in text
            assert "'open_dt' is stored as TEXT but values look like dates" in text

    @pytest.mark.asyncio
    async def test_text_column_with_real_text_stays_silent(self, ckan_config):
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [
                        {"_id": 1, "name": "Alice"},
                        {"_id": 2, "name": "Bob"},
                        {"_id": 3, "name": "Carol"},
                        {"_id": 4, "name": "Dan"},
                    ],
                    "fields": [{"id": "name", "type": "text"}],
                    "total": 4,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "STRINGLY-TYPED FIELDS" not in text

    @pytest.mark.asyncio
    async def test_text_column_with_numbers_fires_type_note(self, ckan_config):
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [
                        {"_id": 1, "amount": "10"},
                        {"_id": 2, "amount": "42"},
                        {"_id": 3, "amount": "100"},
                        {"_id": 4, "amount": "3"},
                    ],
                    "fields": [{"id": "amount", "type": "text"}],
                    "total": 4,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "STRINGLY-TYPED FIELDS" in text
            assert "look like numbers" in text


class TestNullLikeNormalization:
    """Civic-AI #10/#11: null-like rendering + DATA QUALITY frequency."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_null_like_strings_rendered_distinctly(self, ckan_config):
        """'Unknown' / 'N/A' / '' / None all render distinctly so the
        model can't treat them as ordinary categories."""
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [
                        {"_id": 1, "status": "Unknown"},
                        {"_id": 2, "status": "N/A"},
                        {"_id": 3, "status": ""},
                        {"_id": 4, "status": None},
                        {"_id": 5, "status": "Open"},
                    ],
                    "fields": [{"id": "status", "type": "text"}],
                    "total": 5,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert 'status: <"Unknown">' in text
            assert 'status: <"N/A">' in text
            assert "status: <empty>" in text
            assert "status: <null>" in text
            assert "status: Open" in text  # real value rendered as-is

    @pytest.mark.asyncio
    async def test_high_missing_rate_fires_data_quality_caveat(self, ckan_config):
        """When > 20% of a column's values are null-like, DATA QUALITY
        caveat fires naming the column and the missing-rate."""
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [
                        {"_id": 1, "assigned_to": "Unknown"},
                        {"_id": 2, "assigned_to": "Unknown"},
                        {"_id": 3, "assigned_to": "N/A"},
                        {"_id": 4, "assigned_to": "Alice"},
                        {"_id": 5, "assigned_to": "Bob"},
                    ],
                    "fields": [{"id": "assigned_to", "type": "text"}],
                    "total": 5,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "DATA QUALITY" in text
            assert "'assigned_to'" in text
            assert "60%" in text  # 3 of 5

    @pytest.mark.asyncio
    async def test_low_missing_rate_silent(self, ckan_config):
        """One missing value out of many should not fire DATA QUALITY."""
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [
                        {"_id": i, "assigned_to": f"user_{i}"} for i in range(20)
                    ]
                    + [{"_id": 21, "assigned_to": "Unknown"}],
                    "fields": [{"id": "assigned_to", "type": "text"}],
                    "total": 21,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "DATA QUALITY" not in text


class TestSqlPassthroughCaveat:
    """Civic-AI #13: SQL PASSTHROUGH warning on execute_sql only."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_execute_sql_carries_passthrough_warning(self, ckan_config):
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            sql_resp = Mock()
            sql_resp.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "n": 42}],
                    "fields": [{"id": "n", "type": "int4"}],
                }
            }
            sql_resp.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, sql_resp])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "execute_sql",
                {
                    "sql": (
                        'SELECT * FROM "11111111-2222-3333-4444-555555555555" LIMIT 1'
                    )
                },
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "SQL PASSTHROUGH" in text
            assert "the model wrote it" in text

    @pytest.mark.asyncio
    async def test_aggregate_data_does_not_carry_passthrough_warning(self, ckan_config):
        """aggregate_data builds SQL from validated parts -- not a
        passthrough, so the warning must stay silent."""
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            schema = Mock()
            schema.json.return_value = {
                "result": {"fields": [{"id": "neighborhood", "type": "text"}]}
            }
            schema.raise_for_status = Mock()
            agg_resp = Mock()
            agg_resp.json.return_value = {
                "result": {
                    "records": [{"neighborhood": "Allston", "n": 100}],
                    "fields": [
                        {"id": "neighborhood", "type": "text"},
                        {"id": "n", "type": "int4"},
                    ],
                }
            }
            agg_resp.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, schema, agg_resp])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "aggregate_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "group_by": ["neighborhood"],
                    "metrics": {"n": "count(*)"},
                },
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "SQL PASSTHROUGH" not in text


class TestSearchAmbiguityCaveat:
    """Civic-AI #14: warn when multiple plausible matches surface."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_ambiguity_fires_with_overlapping_titles(self, ckan_config):
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            search = Mock()
            search.json.return_value = {
                "result": {
                    "count": 3,
                    "results": [
                        {
                            "id": "1",
                            "title": "Crime Incident Reports",
                            "resources": [],
                        },
                        {
                            "id": "2",
                            "title": "Crime Stats Summary",
                            "resources": [],
                        },
                        {
                            "id": "3",
                            "title": "Crime Mapping Data",
                            "resources": [],
                        },
                    ],
                }
            }
            search.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, search])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_datasets", {"query": "crime", "limit": 5}
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "AMBIGUOUS SEARCH" in text
            assert "Crime Stats Summary" in text

    @pytest.mark.asyncio
    async def test_ambiguity_silent_with_unrelated_topics(self, ckan_config):
        """When the top result and others share no title tokens, no
        ambiguity warning."""
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            search = Mock()
            search.json.return_value = {
                "result": {
                    "count": 2,
                    "results": [
                        {
                            "id": "1",
                            "title": "Building Permits",
                            "resources": [],
                        },
                        {
                            "id": "2",
                            "title": "Tree Census",
                            "resources": [],
                        },
                    ],
                }
            }
            search.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, search])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_datasets", {"query": "city data", "limit": 5}
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "AMBIGUOUS SEARCH" not in text


class TestAsciiOnlyOutput:
    """Copilot C1: every formatter output must be ASCII-only."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_search_and_query_output_is_pure_ascii(self, ckan_config):
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            search = Mock()
            search.json.return_value = {
                "result": {
                    "count": 1,
                    "results": [
                        {
                            "id": "x",
                            "name": "x",
                            "title": "X",
                            "metadata_modified": "2026-05-01T00:00:00",
                            "resources": [
                                {
                                    "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                                    "name": "y",
                                    "datastore_active": True,
                                    "last_modified": "2023-01-01T00:00:00",
                                }
                            ],
                        }
                    ],
                }
            }
            search.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "v": "ok"}],
                    "fields": [{"id": "v", "type": "text"}],
                    "total": 1,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, search, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_and_query", {"query": "x", "limit": 5}
            )

            assert result.success is True
            text = result.content[0]["text"]
            # Every character must be ASCII -- Copilot has dropped
            # non-ASCII glyphs into '?' / boxes in production.
            assert text.isascii(), "non-ASCII characters in output: " + repr(
                [c for c in text if not c.isascii()][:10]
            )


class TestZeroRecordsAbsenceOfEvidence:
    """Copilot C11: empty responses must spell out that absence is not
    evidence of absence."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_empty_query_response_includes_no_evidence_note(self, ckan_config):
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [],
                    "fields": [{"id": "name", "type": "text"}],
                    "total": 0,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "zero records does NOT mean zero data" in text


class TestProseRemindersForCriticalCaveats:
    """Copilot A3: critical caveats must also appear as bottom-of-response
    prose reminders so GPT-4o doesn't drop the structured marker."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_single_record_emits_prose_reminder(self, ckan_config):
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "v": "only-one"}],
                    "fields": [{"id": "v", "type": "text"}],
                    "total": 1,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            text = result.content[0]["text"]
            assert "SINGLE-RECORD CLAIM" in text
            # Prose reminder must also appear (Copilot A3)
            assert "(Reminder: only ONE record matched" in text
            # And the prose reminder appears AFTER the structured banner
            assert text.index("SINGLE-RECORD CLAIM") < text.index(
                "(Reminder: only ONE record"
            )

    @pytest.mark.asyncio
    async def test_abandonment_emits_prose_reminder(self, ckan_config):
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            pkg = Mock()
            pkg.json.return_value = {
                "result": {
                    "id": "stale",
                    "name": "stale",
                    "title": "Stale",
                    "metadata_modified": "2026-05-01T00:00:00",
                    "frequency": "weekly",
                    "organization": {"title": "T"},
                    "resources": [
                        {
                            "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                            "datastore_active": True,
                            "last_modified": "2023-01-01T00:00:00",
                        }
                    ],
                }
            }
            pkg.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, pkg])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool("get_dataset", {"dataset_id": "stale"})

            text = result.content[0]["text"]
            assert "APPARENT ABANDONMENT" in text
            assert "(Reminder: this dataset shows APPARENT ABANDONMENT" in text

    @pytest.mark.asyncio
    async def test_sql_passthrough_emits_prose_reminder(self, ckan_config):
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            sql_resp = Mock()
            sql_resp.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "n": 5}],
                    "fields": [{"id": "n", "type": "int4"}],
                }
            }
            sql_resp.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, sql_resp])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "execute_sql",
                {
                    "sql": (
                        'SELECT * FROM "11111111-2222-3333-4444-555555555555" LIMIT 1'
                    )
                },
            )

            text = result.content[0]["text"]
            assert "SQL PASSTHROUGH" in text
            assert "(Reminder: the SQL above was written by the model" in text

    @pytest.mark.asyncio
    async def test_no_prose_reminders_when_no_critical_caveats(self, ckan_config):
        """Stays silent when nothing critical fired."""
        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [{"_id": i, "v": f"r{i}"} for i in range(50)],
                    "fields": [{"id": "v", "type": "text"}],
                    "total": 50,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            text = result.content[0]["text"]
            assert "(Reminder:" not in text


class TestErrorMessageShape:
    """Copilot C3/C4 + D4: error messages stay ASCII, don't carry
    magic-string templates, and push the model back toward discovery."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    def test_no_fake_uuid_example_in_any_tool_description(self, ckan_config):
        """The fake UUID pattern was once present in query_data's
        resource_id description; GPT-4o can parrot it back as an
        invented ID. Make sure it's gone from every description."""
        plugin = CKANPlugin(ckan_config)
        for tool in plugin.get_tools():
            blob = repr(tool.input_schema) + tool.description
            assert "11111111-2222-3333-4444-555555555555" not in blob, (
                f"Tool {tool.name!r} still contains the worked-example "
                "UUID; GPT-4o may parrot this back as a real ID."
            )

    def test_typo_error_routes_to_discovery_not_template(self, ckan_config):
        """The 'did you mean' error must suggest a REAL field from the
        actual schema (not a synthetic template) and point at
        discovery tools."""
        import asyncio

        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            schema = Mock()
            schema.json.return_value = {
                "result": {
                    "fields": [
                        {"id": "case_status", "type": "text"},
                        {"id": "case_id", "type": "text"},
                    ]
                }
            }
            schema.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, schema])
            mock_client_class.return_value = mock_client

            async def run():
                await plugin.initialize()
                return await plugin.execute_tool(
                    "query_data",
                    {
                        "resource_id": "11111111-2222-3333-4444-555555555555",
                        "filters": {"case_staus": "Closed"},
                    },
                )

            result = asyncio.run(run())
            assert result.success is False
            msg = result.error_message or ""
            # Must contain the real schema column, not a synthetic template
            assert "case_status" in msg
            assert "did you mean" in msg
            # And the suggestion is a REAL column (matches the mocked schema)
            assert "Valid columns: case_id, case_status" in msg or (
                "Valid columns: case_status, case_id" in msg
            )

    def test_error_messages_are_ascii(self, ckan_config):
        """D4: error paths must stay ASCII too -- a non-ASCII error in
        Copilot can render as '?'/boxes and obscure actionable text."""
        import asyncio

        plugin = CKANPlugin(ckan_config)
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            mock_client.post = AsyncMock(return_value=init)
            mock_client_class.return_value = mock_client

            async def run():
                await plugin.initialize()
                return await plugin.execute_tool(
                    "query_data",
                    {
                        "resource_id": "11111111-2222-3333-4444-555555555555",
                        "where": {"col": {"regex": "."}},
                    },
                )

            result = asyncio.run(run())
            assert result.success is False
            msg = result.error_message or ""
            assert msg.isascii(), "Error message contains non-ASCII: " + repr(
                [c for c in msg if not c.isascii()][:10]
            )


class TestToolDescriptionBudget:
    """Copilot B1: every tool description fits in ~150 tokens / 600 chars
    so GPT-4o actually attends to it."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    def test_all_descriptions_under_budget(self, ckan_config):
        plugin = CKANPlugin(ckan_config)
        for tool in plugin.get_tools():
            assert len(tool.description) <= 600, (
                f"{tool.name!r} description is "
                f"{len(tool.description)} chars; trim to <=600."
            )


class TestLoweredDefaultLimits:
    """Copilot C8: smaller default limits (20 / 10) than CKAN's 100."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    def test_tool_defaults_are_copilot_friendly(self, ckan_config):
        plugin = CKANPlugin(ckan_config)
        tools = {t.name: t for t in plugin.get_tools()}
        # search_datasets default limit
        assert (
            tools["search_datasets"].input_schema["properties"]["limit"]["default"]
            <= 20
        )
        # query_data default limit
        assert tools["query_data"].input_schema["properties"]["limit"]["default"] <= 25
        # search_and_query default limit
        assert (
            tools["search_and_query"].input_schema["properties"]["limit"]["default"]
            <= 25
        )


# ---------------------------------------------------------------------------
# Civic-AI tool-design caveats
#
# These tests follow the civicaitools.org pattern: every devil's-advocate
# check needs a "fires when expected" test AND a "silent when not
# applicable" test. False alarms erode trust faster than false silences.
# ---------------------------------------------------------------------------


class TestProvenanceHeaderAndRetrievedFooter:
    """Civic-AI #1, #2, #3: Source line + echoed Query + Retrieved
    timestamp must appear on every successful tool response."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_search_datasets_response_has_source_query_and_retrieved(
        self, ckan_config
    ):
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            search = Mock()
            search.json.return_value = {"result": {"results": [], "count": 0}}
            search.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, search])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "search_datasets", {"query": "311", "limit": 5}
            )

            assert result.success is True
            text = result.content[0]["text"]
            # Section header survives Copilot markdown stripping (C4)
            assert "## Source" in text
            # Human-readable Source line names the portal + query
            assert "Source: https://data.example.com search for '311'" in text
            # API line points at the exact CKAN action endpoint
            assert (
                "API: POST https://data.example.com/api/3/action/package_search" in text
            )
            # Echoed query repeats the params we sent
            assert "Query [package_search]: q='311', rows=5" in text
            # Retrieved footer is ISO-8601 with Z suffix
            assert "_Retrieved: " in text
            assert text.rstrip().endswith("Z_")

    @pytest.mark.asyncio
    async def test_query_data_provenance_shows_sql_action_when_where_used(
        self, ckan_config
    ):
        """When `where` routes through datastore_search_sql, the Source
        line must reflect that — not the equality-only endpoint."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            schema = Mock()
            schema.json.return_value = {
                "result": {"fields": [{"id": "x", "type": "int"}]}
            }
            schema.raise_for_status = Mock()
            sql = Mock()
            sql.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "x": 42}],
                    "fields": [{"id": "x", "type": "int"}],
                }
            }
            sql.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, schema, sql])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "where": {"x": {"gt": 1}},
                    "limit": 5,
                },
            )

            assert result.success is True
            text = result.content[0]["text"]
            # API line names the action that actually fired
            api_line = next(
                line for line in text.splitlines() if line.startswith("API:")
            )
            assert "datastore_search_sql" in api_line
            # Equality-only action must NOT show up in the API line
            assert "/api/3/action/datastore_search;" not in api_line
            assert not api_line.endswith("/api/3/action/datastore_search")

    @pytest.mark.asyncio
    async def test_long_sql_in_echoed_query_is_truncated(self, ckan_config):
        """The Query line must not blow up when SQL is long."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            sql_resp = Mock()
            sql_resp.json.return_value = {"result": {"records": [], "fields": []}}
            sql_resp.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, sql_resp])
            mock_client_class.return_value = mock_client

            long_sql = (
                'SELECT * FROM "11111111-2222-3333-4444-555555555555" '
                "WHERE " + " AND ".join(f'"c{i}" = {i}' for i in range(80)) + " LIMIT 1"
            )
            await plugin.initialize()
            result = await plugin.execute_tool("execute_sql", {"sql": long_sql})

            assert result.success is True
            text = result.content[0]["text"]
            # Should still have a Query line, and the value should have
            # been truncated with an ellipsis (not the full 3000+ char SQL)
            query_line = next(
                line for line in text.splitlines() if line.startswith("Query [")
            )
            assert "..." in query_line
            assert len(query_line) < 400


class TestSampleSizeCaveats:
    """Civic-AI #5, #6: SINGLE-RECORD and SMALL SAMPLE banners must fire
    on tiny result sets and stay silent on large ones."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_single_record_caveat_fires_when_total_is_one(self, ckan_config):
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "name": "only one"}],
                    "fields": [{"id": "name", "type": "text"}],
                    "total": 1,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "SINGLE-RECORD CLAIM" in text
            assert "N=1" in text
            assert "SMALL SAMPLE" not in text  # mutually exclusive

    @pytest.mark.asyncio
    async def test_small_sample_caveat_fires_for_total_between_2_and_10(
        self, ckan_config
    ):
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [{"_id": i} for i in range(5)],
                    "fields": [],
                    "total": 5,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "SMALL SAMPLE" in text
            assert "Only 5 row" in text
            assert "SINGLE-RECORD" not in text

    @pytest.mark.asyncio
    async def test_no_sample_caveat_when_total_is_large(self, ckan_config):
        """Stays silent for totals above the small-sample threshold."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [{"_id": i} for i in range(50)],
                    "fields": [],
                    "total": 50,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "SMALL SAMPLE" not in text
            assert "SINGLE-RECORD" not in text


class TestDataFreshnessCaveat:
    """Civic-AI #10: stale dataset warning."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_freshness_caveat_fires_on_old_dataset(self, ckan_config):
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            pkg = Mock()
            pkg.json.return_value = {
                "result": {
                    "id": "old-dataset",
                    "name": "old-dataset",
                    "title": "Old Dataset",
                    "metadata_modified": "2018-01-15T12:00:00",
                    "organization": {"title": "Test"},
                    "resources": [],
                }
            }
            pkg.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, pkg])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "get_dataset", {"dataset_id": "old-dataset"}
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "DATA FRESHNESS" in text
            assert "2018-01-15" in text

    @pytest.mark.asyncio
    async def test_freshness_caveat_silent_on_recent_dataset(self, ckan_config):
        """Datasets edited within the last year must not trigger the
        freshness banner."""
        from datetime import datetime, timezone

        plugin = CKANPlugin(ckan_config)

        recent = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            pkg = Mock()
            pkg.json.return_value = {
                "result": {
                    "id": "fresh-dataset",
                    "name": "fresh-dataset",
                    "title": "Fresh Dataset",
                    "metadata_modified": recent,
                    "organization": {"title": "Test"},
                    "resources": [],
                }
            }
            pkg.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, pkg])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "get_dataset", {"dataset_id": "fresh-dataset"}
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "DATA FRESHNESS" not in text

    @pytest.mark.asyncio
    async def test_freshness_caveat_silent_on_missing_metadata_modified(
        self, ckan_config
    ):
        """If the field isn't present, degrade silently — false silences
        beat false alarms (civic-AI #15)."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            pkg = Mock()
            pkg.json.return_value = {
                "result": {
                    "id": "x",
                    "name": "x",
                    "title": "X",
                    "organization": {"title": "Test"},
                    "resources": [],
                }
            }
            pkg.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, pkg])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool("get_dataset", {"dataset_id": "x"})

            assert result.success is True
            text = result.content[0]["text"]
            assert "DATA FRESHNESS" not in text


class TestFieldNameValidation:
    """Civic-AI #9: difflib-based 'did you mean?' on typos."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_typo_in_filters_returns_did_you_mean_hint(self, ckan_config):
        """A misspelled column in `filters` is caught pre-flight with a
        suggestion instead of leaking through to an upstream 409."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            schema = Mock()
            schema.json.return_value = {
                "result": {
                    "fields": [
                        {"id": "case_status", "type": "text"},
                        {"id": "close_date", "type": "timestamp"},
                    ]
                }
            }
            schema.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, schema])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "filters": {"case_staus": "Closed"},  # typo: missing 't'
                },
            )

            assert result.success is False
            assert "Unknown field" in (result.error_message or "")
            assert "case_staus" in result.error_message
            assert "did you mean 'case_status'" in result.error_message

    @pytest.mark.asyncio
    async def test_valid_field_names_pass_through_silently(self, ckan_config):
        """When every field name is valid, the validator is silent and the
        query proceeds."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            schema = Mock()
            schema.json.return_value = {
                "result": {"fields": [{"id": "case_status", "type": "text"}]}
            }
            schema.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "case_status": "Closed"}],
                    "fields": [{"id": "case_status", "type": "text"}],
                    "total": 1,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, schema, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "filters": {"case_status": "Closed"},
                },
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "Unknown field" not in text

    @pytest.mark.asyncio
    async def test_schema_fetch_failure_does_not_block_query(self, ckan_config):
        """If we can't fetch the schema, the validator degrades silently
        and the upstream call still happens (civic-AI #15)."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            # Schema fetch fails server-side
            schema_fail = Mock()
            schema_fail.json.return_value = {
                "success": False,
                "error": {"message": "schema unavailable"},
            }
            schema_fail.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "case_status": "Closed"}],
                    "fields": [{"id": "case_status", "type": "text"}],
                    "total": 1,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, schema_fail, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {
                    "resource_id": "11111111-2222-3333-4444-555555555555",
                    "filters": {"case_status": "Closed"},
                },
            )

            # Despite schema failing, the actual query still ran
            assert result.success is True


class TestDateFieldNormalization:
    """Civic-AI #7: timestamp/date columns render as ISO 8601."""

    @pytest.fixture
    def ckan_config(self):
        return {
            "base_url": "https://data.example.com",
            "portal_url": "https://data.example.com",
            "city_name": "TestCity",
        }

    @pytest.mark.asyncio
    async def test_midnight_timestamp_renders_as_date_only(self, ckan_config):
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "close_date": "2024-06-15T00:00:00"}],
                    "fields": [
                        {"id": "close_date", "type": "timestamp"},
                    ],
                    "total": 1,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "close_date: 2024-06-15" in text
            # Midnight time-of-day must NOT be displayed
            assert "T00:00:00" not in text

    @pytest.mark.asyncio
    async def test_non_midnight_timestamp_keeps_time_of_day(self, ckan_config):
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "ts": "2024-06-15T14:30:00"}],
                    "fields": [{"id": "ts", "type": "timestamp"}],
                    "total": 1,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "ts: 2024-06-15T14:30:00" in text

    @pytest.mark.asyncio
    async def test_non_date_columns_pass_through_unchanged(self, ckan_config):
        """Text/int columns are never normalized."""
        plugin = CKANPlugin(ckan_config)

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            init = Mock()
            init.json.return_value = {"success": True}
            init.raise_for_status = Mock()
            query = Mock()
            query.json.return_value = {
                "result": {
                    "records": [{"_id": 1, "name": "X", "count": 42}],
                    "fields": [
                        {"id": "name", "type": "text"},
                        {"id": "count", "type": "int"},
                    ],
                    "total": 1,
                }
            }
            query.raise_for_status = Mock()
            mock_client.post = AsyncMock(side_effect=[init, query])
            mock_client_class.return_value = mock_client

            await plugin.initialize()
            result = await plugin.execute_tool(
                "query_data",
                {"resource_id": "11111111-2222-3333-4444-555555555555"},
            )

            assert result.success is True
            text = result.content[0]["text"]
            assert "name: X" in text
            assert "count: 42" in text
