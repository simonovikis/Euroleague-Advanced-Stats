"""
test_db_connection.py — Database Connectivity Tests
=====================================================
Validates that the configured database host is reachable before
SQLAlchemy attempts to open a connection.

Run:
    pytest tests/test_db_connection.py -v
"""

import os
import socket
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add project root to path for direct script execution
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_pipeline.load_to_db import get_engine


class TestDatabaseConnectivity:
    """Verify the database host resolves and the port is reachable."""

    def _parse_host_port(self):
        """Extract host and port from the engine URL."""
        engine = get_engine()
        url = str(engine.url)
        # url format: postgresql+psycopg2://user:pass@host:port/db
        after_at = url.split("@")[-1]  # host:port/db
        host_port = after_at.split("/")[0]  # host:port
        if ":" in host_port:
            host, port = host_port.rsplit(":", 1)
            return host, int(port)
        return host_port, 5432

    def test_db_host_dns_resolves(self):
        """The configured database hostname must resolve via DNS."""
        host, _ = self._parse_host_port()
        try:
            socket.getaddrinfo(host, None)
        except socket.gaierror:
            pytest.fail(
                f"DNS resolution failed for database host '{host}'. "
                "Check DATABASE_URL or POSTGRES_HOST in your .env file."
            )

    def test_db_port_reachable(self):
        """The configured database port must accept TCP connections."""
        host, port = self._parse_host_port()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect((host, port))
        except (socket.timeout, ConnectionRefusedError, OSError) as exc:
            pytest.fail(
                f"Cannot reach {host}:{port} — {exc}. "
                "Is the database server running?"
            )
        finally:
            sock.close()

    def test_sqlalchemy_engine_connects(self):
        """SQLAlchemy engine.connect() must succeed."""
        from sqlalchemy import text
        engine = get_engine()
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as exc:
            pytest.fail(f"SQLAlchemy connection failed: {exc}")


class TestGetEngineURLParsing:
    """Verify get_engine builds the correct URL from env vars."""

    @patch.dict(os.environ, {
        "DATABASE_URL": "postgresql://user:pass@myhost.example.com:5432/mydb",
    })
    def test_database_url_preferred(self):
        engine = get_engine()
        url = str(engine.url)
        assert "myhost.example.com" in url

    @patch.dict(os.environ, {
        "DATABASE_URL": "",
        "POSTGRES_URL": "",
        "POSTGRES_HOST": "custom-host",
        "POSTGRES_PORT": "6543",
        "POSTGRES_USER": "testuser",
        "POSTGRES_PASSWORD": "testpass",
        "POSTGRES_DB": "testdb",
    })
    def test_falls_back_to_individual_vars(self):
        engine = get_engine()
        url = str(engine.url)
        assert "custom-host" in url
        assert "6543" in url

    @patch.dict(os.environ, {
        "DATABASE_URL": "postgres://user:pass@host:5432/db",
    })
    def test_postgres_scheme_rewritten_to_psycopg2(self):
        engine = get_engine()
        assert "psycopg2" in str(engine.url)
