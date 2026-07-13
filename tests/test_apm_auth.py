"""Tests for Elastic APM auth resolution."""

from __future__ import annotations

import os
import unittest
from multiprocessing import get_context

from frosty.api.apm import apm_client_env, apm_config_from_env, create_apm_client


def _spawn_worker_auth_state() -> tuple[bool, bool]:
    from frosty.tracing import init_apm_worker

    init_apm_worker()
    try:
        import elasticapm
    except ImportError:
        return False, False
    client = elasticapm.get_client()
    if client is None:
        return False, False
    return True, bool((client.config.api_key or "").strip())


class ApmAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = os.environ.copy()

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)
        try:
            import elasticapm
            import elasticapm.base as apm_base

            client = elasticapm.get_client()
            if client is not None:
                client.close()
            apm_base.CLIENT_SINGLETON = None
        except ImportError:
            pass

    def test_apm_config_does_not_use_elastic_api_key(self) -> None:
        os.environ["ELASTIC_APM_SERVER_URL"] = "https://apm.example.io"
        os.environ.pop("ELASTIC_APM_API_KEY", None)
        os.environ["ELASTIC_API_KEY"] = "id:secret"
        self.assertIsNone(apm_config_from_env())

    def test_apm_config_uses_dedicated_agent_key(self) -> None:
        os.environ["ELASTIC_APM_SERVER_URL"] = "https://apm.example.io"
        os.environ["ELASTIC_APM_API_KEY"] = "apm-agent-key"
        config = apm_config_from_env()
        assert config is not None
        self.assertEqual(config["API_KEY"], "apm-agent-key")

    def test_apm_client_env_injects_resolved_key_when_env_blank(self) -> None:
        os.environ.pop("ELASTIC_APM_API_KEY", None)
        config = {
            "API_KEY": "apm-agent-key",
            "SERVER_URL": "https://apm.example.io",
        }
        env = apm_client_env(config)
        self.assertEqual(env["ELASTIC_APM_API_KEY"], "apm-agent-key")

    def test_blank_elastic_apm_api_key_env_does_not_clear_client_auth(self) -> None:
        os.environ["ELASTIC_APM_SERVER_URL"] = "https://apm.example.io"
        os.environ["ELASTIC_APM_API_KEY"] = "apm-agent-key"
        os.environ["ELASTIC_APM_DISABLE_SEND"] = "true"
        client = create_apm_client()
        assert client is not None
        self.assertEqual(client.config.api_key, "apm-agent-key")

    def test_create_apm_client_refreshes_unauthenticated_singleton(self) -> None:
        os.environ["ELASTIC_APM_SERVER_URL"] = "https://apm.example.io"
        os.environ.pop("ELASTIC_APM_API_KEY", None)
        os.environ["ELASTIC_APM_DISABLE_SEND"] = "true"

        from elasticapm.base import Client
        import elasticapm.base as apm_base

        apm_base.CLIENT_SINGLETON = Client(
            {
                "SERVICE_NAME": "broken",
                "SERVER_URL": "https://apm.example.io",
                "DISABLE_SEND": True,
            }
        )
        broken = apm_base.CLIENT_SINGLETON
        assert broken is not None
        self.assertFalse((broken.config.api_key or "").strip())

        os.environ["ELASTIC_APM_API_KEY"] = "apm-agent-key"
        client = create_apm_client()
        assert client is not None
        self.assertEqual(client.config.api_key, "apm-agent-key")

    def test_spawn_worker_resolves_apm_auth(self) -> None:
        os.environ["ELASTIC_APM_SERVER_URL"] = "https://apm.example.io"
        os.environ["ELASTIC_APM_API_KEY"] = "apm-agent-key"
        os.environ["ELASTIC_APM_DISABLE_SEND"] = "true"

        ctx = get_context("spawn")
        with ctx.Pool(processes=1) as pool:
            has_client, has_auth = pool.apply(_spawn_worker_auth_state)

        self.assertTrue(has_client)
        self.assertTrue(has_auth)


if __name__ == "__main__":
    unittest.main()
