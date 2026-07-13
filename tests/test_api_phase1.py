"""API tests for Phase 1 hardening and observability wiring."""

from __future__ import annotations

import os
import unittest
from unittest import mock

from fastapi.testclient import TestClient

from frosty.api.app import create_app
from frosty.api.jobs import JobManager
from frosty.api.schemas import JobType
from frosty.config import FrostyConfig


class AuthTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = os.environ.copy()

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)

    def test_startup_fails_when_api_key_required_but_missing(self) -> None:
        os.environ["FROSTY_REQUIRE_API_KEY"] = "true"
        os.environ.pop("FROSTY_API_KEY", None)
        with self.assertRaises(RuntimeError):
            create_app(FrostyConfig())

    def test_metrics_requires_metrics_key_when_configured(self) -> None:
        os.environ.pop("FROSTY_REQUIRE_API_KEY", None)
        os.environ.pop("FROSTY_API_KEY", None)
        os.environ["FROSTY_METRICS_API_KEY"] = "metrics-secret"
        app = create_app(FrostyConfig())
        client = TestClient(app)

        self.assertEqual(client.get("/metrics").status_code, 401)
        response = client.get("/metrics", headers={"X-API-Key": "metrics-secret"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/plain", response.headers.get("content-type", ""))

    def test_v1_routes_require_api_key_when_configured(self) -> None:
        os.environ["FROSTY_API_KEY"] = "api-secret"
        os.environ.pop("FROSTY_REQUIRE_API_KEY", None)
        app = create_app(FrostyConfig())
        client = TestClient(app)

        self.assertEqual(client.get("/v1/jobs").status_code, 401)
        self.assertEqual(
            client.get("/v1/jobs", headers={"X-API-Key": "api-secret"}).status_code,
            200,
        )

    def test_health_reports_remote_write_flag(self) -> None:
        os.environ.pop("FROSTY_REQUIRE_API_KEY", None)
        os.environ.pop("FROSTY_API_KEY", None)
        os.environ.pop("FROSTY_METRICS_API_KEY", None)
        app = create_app(
            FrostyConfig(
                elastic_url="https://example.es.io",
                api_key="elastic-key",
            )
        )
        client = TestClient(app)
        payload = client.get("/health").json()
        self.assertTrue(payload["prometheus_remote_write_enabled"])


class JobManagerTests(unittest.TestCase):
    def test_failed_job_does_not_include_traceback(self) -> None:
        manager = JobManager(max_workers=1)

        def _fail() -> None:
            raise ValueError("boom")

        with mock.patch("frosty.api.jobs.record_api_job") as record_api_job:
            job = manager.submit(JobType.SCAN, _fail)
            manager._futures[job.job_id].result(timeout=5)

            self.assertEqual(job.status.value, "failed")
            self.assertEqual(job.error, "boom")
            self.assertIsNone(job.result)

            statuses = [call.kwargs["status"] for call in record_api_job.call_args_list]
            self.assertEqual(statuses, ["submitted", "running", "failed"])

    def test_list_jobs_limit_validation(self) -> None:
        os.environ.pop("FROSTY_REQUIRE_API_KEY", None)
        os.environ["FROSTY_API_KEY"] = "api-secret"
        app = create_app(FrostyConfig())
        client = TestClient(app)
        headers = {"X-API-Key": "api-secret"}

        self.assertEqual(client.get("/v1/jobs?limit=0", headers=headers).status_code, 422)
        self.assertEqual(client.get("/v1/jobs?limit=501", headers=headers).status_code, 422)
        self.assertEqual(client.get("/v1/jobs?limit=50", headers=headers).status_code, 200)


class RemoteWriteLifespanTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = os.environ.copy()

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)

    def test_lifespan_starts_and_stops_remote_write_pusher(self) -> None:
        os.environ.pop("FROSTY_REQUIRE_API_KEY", None)
        os.environ.pop("FROSTY_API_KEY", None)
        os.environ.pop("FROSTY_METRICS_API_KEY", None)

        pusher = mock.Mock()
        with mock.patch("frosty.api.app.start_remote_write_pusher", return_value=pusher):
            app = create_app(FrostyConfig())
            with TestClient(app) as client:
                self.assertEqual(client.get("/health").status_code, 200)
            pusher.stop.assert_called_once()


class ElasticVerifyErrorMappingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = os.environ.copy()

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)

    def test_verify_maps_frosty_config_error_to_400(self) -> None:
        from frosty.errors import FrostyConfigError

        os.environ.pop("FROSTY_REQUIRE_API_KEY", None)
        os.environ.pop("FROSTY_API_KEY", None)
        with mock.patch("frosty.api.app.FrostyClient") as client_cls:
            instance = client_cls.return_value
            instance.verify_elastic.side_effect = FrostyConfigError("missing key")
            app = create_app(FrostyConfig())
            http = TestClient(app)
            response = http.post("/v1/elastic/verify")
            self.assertEqual(response.status_code, 400)
            self.assertIn("missing key", response.json()["detail"])

    def test_verify_maps_frosty_error_to_502(self) -> None:
        from frosty.errors import FrostyError

        os.environ.pop("FROSTY_REQUIRE_API_KEY", None)
        os.environ.pop("FROSTY_API_KEY", None)
        with mock.patch("frosty.api.app.FrostyClient") as client_cls:
            instance = client_cls.return_value
            instance.verify_elastic.side_effect = FrostyError("es down")
            app = create_app(FrostyConfig())
            http = TestClient(app)
            response = http.post("/v1/elastic/verify")
            self.assertEqual(response.status_code, 502)
            self.assertIn("es down", response.json()["detail"])

    def test_ingest_job_failure_surfaces_frosty_config_error(self) -> None:
        import time

        from frosty.errors import FrostyConfigError

        os.environ.pop("FROSTY_REQUIRE_API_KEY", None)
        os.environ.pop("FROSTY_API_KEY", None)
        with mock.patch("frosty.api.app.FrostyClient") as client_cls:
            instance = client_cls.return_value
            instance.ingest.side_effect = FrostyConfigError("bad config")
            app = create_app(FrostyConfig())
            http = TestClient(app)
            created = http.post("/v1/jobs/ingest", json={})
            self.assertEqual(created.status_code, 202)
            job_id = created.json()["job_id"]

            payload = None
            for _ in range(50):
                payload = http.get(f"/v1/jobs/{job_id}").json()
                if payload["status"] in ("completed", "failed"):
                    break
                time.sleep(0.05)
            self.assertIsNotNone(payload)
            assert payload is not None
            self.assertEqual(payload["status"], "failed")
            self.assertEqual(payload["error"], "bad config")


if __name__ == "__main__":
    unittest.main()
