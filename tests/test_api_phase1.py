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
        with mock.patch("frosty.api.app.verify_cluster", return_value={"ok": True}):
            app = create_app(
                FrostyConfig(
                    elastic_url="https://example.es.io",
                    api_key="elastic-key",
                )
            )
            client = TestClient(app)
            payload = client.get("/health").json()
            self.assertTrue(payload["prometheus_remote_write_enabled"])
            self.assertEqual(payload["status"], "ok")
            self.assertTrue(payload["elastic_configured"])
            self.assertTrue(payload["elastic_reachable"])

    def test_health_degraded_when_elastic_unreachable(self) -> None:
        os.environ.pop("FROSTY_REQUIRE_API_KEY", None)
        os.environ.pop("FROSTY_API_KEY", None)
        with mock.patch(
            "frosty.api.app.verify_cluster",
            side_effect=RuntimeError("down"),
        ):
            app = create_app(
                FrostyConfig(
                    elastic_url="https://example.es.io",
                    api_key="elastic-key",
                )
            )
            client = TestClient(app)
            response = client.get("/health")
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["status"], "degraded")
            self.assertTrue(payload["elastic_configured"])
            self.assertFalse(payload["elastic_reachable"])


class JobManagerTests(unittest.TestCase):
    def _wait_for_terminal(self, manager: JobManager, job, *, timeout: float = 5.0):
        import time

        deadline = time.time() + timeout
        while time.time() < deadline:
            if job.status.value in ("completed", "failed"):
                return
            time.sleep(0.01)
        self.fail(f"job did not finish: status={job.status}")

    def test_failed_job_does_not_include_traceback(self) -> None:
        manager = JobManager(max_workers=1)

        def _fail() -> None:
            raise ValueError("boom")

        with mock.patch("frosty.api.jobs.record_api_job") as record_api_job:
            job = manager.submit(JobType.SCAN, _fail)
            self._wait_for_terminal(manager, job)

            self.assertEqual(job.status.value, "failed")
            self.assertEqual(job.error, "boom")
            self.assertIsNone(job.result)

            statuses = [call.kwargs["status"] for call in record_api_job.call_args_list]
            self.assertEqual(statuses, ["submitted", "running", "failed"])

    def test_terminal_jobs_are_pruned(self) -> None:
        manager = JobManager(max_workers=2, max_terminal_jobs=5)
        with mock.patch("frosty.api.jobs.record_api_job"):
            jobs = [manager.submit(JobType.SCAN, lambda: {"ok": True}) for _ in range(12)]
            for job in jobs:
                self._wait_for_terminal(manager, job)
            with manager._lock:
                self.assertLessEqual(len(manager._jobs), 5)
                self.assertFalse(hasattr(manager, "_futures"))

    def test_list_jobs_limit_validation(self) -> None:
        os.environ.pop("FROSTY_REQUIRE_API_KEY", None)
        os.environ["FROSTY_API_KEY"] = "api-secret"
        app = create_app(FrostyConfig())
        client = TestClient(app)
        headers = {"X-API-Key": "api-secret"}

        self.assertEqual(client.get("/v1/jobs?limit=0", headers=headers).status_code, 422)
        self.assertEqual(client.get("/v1/jobs?limit=501", headers=headers).status_code, 422)
        self.assertEqual(client.get("/v1/jobs?limit=50", headers=headers).status_code, 200)

    def test_buckets_pagination_validation(self) -> None:
        os.environ.pop("FROSTY_REQUIRE_API_KEY", None)
        os.environ["FROSTY_API_KEY"] = "api-secret"
        with mock.patch("frosty.api.app.FrostyClient") as client_cls:
            instance = client_cls.return_value
            instance.list_buckets.return_value = []
            app = create_app(FrostyConfig())
            http = TestClient(app)
            headers = {"X-API-Key": "api-secret"}
            self.assertEqual(http.get("/v1/buckets?limit=0", headers=headers).status_code, 422)
            self.assertEqual(http.get("/v1/buckets?limit=501", headers=headers).status_code, 422)
            self.assertEqual(http.get("/v1/buckets?offset=-1", headers=headers).status_code, 422)
            response = http.get("/v1/buckets?limit=10&offset=0", headers=headers)
            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["limit"], 10)
            self.assertEqual(payload["offset"], 0)
            self.assertEqual(payload["total"], 0)
            self.assertEqual(payload["count"], 0)


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
