import os
import tempfile
import unittest
import importlib

class TestSupersetConfig(unittest.TestCase):
    def test_missing_secret_key_fails(self):
        os.environ["SUPERSET_SECRET_KEY_PATH"] = "/nonexistent/path/secret-key"
        if "superset_config" in importlib.sys.modules:
            del importlib.sys.modules["superset_config"]
        with self.assertRaises(Exception):
            import superset_config

    def test_config_values(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            key_file = os.path.join(tmpdir, "secret-key")
            with open(key_file, "w", encoding="utf-8") as f:
                f.write("test-secret-key-12345\n")

            os.environ["SUPERSET_SECRET_KEY_PATH"] = key_file
            if "superset_config" in importlib.sys.modules:
                del importlib.sys.modules["superset_config"]
            import superset_config

            self.assertEqual(
                superset_config.SQLALCHEMY_DATABASE_URI,
                os.environ.get(
                    "SQLALCHEMY_DATABASE_URI",
                    "postgresql+psycopg2://superset:superset-local@postgres:5432/superset",
                ),
            )
            self.assertEqual(superset_config.SECRET_KEY, "test-secret-key-12345")
            self.assertEqual(superset_config.CACHE_CONFIG, {"CACHE_TYPE": "SimpleCache", "CACHE_THRESHOLD": 500})
            self.assertEqual(superset_config.DATA_CACHE_CONFIG, {"CACHE_TYPE": "SimpleCache", "CACHE_THRESHOLD": 500})
            self.assertEqual(superset_config.FEATURE_FLAGS.get("DASHBOARD_NATIVE_FILTERS"), True)
            self.assertEqual(superset_config.FEATURE_FLAGS.get("ALERT_REPORTS"), False)
            self.assertIsNone(superset_config.RESULTS_BACKEND)
            self.assertEqual(superset_config.SUPERSET_WEBSERVER_TIMEOUT, 60)

if __name__ == "__main__":
    unittest.main()
