import os
import unittest

from batch.session import get_spark_session


class TestSparkSessionBuilder(unittest.TestCase):
    def test_get_spark_session(self):
        spark = get_spark_session("test-session-builder")
        self.assertIsNotNone(spark)
        
        # Verify Kryo serializer
        serializer = spark.conf.get("spark.serializer", "")
        self.assertEqual(serializer, "org.apache.spark.serializer.KryoSerializer")
        
        # Verify Hudi extensions and catalog
        extensions = spark.conf.get("spark.sql.extensions", "")
        self.assertEqual(extensions, "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        
        catalog = spark.conf.get("spark.sql.catalog.spark_catalog", "")
        self.assertEqual(catalog, "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        
        # Verify Hadoop S3A configurations
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        endpoint = hadoop_conf.get("fs.s3a.endpoint")
        self.assertTrue(endpoint.endswith(":8333") or "seaweedfs" in endpoint or "localhost" in endpoint)
        
        path_style = hadoop_conf.get("fs.s3a.path.style.access")
        self.assertEqual(path_style, "true")


if __name__ == "__main__":
    unittest.main()
