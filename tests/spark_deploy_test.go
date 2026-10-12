package tests

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

func TestSparkDefaultsConf(t *testing.T) {
	path := "deploy/spark/spark-defaults.conf"
	if _, err := os.Stat(path); err != nil {
		path = "../deploy/spark/spark-defaults.conf"
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("spark-defaults.conf missing: %v", err)
	}
	defer f.Close()

	props := make(map[string]string)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			props[fields[0]] = strings.Join(fields[1:], " ")
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	expected := map[string]string{
		"spark.master":                          "local[1]",
		"spark.serializer":                      "org.apache.spark.serializer.KryoSerializer",
		"spark.sql.extensions":                  "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
		"spark.sql.catalog.spark_catalog":       "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
		"spark.hadoop.fs.s3a.endpoint":          "http://seaweedfs:8333",
		"spark.hadoop.fs.s3a.path.style.access": "true",
		"spark.hadoop.fs.s3a.impl":              "org.apache.hadoop.fs.s3a.S3AFileSystem",
	}

	for k, want := range expected {
		got, ok := props[k]
		if !ok {
			t.Errorf("missing property %s", k)
		} else if got != want {
			t.Errorf("property %s = %q, want %q", k, got, want)
		}
	}
}

func TestSparkMakefileCheck(t *testing.T) {
	data, err := os.ReadFile("../Makefile")
	if err != nil {
		data, err = os.ReadFile("Makefile")
		if err != nil {
			t.Fatal(err)
		}
	}
	content := string(data)
	if !strings.Contains(content, ".PHONY: spark-check") || !strings.Contains(content, "spark-check:") {
		t.Fatal("Makefile missing spark-check target")
	}
	idx := strings.Index(content, "spark-check:")
	block := content[idx:]
	if next := strings.Index(block[1:], "\n.PHONY:"); next != -1 {
		block = block[:next+1]
	}
	for _, want := range []string{
		"deploy/spark.Dockerfile",
		"swipeline-spark:m4",
		"pyspark",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("spark-check missing %q", want)
		}
	}
}

func TestReadmeLifecycleSection(t *testing.T) {
	path := "README.md"
	if _, err := os.Stat(path); err != nil {
		path = "../README.md"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content := string(data)

	for _, want := range []string{
		"## Follow a Transaction's Lifecycle",
		"swipeline inspect",
		"CLEARED",
		"REVERSED",
		"CHARGED_BACK",
		"SeaweedFS",
		"Hudi",
		"8.25 GiB",
		"150 bps",
		"make m4-demo",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("README.md missing %q", want)
		}
	}
}

func TestSparkExecutableDeployment(t *testing.T) {
	dockerfile, err := os.ReadFile("../deploy/spark.Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"COPY batch /app/batch", `CMD ["python", "-m", "batch.worker", "--poll", "60s"]`} {
		if !strings.Contains(string(dockerfile), want) {
			t.Errorf("missing %s", want)
		}
	}
	content := string(dockerfile)
	if strings.Contains(content, "archive.apache.org") {
		t.Errorf("spark.Dockerfile must not download from archive.apache.org")
	}
	wantPySpark := "pyspark==4.1.3"
	wantPySparkHash := "b600238e771e45618a58beaf9d63cfd9aa06f61ce193244cedf890a04a4409f7"
	if !strings.Contains(content, wantPySpark) || !strings.Contains(content, "--require-hashes") || !strings.Contains(content, wantPySparkHash) {
		t.Errorf("spark.Dockerfile must install pinned %s with --require-hashes and hash %s", wantPySpark, wantPySparkHash)
	}
	if !strings.Contains(content, "SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark") {
		t.Errorf("spark.Dockerfile must set SPARK_HOME to pyspark package directory")
	}
	for _, flag := range []string{"--retry 5", "--retry-delay 5", "--connect-timeout 30"} {
		if !strings.Contains(content, flag) {
			t.Errorf("spark.Dockerfile curl missing fail fast/retry flag %q", flag)
		}
	}
	jars := []struct {
		name   string
		sha1   string
		sha256 string
	}{
		{"hudi-spark4.1-bundle_2.13-1.2.1.jar", "90d03303ca994bdae82f91136d8c149e8df26b68", ""},
		{"hadoop-aws-3.4.2.jar", "16f9de6da5c7862241d55a13d34843404d0cbb88", ""},
		{"bundle-2.29.52.jar", "b63eb928c2ac13bde07746bcfe7fb38bbdb4b5c5", ""},
		{"spark-sql-kafka-0-10_2.13-4.1.3.jar", "fa6f5869ac9f8f183e1941e0a8faae6257fd1ca2", ""},
		{"kafka-clients-3.9.1.jar", "", "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9"},
		{"spark-token-provider-kafka-0-10_2.13-4.1.3.jar", "3903171bdc843d9560916a91ada4ecf9d10db1e7", ""},
		{"commons-pool2-2.12.1.jar", "e5c7b6c9f5a4b59be2bba213fbec70b413376fc1", ""},
	}
	for _, j := range jars {
		if !strings.Contains(content, j.name) {
			t.Errorf("spark.Dockerfile missing jar %s", j.name)
			continue
		}
		hasCheck := false
		if j.sha1 != "" && strings.Contains(content, j.sha1) && strings.Contains(content, "sha1sum") {
			hasCheck = true
		}
		if j.sha256 != "" && strings.Contains(content, j.sha256) && strings.Contains(content, "sha256sum") {
			hasCheck = true
		}
		if !hasCheck {
			t.Errorf("spark.Dockerfile missing checksum verification for jar %s", j.name)
		}
	}
	compose, err := os.ReadFile("../compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"mini", "-dir=/data", "-bucket=swipeline"`, "http://127.0.0.1:8080/health", "SWIPELINE_SEAWEED_PORT"} {
		if !strings.Contains(string(compose), want) {
			t.Errorf("missing %s", want)
		}
	}
	demo, err := os.ReadFile("m4-demo.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"TestInstalledLifecyclePrepare", "restart spark-batch", "SWIPELINE_SEAWEED_PORT=0", "SWIPELINE_FIXTURES"} {
		if !strings.Contains(string(demo), want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(string(demo), "batch.worker --once") {
		t.Fatal("demo starts second Spark session")
	}
}

func TestFreshAnalyticsRuntimeFixtures(t *testing.T) {
	data, err := os.ReadFile("../deploy/superset.Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	content := string(data)
	owned := strings.Index(content, "mkdir -p /secrets")
	switched := strings.LastIndex(content, "USER superset")
	if owned < 0 || owned > switched || !strings.Contains(content, "chown superset:superset /secrets") {
		t.Fatal("fresh Superset secrets volume must copy up an owned directory before USER superset")
	}
	data, err = os.ReadFile("m3-demo.sh")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "if ! docker image inspect swipeline-superset:m3") {
		t.Fatal("cached Superset image can retain stale source")
	}
}

func TestM2LatencyStartsOnlyRequiredServices(t *testing.T) {
	data, err := os.ReadFile("m2-latency.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(line, "docker compose") && strings.Contains(line, " up ") && !strings.Contains(line, "postgres kafka gateway replayer prometheus flink-jobmanager flink-taskmanager") {
			t.Fatalf("unscoped latency startup: %s", line)
		}
	}
}

func TestM4ModuleCacheCleanupRestoresDirectoryWrites(t *testing.T) {
	data, err := os.ReadFile("m4-demo.sh")
	if err != nil {
		t.Fatal(err)
	}
	content := string(data)
	restore := strings.Index(content, `chmod -R u+w "$work/go-mod"`)
	remove := strings.Index(content, `rm -rf "$work"`)
	if restore < 0 || remove < restore {
		t.Fatal("read-only Go module cache must become writable before task cleanup")
	}
}

func TestSeaweedSignedS3Bootstrap(t *testing.T) {
	data, err := os.ReadFile("../compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"-s3.config=/etc/seaweed-s3.json", "source: ./deploy/seaweed-s3.json", "http://127.0.0.1:8333/healthz"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("missing signed S3 bootstrap: %s", want)
		}
	}
	data, err = os.ReadFile("../deploy/seaweed-s3.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"accessKey": "any"`, `"secretKey": "any"`, `"Write"`, `"List"`} {
		if !strings.Contains(string(data), want) {
			t.Errorf("missing pinned S3A credentials/actions: %s", want)
		}
	}
}
