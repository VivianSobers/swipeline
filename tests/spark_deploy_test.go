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
