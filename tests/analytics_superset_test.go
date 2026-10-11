package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSupersetSecretGeneration(t *testing.T) {
	script := "deploy/superset/init.sh"
	if _, err := os.Stat(script); err != nil {
		script = "../deploy/superset/init.sh"
		if _, err := os.Stat(script); err != nil {
			t.Fatal("cannot find init.sh")
		}
	}

	tmpDir := t.TempDir()

	run := func() {
		cmd := exec.Command("sh", script)
		cmd.Env = append(os.Environ(), "SUPERSET_SECRETS_DIR="+tmpDir, "SUPERSET_SECRETS_ONLY=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("init.sh failed: %v: %s", err, string(out))
		}
	}

	run()

	secretKeyPath := filepath.Join(tmpDir, "secret-key")
	adminPassPath := filepath.Join(tmpDir, "admin-password")

	fiKey, err := os.Stat(secretKeyPath)
	if err != nil {
		t.Fatalf("secret-key not generated: %v", err)
	}
	if perm := fiKey.Mode().Perm(); perm != 0600 {
		t.Fatalf("secret-key permissions: got %04o, want 0600", perm)
	}

	fiPass, err := os.Stat(adminPassPath)
	if err != nil {
		t.Fatalf("admin-password not generated: %v", err)
	}
	if perm := fiPass.Mode().Perm(); perm != 0600 {
		t.Fatalf("admin-password permissions: got %04o, want 0600", perm)
	}

	keyData, err := os.ReadFile(secretKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	keyStr := strings.TrimSpace(string(keyData))
	if len(keyStr) == 0 {
		t.Fatal("secret-key is empty")
	}

	passData, err := os.ReadFile(adminPassPath)
	if err != nil {
		t.Fatal(err)
	}
	passStr := strings.TrimSpace(string(passData))
	if len(passStr) == 0 {
		t.Fatal("admin-password is empty")
	}

	// Run second time; must not overwrite
	run()

	keyData2, err := os.ReadFile(secretKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(keyData2)) != keyStr {
		t.Fatalf("secret-key overwritten: was %s, now %s", keyStr, string(keyData2))
	}

	passData2, err := os.ReadFile(adminPassPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(passData2)) != passStr {
		t.Fatalf("admin-password overwritten: was %s, now %s", passStr, string(passData2))
	}
}
