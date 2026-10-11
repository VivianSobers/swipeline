package store

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestAnalyticsMetadataIsolation(t *testing.T) {
	dsn := os.Getenv("SWIPELINE_DB")
	if dsn == "" {
		t.Skip("run make db-test")
	}

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}

	for _, user := range []string{"druid", "superset"} {
		t.Run(user+"_cannot_access_gateway", func(t *testing.T) {
			gwURL := *u
			gwURL.User = url.UserPassword(user, user+"-local")
			gwURL.Path = "/gateway"

			db, err := sql.Open("pgx", gwURL.String())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			ctx := context.Background()
			var dummy int
			err = db.QueryRowContext(ctx, "SELECT 1 FROM authorizations LIMIT 1").Scan(&dummy)
			if err == nil {
				t.Fatalf("%s was able to query gateway.authorizations", user)
			}
			if !strings.Contains(err.Error(), "permission denied") {
				t.Fatalf("%s expected permission error, got: %v", user, err)
			}
		})

		t.Run(user+"_can_create_table_in_own_db", func(t *testing.T) {
			ownURL := *u
			ownURL.User = url.UserPassword(user, user+"-local")
			ownURL.Path = "/" + user

			db, err := sql.Open("pgx", ownURL.String())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			ctx := context.Background()
			if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test_isolation (id int);"); err != nil {
				t.Fatalf("%s failed to create table in %s: %v", user, user, err)
			}
			if _, err := db.ExecContext(ctx, "DROP TABLE test_isolation;"); err != nil {
				t.Fatalf("%s failed to drop table in %s: %v", user, user, err)
			}
		})
	}
}
