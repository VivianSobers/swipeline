package main

import (
	"fmt"
	"os"

	"github.com/VivianSobers/swipeline/internal/dashboard"
)

func main() {
	queriesDir := "dashboards/queries"
	chartsDir := "dashboards/payments-operations/charts"
	if err := dashboard.GenerateCharts(queriesDir, chartsDir); err != nil {
		fmt.Fprintf(os.Stderr, "Error generating charts: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Dashboard charts generated successfully.")
}
