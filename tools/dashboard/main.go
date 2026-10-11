package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/VivianSobers/swipeline/internal/dashboard"
)

func main() {
	bundleFlag := flag.Bool("bundle", false, "build zip bundle")
	flag.Parse()

	queriesDir := "dashboards/queries"
	chartsDir := "dashboards/payments-operations/charts"
	bundleSrc := "dashboards/payments-operations"
	bundleOut := "dashboards/payments-operations.zip"

	if err := dashboard.GenerateCharts(queriesDir, chartsDir); err != nil {
		fmt.Fprintf(os.Stderr, "Error generating charts: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Dashboard charts generated successfully.")

	if *bundleFlag || len(os.Args) == 1 {
		if err := dashboard.BuildBundleZip(bundleSrc, bundleOut); err != nil {
			fmt.Fprintf(os.Stderr, "Error building bundle zip: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Dashboard bundle zip built successfully.")
	}
}
