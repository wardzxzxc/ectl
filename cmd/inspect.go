package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wardzxzxc/ectl/internal/data"
	"github.com/wardzxzxc/ectl/internal/output"
)

func init() {
	rootCmd.AddCommand(inspectCmd)
}

var inspectCmd = &cobra.Command{
	Use:   "inspect <file>",
	Short: "print schema and summary of file",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		filePath := args[0]
		var result *data.InspectResult
		var err error

		switch {
		case strings.HasSuffix(filePath, ".parquet"):
			result, err = data.InspectParquet(filePath)
		default:
			fmt.Errorf("unsupported file format: %s", filePath)
		}

		output.RenderTable(result)

		if err != nil {
			return err
		}

		return err
	},
}
