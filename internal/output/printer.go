package output

import (
	"fmt"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/lipgloss"
	"github.com/wardzxzxc/ectl/internal/data"
)

var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240"))

func RenderTable(result *data.InspectResult) error {
	// Create columns for the table
	columns := []table.Column{
		{Title: "Column", Width: 20},
		{Title: "Type", Width: 15},
		{Title: "Sample", Width: 40},
	}

	// Create rows showing schema + sample values
	rows := []table.Row{}
	for _, col := range result.Columns {
		sample := ""
		if len(result.Preview) > 0 {
			// Get first non-null value as sample
			if val, ok := result.Preview[0][col.Name]; ok && val != nil {
				sample = fmt.Sprintf("%v", val)
				if len(sample) > 37 {
					sample = sample[:37] + "..."
				}
			}
		}

		rows = append(rows, table.Row{
			col.Name,
			col.Type,
			sample,
		})
	}

	t := table.New(
		table.WithColumns(columns),
		table.WithRows(rows),
		table.WithFocused(true),
		table.WithHeight(min(len(rows)+2, 20)),
	)

	// Style the table
	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)
	t.SetStyles(s)

	// Add header with file info
	header := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("99")).
		Render(fmt.Sprintf("\n📊 File Inspector\n\nRows: %d | Size: %.2f MB\n",
			result.RowCount,
			float64(result.FileSizeBytes)/(1024*1024)))

	fmt.Println(header)
	fmt.Println(baseStyle.Render(t.View()))
	return nil
}
