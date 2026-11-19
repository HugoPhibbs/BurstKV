package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"burstkv/internal/core/database_status"
	"burstkv/internal/core/pod_resources.go"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/go-resty/resty/v2"
)

const refreshInterval = 2 * time.Second

// Run starts the TUI and blocks until the user quits (ctrl+c).
func Run(addr string) error {
	addr = strings.TrimRight(addr, "/")
	client := resty.New().SetBaseURL(addr).SetTimeout(5 * time.Second)

	app := tview.NewApplication()

	table := tview.NewTable()
	output := tview.NewTextView()
	idInput := tview.NewInputField().SetLabel("database id: ")
	menu := tview.NewList().ShowSecondaryText(false)

	table.SetTitle(" BurstKV databases @ " + addr + " ").SetBorder(true)
	table.SetCell(0, 0, tview.NewTableCell("loading..."))

	// Runs an action in the background so the UI doesn't freeze during the HTTP call.
	run := func(action, id string) {
		output.SetText(action + "...")
		go func() {
			result := runDatabaseAction(client, action, id)
			app.QueueUpdateDraw(func() { output.SetText(result) })
		}()
	}

	// Action waiting for a database ID to be typed in.
	pending := ""

	menu.AddItem("create", "", 'c', func() { run("create", "") })
	for _, action := range []string{"pause", "resume", "delete"} {
		menu.AddItem(action, "", rune(action[0]), func() {
			pending = action
			app.SetFocus(idInput)
		})
	}

	idInput.SetDoneFunc(func(key tcell.Key) {
		id := strings.TrimSpace(idInput.GetText())
		if key == tcell.KeyEnter && pending != "" && id != "" {
			run(pending, id)
		}
		pending = ""
		idInput.SetText("")
		app.SetFocus(menu)
	})

	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(table, 0, 1, false).
		AddItem(menu, 4, 0, true).
		AddItem(idInput, 1, 0, false).
		AddItem(output, 3, 0, false)

	// Refresh the table in the background.
	go func() {
		for {
			statuses, err := fetchStatuses(client)
			app.QueueUpdateDraw(func() { showStatuses(table, statuses, err) })
			time.Sleep(refreshInterval)
		}
	}()

	return app.SetRoot(layout, true).EnableMouse(true).Run()
}

func showStatuses(table *tview.Table, statuses map[string]database_status.DatabaseStatus, err error) {
	table.Clear()
	if err != nil {
		table.SetCell(0, 0, tview.NewTableCell("error: "+err.Error()))
		return
	}
	if len(statuses) == 0 {
		table.SetCell(0, 0, tview.NewTableCell("no databases"))
		return
	}

	ids := make([]string, 0, len(statuses))
	for id := range statuses {
		ids = append(ids, id)
	}
	sort.Strings(ids) // map order is random; keep rows stable between refreshes

	for col, header := range []string{"DATABASE ID", "POD NAME", "STATUS", "CPU (m)", "MEM (MiB)"} {
		table.SetCell(0, col, tview.NewTableCell(header).SetTextColor(tcell.ColorYellow).SetExpansion(1))
	}
	for i, id := range ids {
		s := statuses[id]
		state := "paused"
		if s.Running {
			state = "running"
		}
		cpu, mem := formatResources(s.UsedPodResources, s.MaxPodResources)
		for col, text := range []string{id, s.PodName, state, cpu, mem} {
			table.SetCell(i+1, col, tview.NewTableCell(text))
		}
	}
}

var errBody struct {
	Error string `json:"error"`
}

// Fetches all statuses from the controller.
func fetchStatuses(client *resty.Client) (map[string]database_status.DatabaseStatus, error) {
	var body struct {
		Statuses map[string]database_status.DatabaseStatus `json:"statuses"`
	}

	resp, err := client.R().
		SetResult(&body).   // decoded on 2xx
		SetError(&errBody). // decoded on 4xx/5xx
		Get("/database")

	if err != nil {
		return nil, err // network failure / timeout
	}

	if resp.IsError() {
		return nil, fmt.Errorf("controller returned %s: %s", resp.Status(), errBody.Error)
	}

	return body.Statuses, nil
}

func createDatabase(client *resty.Client) string {
	var createRespBody struct {
		databaseID string
	}

	resp, err := client.R().SetBody(&createRespBody).SetError(&errBody).Post("/database")

	if err != nil {
		return "Error: " + err.Error()
	}

	if resp.IsError() {
		return errBody.Error
	}
	return createRespBody.databaseID
}

func resumeDatabase(client *resty.Client, id string) string {
	resp, err := client.R().SetError(&errBody).Post("/database/resume/" + id)

	if err != nil {
		return "Error: " + err.Error()
	}
	if resp.IsError() {
		return errBody.Error
	}
	return resp.Status()
}

func pauseDatabase(client *resty.Client, id string) string {
	resp, err := client.R().SetError(&errBody).Post("/database/pause/" + id)

	if err != nil {
		return "Error: " + err.Error()
	}
	if resp.IsError() {
		return errBody.Error
	}

	return resp.Status()
}

func deleteDatabase(client *resty.Client, id string) string {
	resp, err := client.R().SetError(&errBody).Delete("/database/" + id)

	if err != nil {
		return "Error: " + err.Error()
	}

	if resp.IsError() {
		return errBody.Error
	}

	return resp.Status()
}

// Calls the controller for an action and returns a line to show the user.
// pause/resume/delete all follow /database/<action>/<id>; create is POST /database.
func runDatabaseAction(client *resty.Client, action, id string) string {
	switch action {
	case "create":
		return createDatabase(client)
	case "delete":
		return deleteDatabase(client, id)
	case "resume":
		return resumeDatabase(client, id)
	case "pause":
		return pauseDatabase(client, id)
	default:
		panic(fmt.Errorf("Unknown action: %s", action))
	}
}

// Formats "used/max" pairs; "-" when resources aren't available (e.g. paused, no metrics-server).
func formatResources(used, max *pod_resources.PodResources) (cpu, mem string) {
	if used == nil || max == nil {
		return "-", "-"
	}
	cpu = fmt.Sprintf("%d/%d", used.Cpu, max.Cpu)
	mem = fmt.Sprintf("%d/%d", used.Memory>>20, max.Memory>>20)
	return cpu, mem
}
