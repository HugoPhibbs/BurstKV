// export BKV_ADDR=http://localhost:8080
package cli

import (
	"burstkv/internal/controller/controller_pb"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"slices"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

const requestTimeout = 5 * time.Second

// Starts the cli
func Start() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c := &cli{}
	defer c.onClose()
	return c.rootCmd().ExecuteContext(ctx)
}

func (c *cli) onClose() {
	if c.clientConn != nil {
		c.clientConn.Close()
	}
}

type cli struct {
	addr       string
	client     controller_pb.ControllerClient
	clientConn *grpc.ClientConn
}

func (c *cli) rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "bkv",
		Short: "Manage BurstKV databases",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if c.addr == "" {
				return errors.New("controller address not set, use --addr or the BKV_ADDR env var")
			}
			conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			c.client = controller_pb.NewControllerClient(conn)
			c.clientConn = conn
			return nil
		},
	}

	root.PersistentFlags().StringVar(&c.addr, "addr", os.Getenv("BKV_ADDR"),
		"controller address, defaults to the BKV_ADDR env var")

	root.AddCommand(
		c.createCmd(),
		c.statusCmd(),
		c.dbStatusCmd(),
		c.pauseCmd(),
		c.deleteCmd(),
		c.resumeCmd(),
		c.configCmd(),
	)

	return root
}

func (c *cli) configCmd() *cobra.Command {
	return &cobra.Command{
		Use: "config",
		Short: "Get the internal configuration of the controller",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), requestTimeout)
			defer cancel()

			config, err := c.client.GetControllerConfig(ctx, &emptypb.Empty{})

			if err != nil {
				fmt.Println(err)
				return nil
			}

			fmt.Printf("Redis Port: %d\n", config.ProxyPort)
			fmt.Printf("Idle limit (s): %d\n", config.IdleLimit)
			fmt.Printf("Storage URL: %s\n", config.StorageUrl)
			
			return nil
		},
	}
}

func (c *cli) pauseCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "pause + <id>",
		Short: "Pause a database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), requestTimeout)
			defer cancel()

			databaseID := args[0]
			_, err := c.client.PauseDatabase(ctx, &controller_pb.PauseDatabaseRequest{DatabaseId: databaseID})

			if err != nil {
				fmt.Println(err)
			}
			return nil
		},
	}
}

func (c *cli) resumeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "resume + <id>",
		Short: "Resume a database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), requestTimeout)
			defer cancel()

			databaseID := args[0]
			_, err := c.client.ResumeDatabase(ctx, &controller_pb.ResumeDatabaseRequest{DatabaseId: databaseID})

			if err != nil {
				fmt.Println(err)
			}
			return nil
		},
	}
}

func (c *cli) deleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete + <id>",
		Short: "Delete a database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), requestTimeout)
			defer cancel()

			databaseID := args[0]
			_, err := c.client.DeleteDatabase(ctx, &controller_pb.DeleteDatabaseRequest{DatabaseId: databaseID})

			if err != nil {
				fmt.Println(err)
			}
			return nil
		},
	}
}

func (c * cli) statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Print the status of the controlle",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), requestTimeout)
			defer cancel()

			resp, err := c.client.GetControllerStatus(ctx, &emptypb.Empty{})
			if err != nil {
				fmt.Println(err)
				return nil
			}

			fmt.Printf("Number of pods available: %d\n", resp.NumPodsAvailable)
			fmt.Printf("Number of databases: %d\n", resp.NumDatabases)
			return nil
		},
	}
}

func (c *cli) dbStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "db-status + <id>",
		Short: "Print the status of all databases or just one specified by the id",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), requestTimeout)
			defer cancel()

			var statusResponses map[string]*controller_pb.DatabaseStatus // ID -> status

			if len(args) == 0 {
				databasesResp, err := c.client.GetAllDatabaseStatus(ctx, &emptypb.Empty{})

				if err != nil {
					fmt.Println(err)
					return nil
				}

				statusResponses = databasesResp.GetStatuses()

			} else {
				databaseID := args[0]
				resp, err := c.client.GetDatabaseStatus(ctx, &controller_pb.GetDatabaseStatusRequest{DatabaseId: databaseID})

				if err != nil {
					fmt.Println(err)
					return nil
				}

				statusResponses = map[string]*controller_pb.DatabaseStatus{
					databaseID: resp.GetStatus(),
				}
			}

			printStatuses(cmd, statusResponses)
			return nil
		},
	}
}

func printStatuses(cmd *cobra.Command, statuses map[string]*controller_pb.DatabaseStatus) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 3, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "DATABASE ID\tRUNNING\tMEM (MB)\tCPU (mCPU)")
	for _, id := range slices.Sorted(maps.Keys(statuses)) {
		s := statuses[id]
		used, max := s.GetUsedPodResources(), s.GetMaxPodResources()

		mem := "-"
		cpu := "-"

		if used != nil && max != nil {
			mem = formatPodResource(MB(used.GetMemory()), MB(max.GetMemory()))
			cpu = formatPodResource(used.GetCpu(), max.GetCpu())
		}

		fmt.Fprintf(w, "%s\t%t\t%s\t%s\n",
			id,
			s.GetRunning(),
			mem,
			cpu,
		)
	}
}

func formatPodResource(used int64, max int64) string {
	return fmt.Sprintf("%d/%d", used, max)
}

// Converts raw bytes into mega bytes
func MB(b int64) int64 {
	return b / (1024 * 1024)
}

func (c *cli) createCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create",
		Short: "Create a new database",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), requestTimeout)
			defer cancel()

			resp, err := c.client.CreateDatabase(ctx, &emptypb.Empty{})

			if err != nil {
				fmt.Println(err)
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "id: %s\npassword: %s\n", resp.GetDatabaseId(), resp.GetPassword())
			return nil
		},
	}
}
