package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ehengao/machinery/v1"
	"github.com/ehengao/machinery/v1/config"
	"github.com/ehengao/machinery/v1/log"
	opentracing "github.com/opentracing/opentracing-go"
	opentracing_log "github.com/opentracing/opentracing-go/log"
	uuid "github.com/satori/go.uuid"
	"github.com/urfave/cli"

	exampletasks "github.com/ehengao/machinery/example/tasks"
	tracers "github.com/ehengao/machinery/example/tracers"
	"github.com/ehengao/machinery/v1/tasks"
)

var (
	app        *cli.App
	configPath string
)

func init() {
	// Initialise a CLI app
	app = cli.NewApp()
	app.Name = "machinery"
	app.Usage = "machinery worker and send example tasks with machinery send"
	app.Author = "Heng GAO"
	app.Email = "heng.gao.us@gmail.com"
	app.Version = "0.0.0"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "c",
			Value:       "",
			Destination: &configPath,
			Usage:       "Path to a configuration file",
		},
	}
}

func main() {
	// Set the CLI app commands
	app.Commands = []cli.Command{
		{
			Name:  "worker",
			Usage: "launch machinery worker",
			Action: func(c *cli.Context) error {
				if err := worker(); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
		{
			Name:  "send",
			Usage: "send example tasks ",
			Action: func(c *cli.Context) error {
				if err := send(); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
	}

	// Run the CLI app
	app.Run(os.Args)
}

func loadConfig() (*config.Config, error) {
	if configPath != "" {
		return config.NewFromYaml(configPath, true)
	}

	return config.NewFromEnvironment(true)
}

func startServer(cnf *config.Config) (*machinery.Server, error) {
	// Create server instance
	server, err := machinery.NewServer(cnf)
	if err != nil {
		return nil, err
	}

	// Register tasks
	tasks := map[string]interface{}{
		"proja.test.test_task.go":   exampletasks.Add,
		"proja.test.test_task.tsum": exampletasks.SumInts,
	}

	return server, server.RegisterTasks(tasks)
}

func worker() error {
	cnf, err := loadConfig()
	if err != nil {
		return err
	}
	consumerTag := "machinery_worker"

	cleanup, err := tracers.SetupTracer(consumerTag)
	if err != nil {
		log.FATAL.Fatalln("Unable to instantiate a tracer:", err)
	}
	defer cleanup()

	server, err := startServer(cnf)
	if err != nil {
		return err
	}

	// The second argument is a consumer tag
	// Ideally, each worker should have a unique tag (worker1, worker2 etc)
	worker := server.NewWorker(consumerTag, cnf.Concurrency)

	return worker.Launch()
}

func send() error {
	cleanup, err := tracers.SetupTracer("sender")
	if err != nil {
		log.FATAL.Fatalln("Unable to instantiate a tracer:", err)
	}
	defer cleanup()

	cnf, err := loadConfig()
	if err != nil {
		return err
	}
	server, err := startServer(cnf)
	if err != nil {
		return err
	}

	var (
		addTask0, addTask1, addTask2 tasks.Signature
		// multiplyTask0, multiplyTask1                      tasks.Signature
		// sumIntsTask, sumFloatsTask, concatTask, splitTask tasks.Signature
		// panicTask                                         tasks.Signature
		// longRunningTask                                   tasks.Signature
	)

	var initTasks = func() {
		addTask0 = tasks.Signature{
			Name: "proja.test.test_task.go",
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 1,
				},
				{
					Type:  "int64",
					Value: 1,
				},
			},
		}

		addTask1 = tasks.Signature{
			Name: "proja.test.test_task.go",
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 2,
				},
				{
					Type:  "int64",
					Value: 2,
				},
			},
		}

		addTask2 = tasks.Signature{
			Name: "proja.test.test_task.go",
			Args: []tasks.Arg{
				{
					Type:  "int64",
					Value: 5,
				},
				{
					Type:  "int64",
					Value: 6,
				},
			},
		}
	}
	/*
	 * Lets start a span representing this run of the `send` command and
	 * set a batch id as baggage so it can travel all the way into
	 * the worker functions.
	 */
	span, ctx := opentracing.StartSpanFromContext(context.Background(), "send")
	defer span.Finish()

	batchUUID, err := uuid.NewV4()

	if err != nil {
		return fmt.Errorf("Error generating batch id: %s", err.Error())
	}

	batchID := batchUUID.String()
	span.SetBaggageItem("batch.id", batchID)
	span.LogFields(opentracing_log.String("batch.id", batchID))

	log.INFO.Println("Starting batch:", batchID)
	/*
	 * First, let's try sending a single task
	 */
	initTasks()

	log.INFO.Println("Single task:")

	asyncResult, err := server.SendTaskWithContext(ctx, &addTask0)
	if err != nil {
		return fmt.Errorf("Could not send task: %s", err.Error())
	}

	log.INFO.Println("Task send, waiting for result")

	results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
	if err != nil {
		return fmt.Errorf("Getting task result failed with error: %s", err.Error())
	}
	log.INFO.Printf("1 + 1 = %v\n", tasks.HumanReadableResults(results))
	return nil
}
