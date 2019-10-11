package machinery

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/opentracing/opentracing-go"

	"github.com/ehengao/machinery/v2/backends"
	_log "github.com/ehengao/machinery/v2/log"
	"github.com/ehengao/machinery/v2/retry"
	"github.com/ehengao/machinery/v2/tasks"
	"github.com/ehengao/machinery/v2/tracing"
	"go.uber.org/zap"
	"runtime"
	"runtime/debug"
)

var log = _log.GetLogger()

// Worker represents a single worker process
type Worker struct {
	server       *Server
	ConsumerTag  string
	Concurrency  int
	errorHandler func(err error)
}

// Launch starts a new worker process. The worker subscribes
// to the default queue and processes incoming registered tasks
func (worker *Worker) Launch() error {
	errorsChan := make(chan error)

	worker.LaunchAsync(errorsChan)

	return <-errorsChan
}

// LaunchAsync is a non blocking version of Launch
func (worker *Worker) LaunchAsync(errorsChan chan<- error) {
	cnf := worker.server.GetConfig()
	broker := worker.server.GetBroker()
	broker_url, err := url.Parse(cnf.Broker)
	if err != nil {
		errorsChan <- err
		return
	}
	backend_url, err := url.Parse(cnf.ResultBackend)
	if err != nil {
		errorsChan <- err
		return
	}
	// setting runtime configuration
	if cnf.Maxcpucores > 0 {
		runtime.GOMAXPROCS(cnf.Maxcpucores)
	}
	if cnf.Maxthreads > 0 {
		debug.SetMaxThreads(cnf.Maxthreads)
	}
	// Log some useful information about worker configuration
	defer log.Sync()
	log.Info("Launching a worker with the following settings:")
	if cnf.Maxcpucores > 0 {
		log.Info("runtime info", zap.Int("maxcpucores", cnf.Maxcpucores))
	} else {
		log.Info("runtime info", zap.Int("maxcpucores", runtime.NumCPU()))
	}
	if cnf.Maxthreads > 0 {
		log.Info("runtime info", zap.Int("maxthreads", cnf.Maxthreads))
	} else {
		log.Info("runtime info", zap.Int("maxthreads", 10000))
	}
	log.Info("broker info",
		zap.String("broker.scheme", broker_url.Scheme),
		zap.String("broker.username", broker_url.User.Username()),
		zap.String("broker.host", broker_url.Host))
	log.Info("queue info", zap.String("default.queue.name", cnf.DefaultQueue))
	log.Info("result backend info",
		zap.String("backend.scheme", backend_url.Scheme),
		zap.String("backend.username", backend_url.User.Username()),
		zap.String("backend.host", backend_url.Host))
	if cnf.AMQP != nil {
		log.Info("amqp exchange info",
			zap.String("AMQP.Exchange", cnf.AMQP.Exchange),
			zap.String("AMQP.ExchangeType", cnf.AMQP.ExchangeType),
			zap.String("AMQP.BindingKey", cnf.AMQP.BindingKey),
			zap.Int("AMQP.PrefetchCount", cnf.AMQP.PrefetchCount))
		log.Info("worker info", zap.Int("worker.Concurrency", worker.Concurrency))
	}

	// Goroutine to start broker consumption and handle retries when broker connection dies
	go func() {
		defer log.Sync()
		for {
			retry, err := broker.StartConsuming(worker.ConsumerTag, worker.Concurrency, worker)

			if retry {
				if worker.errorHandler != nil {
					worker.errorHandler(err)
				} else {
					log.Warn("Broker failed with error", zap.Error(err))
				}
			} else {
				errorsChan <- err // stop the goroutine
				return
			}
		}
	}()
	if !cnf.NoUnixSignals {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		var signalsReceived uint

		// Goroutine Handle SIGINT and SIGTERM signals
		go func() {
			for {
				select {
				case s := <-sig:
					log.Warn("Signal received", zap.Any("signal", s))
					signalsReceived++

					if signalsReceived < 2 {
						// After first Ctrl+C start quitting the worker gracefully
						log.Warn("Waiting for running tasks to finish before shutting down")
						go func() {
							worker.Quit()
							errorsChan <- errors.New("Worker quit gracefully")
						}()
					} else {
						// Abort the program when user hits Ctrl+C second time in a row
						errorsChan <- errors.New("Worker quit abruptly")
					}
				}
			}
		}()
	}
}

// Quit tears down the running worker process
func (worker *Worker) Quit() {
	worker.server.GetBroker().StopConsuming()
}

func (worker *Worker) ProcessBatch(parent *tasks.Signature, signatures []*tasks.Signature, pool chan struct{}) error {
	// Update task state to RECEIVED
	if err := worker.server.GetBackend().SetStateReceived(parent); err != nil {
		return fmt.Errorf("Set state received error: %s", err)
	}
	type r struct {
		result []*tasks.TaskResult
		err    error
	}
	c := make(chan *r, len(signatures))
	for _, s := range signatures {
		if !worker.server.IsTaskRegistered(s.Name) {
			c <- &r{
				err: fmt.Errorf("task %s is not registered", s.Name),
			}
			continue
		}

		taskFunc, err := worker.server.GetRegisteredTask(s.Name)
		if err != nil {
			c <- &r{
				err: fmt.Errorf("task %s cannot fetch from registry", s.Name),
			}
			continue
		}

		task, err := tasks.New(taskFunc, s.Args)
		if err != nil {
			c <- &r{
				err: fmt.Errorf("task %s cannot created", s.Name),
			}
			continue
		}
		<-pool
		go func(ch chan *r, task *tasks.Task) {
			results, err := task.Call()
			res := new(r)
			res.result = results
			res.err = err
			ch <- res
			pool <- struct{}{}
		}(c, task)
	}
	br := []*tasks.TaskResult{}
	for _, _ = range signatures {
		res := <-c
		if res.err == nil {
			br = append(br, &tasks.TaskResult{
				Type:  "[]*tasks.TaskResult",
				Value: res.result,
			})
		} else {
			br = append(br, &tasks.TaskResult{
				Type:  "string",
				Value: res.err.Error(),
			},
			)
		}
	}
	return worker.taskSucceeded(parent, br)
}

// Process handles received tasks and triggers success/error callbacks
func (worker *Worker) Process(signature *tasks.Signature) error {
	// If the task is not registered with this worker, do not continue
	// but only return nil as we do not want to restart the worker process
	if !worker.server.IsTaskRegistered(signature.Name) {
		return nil
	}

	taskFunc, err := worker.server.GetRegisteredTask(signature.Name)
	if err != nil {
		return nil
	}

	// Update task state to RECEIVED
	if err = worker.server.GetBackend().SetStateReceived(signature); err != nil {
		return fmt.Errorf("Set state received error: %s", err)
	}

	// Prepare task for processing
	task, err := tasks.New(taskFunc, signature.Args)
	// if this failed, it means the task is malformed, probably has invalid
	// signature, go directly to task failed without checking whether to retry
	if err != nil {
		worker.taskFailed(signature, err)
		return err
	}

	// try to extract trace span from headers and add it to the function context
	// so it can be used inside the function if it has context.Context as the first
	// argument. Start a new span if it isn't found.
	taskSpan := tracing.StartSpanFromHeaders(signature.Headers, signature.Name)
	tracing.AnnotateSpanWithSignatureInfo(taskSpan, signature)
	task.Context = opentracing.ContextWithSpan(task.Context, taskSpan)

	// Update task state to STARTED
	if err = worker.server.GetBackend().SetStateStarted(signature); err != nil {
		return fmt.Errorf("Set state started error: %s", err)
	}

	// Call the task
	results, err := task.Call()
	if err != nil {
		// If a tasks.ErrRetryTaskLater was returned from the task,
		// retry the task after specified duration
		retriableErr, ok := interface{}(err).(tasks.ErrRetryTaskLater)
		if ok {
			return worker.retryTaskIn(signature, retriableErr.RetryIn())
		}

		// Otherwise, execute default retry logic based on signature.RetryCount
		// and signature.RetryTimeout values
		if signature.RetryCount > 0 {
			return worker.taskRetry(signature)
		}

		return worker.taskFailed(signature, err)
	}

	return worker.taskSucceeded(signature, results)
}

// retryTask decrements RetryCount counter and republishes the task to the queue
func (worker *Worker) taskRetry(signature *tasks.Signature) error {
	// Update task state to RETRY
	if err := worker.server.GetBackend().SetStateRetry(signature); err != nil {
		return fmt.Errorf("Set state retry error: %s", err)
	}

	// Decrement the retry counter, when it reaches 0, we won't retry again
	signature.RetryCount--

	// Increase retry timeout
	signature.RetryTimeout = retry.FibonacciNext(signature.RetryTimeout)

	// Delay task by signature.RetryTimeout seconds
	eta := time.Now().UTC().Add(time.Second * time.Duration(signature.RetryTimeout))
	signature.ETA = &eta
	defer log.Sync()
	log.Warn("Task failed. Retrying", zap.String("signature.UUID", signature.UUID), zap.Int("signature.RetryTimeout", signature.RetryTimeout))

	// Send the task back to the queue
	_, err := worker.server.SendTask(signature)
	return err
}

// taskRetryIn republishes the task to the queue with ETA of now + retryIn.Seconds()
func (worker *Worker) retryTaskIn(signature *tasks.Signature, retryIn time.Duration) error {
	// Update task state to RETRY
	if err := worker.server.GetBackend().SetStateRetry(signature); err != nil {
		return fmt.Errorf("Set state retry error: %s", err)
	}

	// Delay task by retryIn duration
	eta := time.Now().UTC().Add(retryIn)
	signature.ETA = &eta
	defer log.Sync()
	log.Warn("Task failed. Retrying", zap.String("signature.UUID", signature.UUID), zap.Float64("retryIn.Seconds()", retryIn.Seconds()))

	// Send the task back to the queue
	_, err := worker.server.SendTask(signature)
	return err
}

// taskSucceeded updates the task state and triggers success callbacks or a
// chord callback if this was the last task of a group with a chord callback
func (worker *Worker) taskSucceeded(signature *tasks.Signature, taskResults []*tasks.TaskResult) error {
	// Update task state to SUCCESS
	if err := worker.server.GetBackend().SetStateSuccess(signature, taskResults); err != nil {
		return fmt.Errorf("Set state success error: %s", err)
	}

	// Log human readable results of the processed task
	var debugResults = "[]"
	defer log.Sync()
	results, err := tasks.ReflectTaskResults(taskResults)
	if err != nil {
		log.Warn("reflect task result failed", zap.Error(err))
	} else {
		debugResults = tasks.HumanReadableResults(results)
	}
	log.Debug("task processed", zap.String("signature.UUID", signature.UUID), zap.String("debugResult", debugResults))

	// Trigger success callbacks

	for _, successTask := range signature.OnSuccess {
		if successTask.Immutable == false {
			// Pass results of the task to success callbacks
			for _, taskResult := range taskResults {
				if taskResult.Type != "[]*tasks.TaskResult" {
					successTask.Args = append(successTask.Args, tasks.Arg{
						Type:  taskResult.Type,
						Value: taskResult.Value,
					})
				} else {
					if trs, ok := taskResult.Value.([]*tasks.TaskResult); ok {
						for _, tr := range trs {
							successTask.Args = append(successTask.Args, tasks.Arg{
								Type:  tr.Type,
								Value: tr.Value,
							})
						}
					} else {
						return fmt.Errorf("Inconsistent result value type %s", taskResult.Value)
					}
				}
			}
		}
		worker.server.SendTask(successTask)
	}

	// If the task was not part of a group, just return
	if signature.GroupUUID == "" || signature.GroupTaskCount == 0 {
		return nil
	}

	// Check if all task in the group has completed
	groupCompleted, err := worker.server.GetBackend().GroupCompleted(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return fmt.Errorf("Group completed error: %s", err)
	}

	// If the group has not yet completed, just return
	if !groupCompleted {
		return nil
	}

	// Defer purging of group meta queue if we are using AMQP backend
	if worker.hasAMQPBackend() || worker.hasRedisCeleryBackend() {
		defer worker.server.GetBackend().PurgeGroupMeta(signature.GroupUUID)
	}

	// There is no chord callback, just return
	if signature.ChordCallback == nil {
		return nil
	}

	// Trigger chord callback
	shouldTrigger, err := worker.server.GetBackend().TriggerChord(signature.GroupUUID)
	if err != nil {
		return fmt.Errorf("Trigger chord error: %s", err)
	}

	// Chord has already been triggered
	if !shouldTrigger {
		return nil
	}

	// Get task states
	taskStates, err := worker.server.GetBackend().GroupTaskStates(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return nil
	}

	// Append group tasks' return values to chord task if it's not immutable
	for _, taskState := range taskStates {
		if !taskState.IsSuccess() {
			return nil
		}

		if signature.ChordCallback.Immutable == false {
			// Pass results of the task to the chord callback
			for _, taskResult := range taskState.Results {
				if taskResult.Type != "[]*tasks.TaskResult" {
					signature.ChordCallback.Args = append(signature.ChordCallback.Args, tasks.Arg{
						Type:  taskResult.Type,
						Value: taskResult.Value,
					})
				} else {
					if trs, ok := taskResult.Value.([]interface{}); ok {
						for _, tr := range trs {
							if t, tok := tr.(map[string]interface{}); tok {
								signature.ChordCallback.Args = append(signature.ChordCallback.Args, tasks.Arg{
									Type:  t["Type"].(string),
									Value: t["Value"],
								})
							}
						}
					} else {
						return fmt.Errorf("Inconsistent result value type %s", taskResult.Value)
					}
				}
			}
		}
	}

	// Send the chord task
	_, err = worker.server.SendTask(signature.ChordCallback)
	if err != nil {
		return err
	}

	return nil
}

// taskFailed updates the task state and triggers error callbacks
func (worker *Worker) taskFailed(signature *tasks.Signature, taskErr error) error {
	// Update task state to FAILURE
	if err := worker.server.GetBackend().SetStateFailure(signature, taskErr.Error()); err != nil {
		return fmt.Errorf("Set state failure error: %s", err)
	}
	defer log.Sync()
	if worker.errorHandler != nil {
		worker.errorHandler(taskErr)
	} else {
		log.Error("failed to process task", zap.String("signature.UUID", signature.UUID), zap.Error(taskErr))
	}

	// Trigger error callbacks
	for _, errorTask := range signature.OnError {
		// Pass error as a first argument to error callbacks
		args := append([]tasks.Arg{{
			Type:  "string",
			Value: taskErr.Error(),
		}}, errorTask.Args...)
		errorTask.Args = args
		worker.server.SendTask(errorTask)
	}

	return nil
}

// Returns true if the worker uses AMQP backend
func (worker *Worker) hasAMQPBackend() bool {
	return false
}

// Returns true if the worker uses Redis backend for celery
func (worker *Worker) hasRedisCeleryBackend() bool {
	_, ok := worker.server.GetBackend().(*backends.RedisCeleryBackend)
	return ok
}

// SetErrorHandler sets a custom error handler for task errors
// A default behavior is just to log the error after all the retry attempts fail
func (worker *Worker) SetErrorHandler(handler func(err error)) {
	worker.errorHandler = handler
}
