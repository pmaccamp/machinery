package tasks

import (
	"context"
	"errors"
	"fmt"
	"github.com/bugsnag/bugsnag-go"
	"reflect"
	"runtime/debug"

	"github.com/opentracing/opentracing-go"
	opentracing_ext "github.com/opentracing/opentracing-go/ext"
	opentracing_log "github.com/opentracing/opentracing-go/log"

	"github.com/pmaccamp/machinery/v1/log"
	"github.com/pmaccamp/machinery/v1/stackframe"
)

// ErrTaskPanicked ...
var ErrTaskPanicked = errors.New("Invoking task caused a panic")

// Task wraps a signature and methods used to reflect task arguments and
// return values after invoking the task
type Task struct {
	TaskFunc      reflect.Value
	UseContext    bool
	Context       context.Context
	Args          []reflect.Value
	BugsnagConfig *bugsnag.Configuration
	Signature     *Signature
}

// New tries to use reflection to convert the function and arguments
// into a reflect.Value and prepare it for invocation
func New(bugsnagConfig *bugsnag.Configuration, signature *Signature, taskFunc interface{}, args []interface{}) (*Task, error) {
	var taskFuncValue = reflect.ValueOf(taskFunc)
	task := &Task{
		TaskFunc:      taskFuncValue,
		Context:       context.Background(),
		BugsnagConfig: bugsnagConfig,
		Signature:     signature,
	}

	taskFuncType := reflect.TypeOf(taskFunc)
	if taskFuncType.NumIn() > 0 {
		arg0Type := taskFuncType.In(0)
		if IsContextType(arg0Type) {
			task.UseContext = true
		}
	}

	if err := task.ReflectArgs(args, &taskFuncValue); err != nil {
		return nil, fmt.Errorf("Reflect task args error: %s", err)
	}

	return task, nil
}

// Call attempts to call the task with the supplied arguments.
//
// `err` is set in the return value in two cases:
// 1. The reflected function invocation panics (e.g. due to a mismatched
//    argument list).
// 2. The task func itself returns a non-nil error.
func (t *Task) Call() (taskResults []*TaskResult, err error, stackFrames []stackframe.StackFrame) {
	// retrieve the span from the task's context and finish it as soon as this function returns
	if span := opentracing.SpanFromContext(t.Context); span != nil {
		defer span.Finish()
	}

	defer func() {
		// Recover from panic and set err.
		if e := recover(); e != nil {
			switch e := e.(type) {
			default:
				err = ErrTaskPanicked
			case error:
				err = e
			case string:
				err = errors.New(e)
			}

			// mark the span as failed and dump the error and stack trace to the span
			if span := opentracing.SpanFromContext(t.Context); span != nil {
				opentracing_ext.Error.Set(span, true)
				span.LogFields(
					opentracing_log.Error(err),
					opentracing_log.Object("stack", string(debug.Stack())),
				)
			}

			if t.BugsnagConfig != nil {
				bugsnag.Configure(*t.BugsnagConfig)

				if val, ok := t.Signature.Kwargs["user_id"]; ok {

					_ = bugsnag.Notify(err,
						bugsnag.MetaData{
							"data": {
								"signature": t.Signature,
								"context":   t.Context,
							},
						},
						bugsnag.User{
							Id: fmt.Sprintf("%v", val),
						},
					)
				} else {
					_ = bugsnag.Notify(err,
						bugsnag.MetaData{
							"data": {
								"signature": t.Signature,
								"context":   t.Context,
							},
						})
				}
			}

			stackFrames = stackframe.CurrentStackFrames()

			// Print stack trace
			log.ERROR.Printf("%s", debug.Stack())
		}
	}()

	args := t.Args

	if t.UseContext {
		ctxValue := reflect.ValueOf(t.Context)
		args = append([]reflect.Value{ctxValue}, args...)
	}

	// Invoke the task
	results := t.TaskFunc.Call(args)

	// Task must return at least a value
	if len(results) == 0 {
		return nil, ErrTaskReturnsNoValue, stackFrames
	}

	// Last returned value
	lastResult := results[len(results)-1]

	// If the last returned value is not nil, it has to be of error type, if that
	// is not the case, return error message, otherwise propagate the task error
	// to the caller
	if !lastResult.IsNil() {
		// If the result implements Retriable interface, return instance of Retriable
		retriableErrorInterface := reflect.TypeOf((*Retriable)(nil)).Elem()
		if lastResult.Type().Implements(retriableErrorInterface) {
			return nil, lastResult.Interface().(ErrRetryTaskLater), stackFrames
		}

		// Otherwise, check that the result implements the standard error interface,
		// if not, return ErrLastReturnValueMustBeError error
		errorInterface := reflect.TypeOf((*error)(nil)).Elem()
		if !lastResult.Type().Implements(errorInterface) {
			return nil, ErrLastReturnValueMustBeError, stackFrames
		}

		// Return the standard error
		return nil, lastResult.Interface().(error), stackFrames
	}

	// Convert reflect values to task results
	taskResults = make([]*TaskResult, len(results)-1)
	for i := 0; i < len(results)-1; i++ {
		val := results[i].Interface()
		typeStr := reflect.TypeOf(val).String()
		taskResults[i] = &TaskResult{
			Type:  typeStr,
			Value: val,
		}
	}

	return taskResults, err, stackFrames
}

// ReflectArgs converts []TaskArg to []reflect.Value
func (t *Task) ReflectArgs(args []interface{}, taskFunc *reflect.Value) error {
	argValues := make([]reflect.Value, len(args))

	numArgs := taskFunc.Type().NumIn()
	if numArgs != len(args) {
		return fmt.Errorf("Number of task arguments %d does not match number of message arguments %d", numArgs, len(args))
	}
	// construct arguments
	for i, arg := range args {
		origType := taskFunc.Type().In(i).Kind()
		msgType := reflect.TypeOf(arg).Kind()
		// special case - convert float64 to int if applicable
		// this is due to json limitation where all numbers are converted to float64
		if origType == reflect.Int && msgType == reflect.Float64 {
			argValues[i] = reflect.ValueOf(int(arg.(float64)))
		} else {
			argValues[i] = reflect.ValueOf(arg)
		}
	}

	t.Args = argValues
	return nil
}
