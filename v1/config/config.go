package config

import (
	"cloud.google.com/go/pubsub"
	"crypto/tls"
	"fmt"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/bugsnag/bugsnag-go"
	"strings"
	"time"
)

const (
	// DefaultResultsExpireIn is a default time used to expire task states and group metadata from the backend
	DefaultResultsExpireIn = 24 * 3600
)

var (
	// Start with sensible default values
	defaultCnf = &Config{
		Broker:          "amqp://guest:guest@localhost:5672/",
		DefaultQueue:    "machinery_tasks",
		ResultBackend:   "amqp://guest:guest@localhost:5672/",
		ResultsExpireIn: DefaultResultsExpireIn,
		AMQP: &AMQPConfig{
			Exchange:      "machinery_exchange",
			ExchangeType:  "direct",
			BindingKey:    "machinery_task",
			PrefetchCount: 3,
		},
		DynamoDB: &DynamoDBConfig{
			TaskStatesTable: "task_states",
			GroupMetasTable: "group_metas",
		},
		GCPPubSub: &GCPPubSubConfig{
			Client: nil,
		},
	}

	reloadDelay = time.Second * 10
)

// Config holds all configuration for our program
type Config struct {
	Broker          string           `yaml:"broker" envconfig:"BROKER"`
	DefaultQueue    string           `yaml:"default_queue" envconfig:"DEFAULT_QUEUE"`
	ResultBackend   string           `yaml:"result_backend" envconfig:"RESULT_BACKEND"`
	ResultsExpireIn int              `yaml:"results_expire_in" envconfig:"RESULTS_EXPIRE_IN"`
	AMQP            *AMQPConfig      `yaml:"amqp"`
	SQS             *SQSConfig       `yaml:"sqs"`
	GCPPubSub       *GCPPubSubConfig `yaml:"-" ignored:"true"`
	TLSConfig       *tls.Config
	BugsnagConfig   *bugsnag.Configuration
	// NoUnixSignals - when set disables signal handling in machinery
	NoUnixSignals bool            `yaml:"no_unix_signals" envconfig:"NO_UNIX_SIGNALS"`
	DynamoDB      *DynamoDBConfig `yaml:"dynamodb"`
}

// QueueBindingArgs arguments which are used when binding to the exchange
type QueueBindingArgs map[string]interface{}

// AMQPConfig wraps RabbitMQ related configuration
type AMQPConfig struct {
	Exchange         string           `yaml:"exchange" envconfig:"AMQP_EXCHANGE"`
	ExchangeType     string           `yaml:"exchange_type" envconfig:"AMQP_EXCHANGE_TYPE"`
	QueueBindingArgs QueueBindingArgs `yaml:"queue_binding_args" envconfig:"AMQP_QUEUE_BINDING_ARGS"`
	BindingKey       string           `yaml:"binding_key" envconfig:"AMQP_BINDING_KEY"`
	PrefetchCount    int              `yaml:"prefetch_count" envconfig:"AMQP_PREFETCH_COUNT"`
}

// DynamoDBConfig wraps DynamoDB related configuration
type DynamoDBConfig struct {
	TaskStatesTable string `yaml:"task_states_table" envconfig:"TASK_STATES_TABLE"`
	GroupMetasTable string `yaml:"group_metas_table" envconfig:"GROUP_METAS_TABLE"`
}

// SQSConfig wraps SQS related configuration
type SQSConfig struct {
	Client          *sqs.SQS
	WaitTimeSeconds int `yaml:"receive_wait_time_seconds" envconfig:"SQS_WAIT_TIME_SECONDS"`
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
	// visibility timeout should default to nil to use the overall visibility timeout for the queue
	VisibilityTimeout *int `yaml:"receive_visibility_timeout" envconfig:"SQS_VISIBILITY_TIMEOUT"`
}

// GCPPubSubConfig wraps GCP PubSub related configuration
type GCPPubSubConfig struct {
	Client *pubsub.Client
}

// Decode from yaml to map (any field whose type or pointer-to-type implements
// envconfig.Decoder can control its own deserialization)
func (args *QueueBindingArgs) Decode(value string) error {
	pairs := strings.Split(value, ",")
	mp := make(map[string]interface{}, len(pairs))
	for _, pair := range pairs {
		kvpair := strings.Split(pair, ":")
		if len(kvpair) != 2 {
			return fmt.Errorf("invalid map item: %q", pair)
		}
		mp[kvpair[0]] = kvpair[1]
	}
	*args = QueueBindingArgs(mp)
	return nil
}
