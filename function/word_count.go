package function

import (
	"context"
	"net/http"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows-deployment/function_protobuf"
	"github.com/hjwalt/flows/format"
	"github.com/hjwalt/flows/message"
	"github.com/hjwalt/flows/protobuf"
	"github.com/hjwalt/flows/router"
	"github.com/hjwalt/flows/runtime"
	"github.com/hjwalt/flows/runtime_bun"
	"github.com/hjwalt/flows/runtime_bunrouter"
	"github.com/hjwalt/flows/runtime_sarama"
	"github.com/hjwalt/flows/stateful"
	"github.com/hjwalt/runway/logger"
	"github.com/hjwalt/runway/reflect"
	"github.com/uptrace/bunrouter"
	"go.uber.org/zap"
)

func WordCountPersistenceId(ctx context.Context, m message.Message[string, string]) (string, error) {
	return m.Key, nil
}

func WordCountStatefulFunction(c context.Context, m message.Message[string, string], s stateful.SingleState[*function_protobuf.WordCountState]) (*message.Message[string, string], stateful.SingleState[*function_protobuf.WordCountState], error) {
	logger.Info("applying")

	// setting defaults
	if s.Content == nil {
		s.Content = &function_protobuf.WordCountState{Count: 0}
	}

	// update state
	s.Content.Count += 1

	logger.Info("count", zap.Int64("count", s.Content.Count))

	// create output message
	outMessage := message.Message[string, string]{
		Topic: "word-count",
		Key:   m.Key,
		Value: reflect.GetString(s.Content.Count),
	}

	return &outMessage, s, nil
}

func WordCountRun() runtime.Runtime {
	statefulFunctionConfiguration := flows.StatefulPostgresqlFunctionConfiguration{
		StatefulFunction: stateful.ConvertOneToOne(
			WordCountStatefulFunction,
			format.Protobuf[*function_protobuf.WordCountState](),
			format.String(),
			format.String(),
			format.String(),
			format.String(),
		),
		PersistenceIdFunction: stateful.ConvertPersistenceId(
			WordCountPersistenceId,
			format.String(),
			format.String(),
		),
		PersistenceTableName: "public.flows_state",

		PostgresqlConfiguration: []runtime.Configuration[*runtime_bun.PostgresqlConnection]{
			runtime_bun.WithApplicationName("flows"),
			runtime_bun.WithConnectionString("postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"),
		},
		KafkaProducerConfiguration: []runtime.Configuration[*runtime_sarama.Producer]{
			runtime_sarama.WithProducerSaramaConfig(runtime_sarama.DefaultConfiguration()),
			runtime_sarama.WithProducerBroker("localhost:9092"),
		},
		KafkaConsumerConfiguration: []runtime.Configuration[*runtime_sarama.Consumer]{
			runtime_sarama.WithConsumerSaramaConfig(runtime_sarama.DefaultConfiguration()),
			runtime_sarama.WithConsumerBroker("localhost:9092"),
			runtime_sarama.WithConsumerTopic("word"),
			runtime_sarama.WithConsumerGroupName("test"),
		},
		RouteConfiguration: []runtime.Configuration[*runtime_bunrouter.Router]{
			runtime_bunrouter.WithRouterGroup("/api"),
			runtime_bunrouter.WithRouterBunHandler(runtime_bunrouter.GET, "/dummy", func(w http.ResponseWriter, req bunrouter.Request) error {
				state := &protobuf.State{
					State: &protobuf.State_V1{
						V1: &protobuf.StateV1{
							OffsetProgress: map[int32]int64{
								1: 1,
							},
						},
					},
				}

				return router.WriteJson(w, 200, state, format.Protobuf[*protobuf.State]())
			}),
			runtime_bunrouter.WithRouterBunHandler(runtime_bunrouter.GET, "/test", func(w http.ResponseWriter, req bunrouter.Request) error {
				state := TestResponse{
					Message: "test",
				}

				return router.WriteJson(w, 200, state, format.Json[TestResponse]())
			}),
		},
	}

	return statefulFunctionConfiguration.Runtime()
}

type TestResponse struct {
	Message string
}
