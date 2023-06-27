package fn_join

import (
	"context"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows-deployment/function_protobuf"
	"github.com/hjwalt/flows/format"
	"github.com/hjwalt/flows/message"
	"github.com/hjwalt/flows/runtime"
	"github.com/hjwalt/flows/runtime_bun"
	"github.com/hjwalt/flows/runtime_sarama"
	"github.com/hjwalt/flows/stateful"
	"github.com/hjwalt/runway/logger"
	"github.com/hjwalt/runway/reflect"
	"go.uber.org/zap"
)

func WordJoinPersistenceId(ctx context.Context, m message.Message[string, string]) (string, error) {
	return m.Key, nil
}

func WordJoinCountFunction(c context.Context, m message.Message[string, string], s stateful.SingleState[*function_protobuf.WordJoinState]) (*message.Message[string, string], stateful.SingleState[*function_protobuf.WordJoinState], error) {
	logger.Info("applying")

	// setting defaults
	if s.Content == nil {
		s.Content = &function_protobuf.WordJoinState{Count: 0, Word: ""}
	}

	// update state
	s.Content.Count += 1

	logger.Info("info", zap.Int64("count", s.Content.Count), zap.String("word", s.Content.Word))

	// create output message
	outMessage := message.Message[string, string]{
		Topic: "word-join",
		Key:   m.Key,
		Value: reflect.GetString(s.Content.Count) + " " + s.Content.Word,
	}

	return &outMessage, s, nil
}

func WordJoinWordFunction(c context.Context, m message.Message[string, string], s stateful.SingleState[*function_protobuf.WordJoinState]) (*message.Message[string, string], stateful.SingleState[*function_protobuf.WordJoinState], error) {
	logger.Info("applying")

	// setting defaults
	if s.Content == nil {
		s.Content = &function_protobuf.WordJoinState{Count: 0, Word: ""}
	}

	// update state
	s.Content.Word = string(m.Value)

	logger.Info("info", zap.Int64("count", s.Content.Count), zap.String("word", s.Content.Word))

	// create output message
	outMessage := message.Message[string, string]{
		Topic: "word-join",
		Key:   m.Key,
		Value: reflect.GetString(s.Content.Count) + " " + s.Content.Word,
	}

	return &outMessage, s, nil
}

func Runtime() runtime.Runtime {
	joinFunctionConfiguration := flows.JoinPostgresqlFunctionConfiguration{

		StatefulFunctions: map[string]stateful.SingleFunction{
			"word": stateful.ConvertOneToOne(
				WordJoinCountFunction,
				format.Protobuf[*function_protobuf.WordJoinState](),
				format.String(),
				format.String(),
				format.String(),
				format.String(),
			),
			"word-type": stateful.ConvertOneToOne(
				WordJoinWordFunction,
				format.Protobuf[*function_protobuf.WordJoinState](),
				format.String(),
				format.String(),
				format.String(),
				format.String(),
			),
		},
		PersistenceIdFunctions: map[string]stateful.PersistenceIdFunction[message.Bytes, message.Bytes]{
			"word": stateful.ConvertPersistenceId(
				WordJoinPersistenceId,
				format.String(),
				format.String(),
			),
			"word-type": stateful.ConvertPersistenceId(
				WordJoinPersistenceId,
				format.String(),
				format.String(),
			),
		},

		IntermediateTopicName: "word-join-intermediate",
		PersistenceTableName:  "public.flows_state",

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
			runtime_sarama.WithConsumerGroupName("test"),
		},
	}

	return joinFunctionConfiguration.Runtime()
}
