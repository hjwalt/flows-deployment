package function

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

func WordJoinPersistenceId(ctx context.Context, m message.Message[message.Bytes, message.Bytes]) (string, error) {
	return string(m.Key), nil
}

func WordJoinCountFunction(c context.Context, m message.Message[message.Bytes, message.Bytes], inState stateful.SingleState[message.Bytes]) ([]message.Message[message.Bytes, message.Bytes], stateful.SingleState[message.Bytes], error) {
	logger.Info("applying")

	// format conversion to something more usable, will be abstracted out also in the future
	protoState, protoStateMapErr := stateful.ConvertSingleState(inState, format.Bytes(), format.Protobuf[*function_protobuf.WordJoinState]())
	if protoStateMapErr != nil {
		return make([]message.Message[[]byte, []byte], 0), inState, protoStateMapErr
	}

	// setting defaults
	if protoState.Content == nil {
		protoState.Content = &function_protobuf.WordJoinState{Count: 0, Word: ""}
	}

	// update state
	protoState.Content.Count += 1

	logger.Info("info", zap.Int64("count", protoState.Content.Count), zap.String("word", protoState.Content.Word))

	// map back to bytes
	nextByteState, nextByteStateMapErr := stateful.ConvertSingleState(protoState, format.Protobuf[*function_protobuf.WordJoinState](), format.Bytes())
	if nextByteStateMapErr != nil {
		return make([]message.Message[[]byte, []byte], 0), inState, nextByteStateMapErr
	}

	// create output message
	outMessage := message.Message[message.Bytes, message.Bytes]{
		Topic: "word-join",
		Key:   m.Key,
		Value: []byte(reflect.GetString(protoState.Content.Count) + " " + protoState.Content.Word),
	}

	return []message.Message[[]byte, []byte]{outMessage}, nextByteState, nil
}

func WordJoinWordFunction(c context.Context, m message.Message[message.Bytes, message.Bytes], inState stateful.SingleState[message.Bytes]) ([]message.Message[message.Bytes, message.Bytes], stateful.SingleState[message.Bytes], error) {
	logger.Info("applying")

	// format conversion to something more usable, will be abstracted out also in the future
	protoState, protoStateMapErr := stateful.ConvertSingleState(inState, format.Bytes(), format.Protobuf[*function_protobuf.WordJoinState]())
	if protoStateMapErr != nil {
		return make([]message.Message[[]byte, []byte], 0), inState, protoStateMapErr
	}

	// setting defaults
	if protoState.Content == nil {
		protoState.Content = &function_protobuf.WordJoinState{Count: 0, Word: ""}
	}

	// update state
	protoState.Content.Word = string(m.Value)

	logger.Info("info", zap.Int64("count", protoState.Content.Count), zap.String("word", protoState.Content.Word))

	// map back to bytes
	nextByteState, nextByteStateMapErr := stateful.ConvertSingleState(protoState, format.Protobuf[*function_protobuf.WordJoinState](), format.Bytes())
	if nextByteStateMapErr != nil {
		return make([]message.Message[[]byte, []byte], 0), inState, nextByteStateMapErr
	}

	// create output message
	outMessage := message.Message[message.Bytes, message.Bytes]{
		Topic: "word-join",
		Key:   m.Key,
		Value: []byte(reflect.GetString(protoState.Content.Count) + " " + protoState.Content.Word),
	}

	return []message.Message[[]byte, []byte]{outMessage}, nextByteState, nil
}

func WordJoinRun() runtime.Runtime {
	joinFunctionConfiguration := flows.JoinPostgresqlFunctionConfiguration{

		StatefulFunctions: map[string]stateful.SingleFunction{
			"word":      WordJoinCountFunction,
			"word-type": WordJoinWordFunction,
		},
		PersistenceIdFunctions: map[string]stateful.PersistenceIdFunction[message.Bytes, message.Bytes]{
			"word":      WordJoinPersistenceId,
			"word-type": WordJoinPersistenceId,
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
