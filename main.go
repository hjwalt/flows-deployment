package main

import (
	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows-deployment/example/example_word_collect"
	"github.com/hjwalt/flows-deployment/example/example_word_count"
	"github.com/hjwalt/flows-deployment/example/example_word_join"
	"github.com/hjwalt/flows-deployment/example/example_word_materialise"
	"github.com/hjwalt/flows-deployment/example/example_word_remap"
	"github.com/hjwalt/runway/configuration"
	"github.com/hjwalt/runway/environment"
	"github.com/hjwalt/runway/format"
)

type Config struct {
	Name   string `yaml:"name"`
	Number int64  `yaml:"number"`
}

func main() {
	m := flows.NewMain()

	configuration.Read("config.yaml", format.Yaml[Config]())

	example_word_collect.Register(m)
	example_word_count.Register(m)
	example_word_join.Register(m)
	example_word_materialise.Register(m)
	example_word_remap.Register(m)

	err := m.Start(environment.GetString("INSTANCE", flows.AllInstances))

	if err != nil {
		panic(err)
	}
}
