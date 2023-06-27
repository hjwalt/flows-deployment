package main

import (
	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows-deployment/fn_join"
	"github.com/hjwalt/flows-deployment/fn_materialise"
	"github.com/hjwalt/flows-deployment/function"
	"github.com/hjwalt/flows/configuration"
	"github.com/hjwalt/flows/format"
	"github.com/hjwalt/runway/environment"
)

type Config struct {
	Name   string `yaml:"name"`
	Number int64  `yaml:"number"`
}

func main() {
	m := flows.Main()

	configuration.Read("config.yaml", format.Yaml[Config]())

	m.Register("word-count", function.WordCountRun)
	m.Register("word-remap", function.WordRemapRun)
	m.Register("word-join", fn_join.Runtime)
	m.Register("materialise", fn_materialise.Runtime)

	err := m.Start(environment.GetString("INSTANCE", "word-count"))

	if err != nil {
		panic(err)
	}
}
