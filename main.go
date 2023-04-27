package main

import (
	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows-deployment/function"
	"github.com/hjwalt/runway/environment"
)

func main() {
	m := flows.Main()

	m.Register("word-count", function.WordCountRun())
	m.Register("word-remap", function.WordRemapRun())
	m.Register("word-join", function.WordJoinRun())

	err := m.Start(environment.GetString("INSTANCE", "word-count"))

	if err != nil {
		panic(err)
	}
}
