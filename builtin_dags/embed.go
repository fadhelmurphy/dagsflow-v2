package builtindags

import (
	"embed"

	"dagsflow-go/dag"
)

//go:embed *.go
var sources embed.FS

func init() { dag.SetBuiltinSources(sources) }
