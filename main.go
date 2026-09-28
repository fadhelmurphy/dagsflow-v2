package main

import (
	_ "dagsflow-go/builtin_dags" // DAGs compiled into the binary (no Go toolchain needed at runtime)
	"dagsflow-go/cmd"
)

func main() {
	cmd.Execute()
}
