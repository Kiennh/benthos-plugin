package main

import (
	"github.com/Jeffail/benthos/lib/service"

	_ "github.com/kiennh/benthos-output-clickhouse/output"
)

func main() {
	service.Run()
}
