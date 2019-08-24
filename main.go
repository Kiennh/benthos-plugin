package main

import (
	"github.com/Jeffail/benthos/lib/service"

	_ "github.com/kiennh/benthos-plugin/output"
	_ "github.com/kiennh/benthos-plugin/processor"
)

func main() {
	service.Run()
}
