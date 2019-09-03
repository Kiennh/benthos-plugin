package output

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/output"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"

	"github.com/kshvakov/clickhouse"
)

func init() {
	output.RegisterPlugin(
		"clickhouse",
		func() interface{} {
			return NewClickHouseConfig()
		},
		func(iconf interface{}, mgr types.Manager, logger log.Modular, stats metrics.Type) (types.Output, error) {
			conf, ok := iconf.(*ClickHouseConfig)
			if !ok {
				return nil, errors.New("failed to cast config")
			}
			return NewClickHouse(*conf, mgr, logger, stats)
		},
	)

	output.DocumentPlugin(
		"clickhouse",
		`
This plugin write output to clickhouse!`,
		nil,
	)
}

type ClickHouseConfig struct {
	ConncetionString string   `json:"connection_string" yaml:"connection_string"`
	Query            string   `json:"query" yaml:"query"`
	Columns          []string `json:"columns" yaml:"columns"`
}

// NewGibberishConfig creates a config with default values.
func NewClickHouseConfig() *ClickHouseConfig {
	return &ClickHouseConfig{
		ConncetionString: "tcp://127.0.0.1:9000?debug=true",
	}
}

//------------------------------------------------------------------------------

// ClickHouse is an example plugin that creates gibberish messages.
type ClickHouse struct {
	conncetionString string
	query            string

	// process
	connect   *sql.DB
	insertSQL string
	tx        *sql.Tx
	stmt      *sql.Stmt
	columns   []*InterpolatedAll

	transactionsChan <-chan types.Transaction

	log   log.Modular
	stats metrics.Type

	closeOnce  sync.Once
	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewClickHouse create a new clickhouse plugin output type.
func NewClickHouse(
	conf ClickHouseConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (output.Type, error) {

	connect, err := sql.Open("clickhouse", conf.ConncetionString)
	if err != nil {
		return nil, err
	}

	var args []*InterpolatedAll
	for _, v := range conf.Columns {
		args = append(args, NewInterpolatedAll(v))
	}

	e := &ClickHouse{
		conncetionString: conf.ConncetionString,
		columns:          args,
		connect:          connect,
		insertSQL:        conf.Query,

		log:   log,
		stats: stats,

		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}

	return e, nil
}

//------------------------------------------------------------------------------

func (e *ClickHouse) loop() {
	defer func() {
		close(e.closedChan)
	}()

	for {
		var tran types.Transaction
		var open bool
		var err error

		select {
		case tran, open = <-e.transactionsChan:
			if !open {
				return
			}
		case <-e.closeChan:
			return
		}

		// re-create  transaction after commit, for the next loop
		e.tx, err = e.connect.Begin()
		if err == nil {
			e.stmt, err = e.tx.Prepare(e.insertSQL)
			defer e.stmt.Close()
			if err == nil {
				err = tran.Payload.Iter(func(i int, p types.Part) error {
					jObj, err := p.JSON()
					if err != nil {
						return err
					}
					obj, ok := jObj.(map[string]interface{})
					if !ok {
						return fmt.Errorf("not ok")
					}
					var data []interface{}
					data = make([]interface{}, len(e.columns))

					for i, c := range e.columns {
						data[i] = c.conv(obj)
					}
					_, err = e.stmt.Exec(data...)
					return err
				})
				if err == nil {
					err = e.tx.Commit()
				}
			}
		}

		if err != nil {
			e.log.Errorf("%v", err)
		}

		select {
		case tran.ResponseChan <- response.NewAck():
		case <-e.closeChan:
			return
		}
	}
}

// Connected returns true if this output is currently connected to its target.
func (e *ClickHouse) Connected() bool {

	isConnect := false

	if err := e.connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Printf("%v \n", err)
		}
		isConnect = true
	}

	return isConnect
}

// Consume starts this output consuming from a transaction channel.
func (e *ClickHouse) Consume(tChan <-chan types.Transaction) error {
	e.transactionsChan = tChan

	go e.loop()
	return nil
}

// CloseAsync shuts down the output and stops processing requests.
func (e *ClickHouse) CloseAsync() {
	e.closeOnce.Do(func() {
		close(e.closeChan)
		// commit the last one
		e.tx.Commit()
		e.stmt.Close()
		e.connect.Close()
	})
}

// WaitForClose blocks until the output has closed down.
func (e *ClickHouse) WaitForClose(timeout time.Duration) error {
	select {
	case <-e.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}
