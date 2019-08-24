package processor

import (
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/processor"
	"github.com/Jeffail/benthos/lib/types"

	"github.com/ua-parser/uap-go/uaparser"
)

//------------------------------------------------------------------------------

func init() {
	processor.RegisterPlugin(
		"useragent",
		func() interface{} {
			return NewUserAgentConfig()
		}, // No configuration needed
		func(
			iconf interface{},
			mgr types.Manager,
			logger log.Modular,
			stats metrics.Type,
		) (types.Processor, error) {
			conf, ok := iconf.(*UserAgentConfig)
			if !ok {
				return nil, errors.New("failed to cast config")
			}
			return NewUserAgent(*conf, logger, stats)
		},
	)
	processor.DocumentPlugin(
		"useragent",
		`useragents  processor.`,
		nil,
	)
}

//------------------------------------------------------------------------------

type UserAgentConfig struct {
	Field string `json:"field" yaml:"field"`
	File  string `json:"file" yaml:"file"`
}

// NewGibberishConfig creates a config with default values.
func NewUserAgentConfig() *UserAgentConfig {
	return &UserAgentConfig{}
}

// useragent is a processor that useragents all messages.
type UserAgent struct {
	field  string
	file   string
	parser *uaparser.Parser

	log   log.Modular
	stats metrics.Type
}

// Newuseragent returns a useragent processor.
func NewUserAgent(conf UserAgentConfig,
	log log.Modular, stats metrics.Type,
) (types.Processor, error) {

	parser, err := uaparser.New(conf.File)
	if err != nil {
		return nil, err
	}

	m := &UserAgent{
		field:  conf.Field,
		parser: parser,

		log:   log,
		stats: stats,
	}
	return m, nil
}

// ProcessMessage applies the processor to a message
func (m *UserAgent) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	// Always create a new copy if we intend to mutate message contents.
	newMsg := msg.Copy()
	uaFamily := fmt.Sprintf("%s_ua_family", m.field)
	uaMajor := fmt.Sprintf("%s_ua_major", m.field)
	uaMinor := fmt.Sprintf("%s_ua_minor", m.field)
	uaPatch := fmt.Sprintf("%s_ua_patch", m.field)

	osFamily := fmt.Sprintf("%s_os_family", m.field)
	osMajor := fmt.Sprintf("%s_os_major", m.field)
	osMinor := fmt.Sprintf("%s_os_minor", m.field)
	osPatch := fmt.Sprintf("%s_os_patch", m.field)

	osPatchMinor := fmt.Sprintf("%s_os_patch_minor", m.field)
	deviceFamily := fmt.Sprintf("%s_device_family", m.field)

	newMsg.Iter(func(i int, p types.Part) error {
		jObj, err := p.JSON()
		if err != nil {
			return err
		}
		obj, ok := jObj.(map[string]interface{})
		if !ok {
			return fmt.Errorf("not ok")
		}
		if fieldData, ok := obj[m.field]; ok {
			if uagent, ok := fieldData.(string); ok {
				client := m.parser.Parse(uagent)
				obj[uaFamily] = client.UserAgent.Family
				obj[uaMajor] = client.UserAgent.Major
				obj[uaMinor] = client.UserAgent.Minor
				obj[uaPatch] = client.UserAgent.Patch

				obj[osFamily] = client.Os.Family
				obj[osMajor] = client.Os.Major
				obj[osMinor] = client.Os.Minor
				obj[osPatch] = client.Os.Patch
				obj[osPatchMinor] = client.Os.PatchMinor
				obj[deviceFamily] = client.Device.Family
			}
		}

		return p.SetJSON(obj)
	})
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (m *UserAgent) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (m *UserAgent) WaitForClose(timeout time.Duration) error {
	return nil
}
