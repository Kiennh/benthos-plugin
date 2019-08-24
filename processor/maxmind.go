package processor

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/processor"
	"github.com/Jeffail/benthos/lib/types"

	"github.com/oschwald/geoip2-golang"
)

//------------------------------------------------------------------------------

func init() {
	processor.RegisterPlugin(
		"geo",
		func() interface{} {
			return NewGeoConfig()
		}, // No configuration needed
		func(
			iconf interface{},
			mgr types.Manager,
			logger log.Modular,
			stats metrics.Type,
		) (types.Processor, error) {
			conf, ok := iconf.(*GeoConfig)
			if !ok {
				return nil, errors.New("failed to cast config")
			}
			return Newgeo(*conf, logger, stats)
		},
	)
	processor.DocumentPlugin(
		"geo",
		`geos the maxmind processor.`,
		nil,
	)
}

//------------------------------------------------------------------------------

type GeoConfig struct {
	File  string `json:"file" yaml:"file"`
	Field string `json:"field" yaml:"field"`
}

// NewGibberishConfig creates a config with default values.
func NewGeoConfig() *GeoConfig {
	return &GeoConfig{}
}

// geo is a processor that geos all messages.
type geo struct {
	file     string
	field    string
	geoip2DB *geoip2.Reader

	log   log.Modular
	stats metrics.Type
}

// Newgeo returns a geo processor.
func Newgeo(conf GeoConfig,
	log log.Modular, stats metrics.Type,
) (types.Processor, error) {
	db, err := geoip2.Open(conf.File)
	if err != nil {
		return nil, err
	}
	m := &geo{
		file:     conf.File,
		log:      log,
		field:    conf.Field,
		geoip2DB: db,
		stats:    stats,
	}
	return m, nil
}

// ProcessMessage applies the processor to a message
func (m *geo) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	// Always create a new copy if we intend to mutate message contents.
	newMsg := msg.Copy()
	cityName := fmt.Sprintf("%s_city", m.field)
	countryName := fmt.Sprintf("%s_country", m.field)
	latitudeName := fmt.Sprintf("%s_latitude", m.field)
	longitudeName := fmt.Sprintf("%s_longtitude", m.field)

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
			if ipStr, ok := fieldData.(string); ok {
				ip := net.ParseIP(ipStr)
				record, err := m.geoip2DB.City(ip)
				if err == nil {
					obj[cityName] = record.City.Names["en"]
					obj[countryName] = record.Country.IsoCode
					obj[latitudeName] = record.Location.Latitude
					obj[longitudeName] = record.Location.Longitude
				}

			}
		}

		return p.SetJSON(obj)
	})
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (m *geo) CloseAsync() {
	m.geoip2DB.Close()
}

// WaitForClose blocks until the processor has closed down.
func (m *geo) WaitForClose(timeout time.Duration) error {
	return nil
}
