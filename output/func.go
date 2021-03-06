package output

import (
	"strconv"
	"strings"
	"time"
)

func stringToInt32(s interface{}, args ...interface{}) interface{} {
	if s, ok := s.(string); ok {
		out, err := strconv.Atoi(s)
		if err == nil {
			return out
		}
	}

	return nil
}

func unixToDateOrNow(s interface{}, args ...interface{}) interface{} {
	if i, ok := s.(int64); ok {
		return time.Unix(i, 0)
	}

	if i, ok := s.(int); ok {
		return time.Unix(int64(i), 0)
	}

	return time.Now()
}

func floatToInt32(s interface{}, args ...interface{}) interface{} {
	if i, ok := s.(float64); ok {
		return int32(i)
	}
	return nil
}

func floatToUInt32(s interface{}, args ...interface{}) interface{} {
	if i, ok := s.(float64); ok {
		return uint32(i)
	}
	return nil
}

func floatToUInt64(s interface{}, args ...interface{}) interface{} {
	if i, ok := s.(float64); ok {
		return uint64(i)
	}
	return nil
}

func floatToUInt8(s interface{}, args ...interface{}) interface{} {
	if i, ok := s.(float64); ok {
		return uint8(i)
	}
	return nil
}

func bypass(s interface{}, args ...interface{}) interface{} {
	return s
}

func stringToDateOrNow(s interface{}, args ...interface{}) interface{} {
	if s, ok := s.(string); ok {
		var format = "2006-01-02T15:04:05.000Z"
		if len(args) > 0 {
			format, _ = args[0].(string)
		}
		t, err := time.Parse(format, s)
		if err == nil {
			return t
		}
	}
	return time.Now()
}

var funcvals = map[string]func(s interface{}, args ...interface{}) interface{}{
	"stringToInt32":     stringToInt32,
	"unixToDateOrNow":   unixToDateOrNow,
	"floatToInt32":      floatToInt32,
	"floatToUInt32":     floatToUInt32,
	"stringToDateOrNow": stringToDateOrNow,
	"floatToUInt64":     floatToUInt64,
	"floatToUInt8":      floatToUInt8,
	"bypass":            bypass,
}

type InterpolatedAll struct {
	f        func(s interface{}, args ...interface{}) interface{}
	jsonPath string
	args     []interface{}
}

func NewInterpolatedAll(s string) *InterpolatedAll {
	splited := strings.Split(s, "$")
	var jsonPath = s
	var fval = funcvals["bypass"]
	var args = []interface{}{}
	if len(splited) == 2 {
		jsonPath = splited[0]
		if funcval, ok := funcvals[splited[1]]; ok {
			fval = funcval
		}
	}
	if len(splited) > 2 {
		jsonPath = splited[0]
		if funcval, ok := funcvals[splited[1]]; ok {
			fval = funcval
			for _, s := range splited[2:] {
				args = append(args, s)
			}
		}
	}
	return &InterpolatedAll{f: fval, jsonPath: jsonPath, args: args}
}

func (ia *InterpolatedAll) conv(obj map[string]interface{}) interface{} {
	if x, ok := obj[ia.jsonPath]; ok {
		return ia.f(x, ia.args...)
	}
	return nil
}
