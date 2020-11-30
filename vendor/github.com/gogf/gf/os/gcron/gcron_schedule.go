// Copyright 2018 gf Author(https://github.com/gogf/gf). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/gogf/gf.

package gcron

import (
	"errors"
	"fmt"
	"github.com/gogf/gf/os/gtime"
	"strconv"
	"strings"
	"time"

	"github.com/gogf/gf/text/gregex"
)

// cronSchedule is the schedule for cron job.
type cronSchedule struct {
	create  int64            // Created timestamp.
	every   int64            // Running interval in seconds.
	pattern string           // The raw cron pattern string.
	second  map[int]struct{} // Job can run in these second numbers.
	minute  map[int]struct{} // Job can run in these minute numbers.
	hour    map[int]struct{} // Job can run in these hour numbers.
	day     map[int]struct{} // Job can run in these day numbers.
	week    map[int]struct{} // Job can run in these week numbers.
	month   map[int]struct{} // Job can run in these moth numbers.
}

const (
	// regular expression for cron pattern, which contains 6 parts of time units.
	gREGEX_FOR_CRON = `^([\-/\d\*\?,]+)\s+([\-/\d\*\?,]+)\s+([\-/\d\*\?,]+)\s+([\-/\d\*\?,]+)\s+([\-/\d\*\?,A-Za-z]+)\s+([\-/\d\*\?,A-Za-z]+)$`
)

var (
	// Predefined pattern map.
	predefinedPatternMap = map[string]string{
		"@yearly":   "0 0 0 1 1 *",
		"@annually": "0 0 0 1 1 *",
		"@monthly":  "0 0 0 1 * *",
		"@weekly":   "0 0 0 * * 0",
		"@daily":    "0 0 0 * * *",
		"@midnight": "0 0 0 * * *",
		"@hourly":   "0 0 * * * *",
	}
	// Short month name to its number.
	monthMap = map[string]int{
		"jan": 1,
		"feb": 2,
		"mar": 3,
		"apr": 4,
		"may": 5,
		"jun": 6,
		"jul": 7,
		"aug": 8,
		"sep": 9,
		"oct": 10,
		"nov": 11,
		"dec": 12,
	}
	// Short week name to its number.
	weekMap = map[string]int{
		"sun": 0,
		"mon": 1,
		"tue": 2,
		"wed": 3,
		"thu": 4,
		"fri": 5,
		"sat": 6,
	}
)

// newSchedule creates and returns a schedule object for given cron pattern.
func newSchedule(pattern string) (*cronSchedule, error) {
	// Check if the predefined patterns.
	if match, _ := gregex.MatchString(`(@\w+)\s*(\w*)\s*`, pattern); len(match) > 0 {
		key := strings.ToLower(match[1])
		if v, ok := predefinedPatternMap[key]; ok {
			pattern = v
		} else if strings.Compare(key, "@every") == 0 {
			if d, err := gtime.ParseDuration(match[2]); err != nil {
				return nil, err
			} else {
				return &cronSchedule{
					create:  time.Now().Unix(),
					every:   int64(d.Seconds()),
					pattern: pattern,
				}, nil
			}
		} else {
			return nil, errors.New(fmt.Sprintf(`invalid pattern: "%s"`, pattern))
		}
	}
	// Handle the common cron pattern, like:
	// 0 0 0 1 1 2
	if match, _ := gregex.MatchString(gREGEX_FOR_CRON, pattern); len(match) == 7 {
		schedule := &cronSchedule{
			create:  time.Now().Unix(),
			every:   0,
			pattern: pattern,
		}
		// Second.
		if m, err := parseItem(match[1], 0, 59, false); err != nil {
			return nil, err
		} else {
			schedule.second = m
		}
		// Minute.
		if m, err := parseItem(match[2], 0, 59, false); err != nil {
			return nil, err
		} else {
			schedule.minute = m
		}
		// Hour.
		if m, err := parseItem(match[3], 0, 23, false); err != nil {
			return nil, err
		} else {
			schedule.hour = m
		}
		// Day.
		if m, err := parseItem(match[4], 1, 31, true); err != nil {
			return nil, err
		} else {
			schedule.day = m
		}
		// Month.
		if m, err := parseItem(match[5], 1, 12, false); err != nil {
			return nil, err
		} else {
			schedule.month = m
		}
		// Week.
		if m, err := parseItem(match[6], 0, 6, true); err != nil {
			return nil, err
		} else {
			schedule.week = m
		}
		return schedule, nil
	} else {
		return nil, errors.New(fmt.Sprintf(`invalid pattern: "%s"`, pattern))
	}
}

// parseItem parses every item in the pattern and returns the result as map.
func parseItem(item string, min int, max int, allowQuestionMark bool) (map[int]struct{}, error) {
	m := make(map[int]struct{}, max-min+1)
	if item == "*" || (allowQuestionMark && item == "?") {
		for i := min; i <= max; i++ {
			m[i] = struct{}{}
		}
	} else {
		for _, item := range strings.Split(item, ",") {
			interval := 1
			intervalArray := strings.Split(item, "/")
			if len(intervalArray) == 2 {
				if i, err := strconv.Atoi(intervalArray[1]); err != nil {
					return nil, errors.New(fmt.Sprintf(`invalid pattern item: "%s"`, item))
				} else {
					interval = i
				}
			}
			var (
				rangeMin   = min
				rangeMax   = max
				fieldType  = byte(0)
				rangeArray = strings.Split(intervalArray[0], "-") // Like: 1-30, JAN-DEC
			)
			switch max {
			case 6:
				// It's checking week field.
				fieldType = 'w'
			case 12:
				// It's checking month field.
				fieldType = 'm'
			}
			// Eg: */5
			if rangeArray[0] != "*" {
				if i, err := parseItemValue(rangeArray[0], fieldType); err != nil {
					return nil, errors.New(fmt.Sprintf(`invalid pattern item: "%s"`, item))
				} else {
					rangeMin = i
					rangeMax = i
				}
			}
			if len(rangeArray) == 2 {
				if i, err := parseItemValue(rangeArray[1], fieldType); err != nil {
					return nil, errors.New(fmt.Sprintf(`invalid pattern item: "%s"`, item))
				} else {
					rangeMax = i
				}
			}
			for i := rangeMin; i <= rangeMax; i += interval {
				m[i] = struct{}{}
			}
		}
	}
	return m, nil
}

// parseItemValue parses the field value to a number according to its field type.
func parseItemValue(value string, fieldType byte) (int, error) {
	if gregex.IsMatchString(`^\d+$`, value) {
		// Pure number.
		if i, err := strconv.Atoi(value); err == nil {
			return i, nil
		}
	} else {
		// Check if contains letter,
		// it converts the value to number according to predefined map.
		switch fieldType {
		case 'm':
			if i, ok := monthMap[strings.ToLower(value)]; ok {
				return i, nil
			}
		case 'w':
			if i, ok := weekMap[strings.ToLower(value)]; ok {
				return i, nil
			}
		}
	}
	return 0, errors.New(fmt.Sprintf(`invalid pattern value: "%s"`, value))
}

// meet checks if the given time <t> meets the runnable point for the job.
func (s *cronSchedule) meet(t time.Time) bool {
	if s.every != 0 {
		// It checks using interval.
		diff := t.Unix() - s.create
		if diff > 0 {
			return diff%s.every == 0
		}
		return false
	} else {
		// It checks using normal cron pattern.
		if _, ok := s.second[t.Second()]; !ok {
			return false
		}
		if _, ok := s.minute[t.Minute()]; !ok {
			return false
		}
		if _, ok := s.hour[t.Hour()]; !ok {
			return false
		}
		if _, ok := s.day[t.Day()]; !ok {
			return false
		}
		if _, ok := s.month[int(t.Month())]; !ok {
			return false
		}
		if _, ok := s.week[int(t.Weekday())]; !ok {
			return false
		}
		return true
	}
}
