package model

import (
	"fmt"
	"strconv"
	"strings"
)

// Entity is a record in the data source.
type Entity struct {
	// ID is a unique identifier for the entity.
	ID string `json:"id"`

	// Name is a human-readable description of the entity.
	Name string `json:"name"`

	// Description is a human-readable description of the entity.
	Description string `json:"description,omitempty"`

	// Types is a (possibly empty) list of entity types for this entity.
	Types []*Type `json:"type"`
}

// Type represents a category of entities.
type Type struct {
	// ID is a unique identifier for the type.
	ID string `json:"id"`

	// Name is a human-readable description of the type.
	Name string `json:"name"`

	// Description is a human-readable description of the type.
	Description string `json:"description,omitempty"`
}

// Property represents a type of attribute that entities can have
// within the data source.
type Property struct {
	// ID is a unique identifier for the property.
	ID string `json:"id"`

	// Name is a human-readable name of the property.
	Name string `json:"name"`

	// Description is a human-readable description of the property.
	Description string `json:"description,omitempty"`

	// ValueType expected for Property values.
	ValueType string `json:"-"`
}

// PropertyValue is a specific value associated to an entity property.
// In this implementation, the value MUST be one of the following Go types:
//   string, bool, int64, float64, or Entity
type PropertyValue struct {
	v interface{}
}

// String coerces the value into a string no matter what.
// For Entity values, returns the ID.
func (p PropertyValue) String() string {
	switch x := p.v.(type) {
	case string:
		return x
	case Entity:
		return x.ID
	default:
		// bool, int64, float64
		return fmt.Sprint(p.v)
	}
}

// Bool attempts to coerce the value into a bool.
// For bool, int64, and float64 returns true if non-zero.
// String values of "YES", "TRUE", "T", "ON", or "1" return true.
// All other values return false.
func (p PropertyValue) Bool() bool {
	switch s := p.v.(type) {
	case bool:
		return s
	case int64:
		return s != 0
	case float64:
		return s != 0.0
	case string:
		s = strings.ToUpper(s)
		if s == "YES" || s == "TRUE" || s == "T" || s == "ON" || s == "1" {
			return true
		}
	}
	return false
}

// Int64 attempts to coerce the value into a 64-bit integer.
// bool values are converted to 0 or 1
// float64 values are truncated to integers.
// String and Entity ID values attempt string conversion to
//    int64 using base 10 representation.
// All other values return 0.
func (p PropertyValue) Int64() int64 {
	switch s := p.v.(type) {
	case bool:
		if s {
			return 1
		}
	case int64:
		return s
	case float64:
		return int64(s)
	case string:
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return n
		}
	case Entity:
		n, err := strconv.ParseInt(s.ID, 10, 64)
		if err == nil {
			return n
		}
	}
	return 0
}

// Float64 attempts to coerce the value into a 64-bit floating point value.
// bool values are converted to 0 or 1
// int64 values are casted to float64.
// String and Entity ID values attempt string conversion to
//    float64 using base 10 representation.
// All other values return 0.
func (p PropertyValue) Float64() float64 {
	switch s := p.v.(type) {
	case bool:
		if s {
			return 1.0
		}
	case int64:
		return float64(s)
	case float64:
		return s
	case string:
		n, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return n
		}
	case Entity:
		n, err := strconv.ParseFloat(s.ID, 64)
		if err == nil {
			return n
		}
	}
	return 0.0
}

// Entity copies the entity data into the provided structure and returns true,
// If the value is not an entity simply returns false.
func (p PropertyValue) Entity(e *Entity) bool {
	if x, ok := p.v.(Entity); ok {
		e.ID = x.ID
		e.Name = x.Name
		if len(x.Types) == 0 {
			e.Types = e.Types[:0]
			return true
		}

		if cap(e.Types) >= len(x.Types) {
			// copy without an allocation
			e.Types = e.Types[:len(x.Types)]
			copy(e.Types, x.Types)
		} else {
			e.Types = make([]*Type, len(x.Types))
			copy(e.Types, x.Types)
		}
		return true
	}
	return false
}
