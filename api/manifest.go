package api

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/joiningdata/recongo/model"
)

// URLTemplate is a URL template pattern.
// Internally, it contains a '%s' placeholder although the spec
// uses '{{id}}' or '${id}' for the placeholder officially.
type URLTemplate string

// Apply returns the URL after interpolating an identifier into the template.
func (u URLTemplate) Apply(ident interface{}) string {
	switch s := ident.(type) {
	case string:
		return fmt.Sprintf(string(u), s)
	default:
		ss := fmt.Sprint(ident)
		return fmt.Sprintf(string(u), ss)
	}

}

// UnmarshalJSON implements the necessary json interface, so it
// can transparently update the template for string interpolation.
func (u *URLTemplate) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	// translate either spec version
	if strings.Contains(s, "{{id}}") {
		s = strings.Replace(s, "{{id}}", "%s", -1)
	} else if strings.Contains(s, "${id}") {
		s = strings.Replace(s, "${id}", "%s", -1)
	} else {
		return fmt.Errorf("recongo.api: url pattern has no placeholder '{{id}}': %s", s)
	}
	*u = URLTemplate(s)
	return nil
}

// MarshalJSON implements the necessary json interface, so it can
// transparently convert the internal representation to expected format.
func (u URLTemplate) MarshalJSON() ([]byte, error) {
	return json.Marshal(strings.Replace(string(u), "%s", "{{id}}", -1))
}

//////

// Extend Settings for the data extension protocol, to fetch property values
type Extend struct {

	// Definition of the settings configurable by the user when fetching a property
	PropertySettings []interface{} `json:"property_settings,omitempty"`

	// Location of the endpoint to propose properties to fetch for a given type
	ProposeProperties *ServiceDefinition `json:"propose_properties,omitempty"`
}

// Preview Settings for the preview protocol, for HTML previews of entities
type Preview struct {
	// Width of the container that will show the HTML preview.
	Width int `json:"width"`

	// Width of the container that will show the HTML preview.
	Height int `json:"height"`

	// URL teamplte to transforms the entity ID into a preview URL for it.
	URL URLTemplate `json:"url"`
}

// Manifest describes the features supported by this Reconciliation endpoint.
type Manifest struct {

	// DefaultTypes contains a list of entity types supported for reconciliation.
	DefaultTypes []*model.Type `json:"defaultTypes,omitempty"`

	// Extend protocol defitions for the data extension protocol, to fetch property values.
	Extend *Extend `json:"extend,omitempty"`

	// IdentifierSpace is a URI describing the entity identifiers used in this service
	IdentifierSpace string `json:"identifierSpace"`

	// Name is a human-readable name for the service or data source.
	Name string `json:"name"`

	// Preview protocol definitions for retrieving HTML previews of entities.
	Preview *Preview `json:"preview,omitempty"`

	// SchemaSpace is a URI describing the schema used in this service.
	SchemaSpace string `json:"schemaSpace"`

	// Suggest protocol defintions for auto-completion of entities, properties and types.
	Suggest *Suggest `json:"suggest,omitempty"`

	// Versions contains a list of API versions supported by this service.
	Versions []string `json:"versions"`

	// View contains a template to turn an entity identifier into a URI.
	View *View `json:"view,omitempty"`
}

// ServiceDefinition describes a service endpoint by its base URL and path.
type ServiceDefinition struct {
	// ServicePath describes the path appended to ServiceURL to access the service.
	ServicePath string `json:"service_path,omitempty"`

	// ServiceURL describes the root URL to access the service.
	ServiceURL string `json:"service_url,omitempty"`
}

// Suggest describes for the suggest protocol, to auto-complete entities, properties and types
type Suggest struct {
	// Entity describes the entity suggestion endpoint.
	Entity *ServiceDefinition `json:"entity,omitempty"`

	// Property describes the property suggestion endpoint.
	Property *ServiceDefinition `json:"property,omitempty"`

	// Type describes the type suggestion endpoint.
	Type *ServiceDefinition `json:"type,omitempty"`
}

// View defines a template to turn an entity identifier into a URI.
type View struct {
	// URL template to transform an entity identifier into the corresponding URI
	URL URLTemplate `json:"url"`
}
