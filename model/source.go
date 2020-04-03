package model

// Source represents a data source.
type Source interface {
	// Name of the data Source.
	Name() string

	// IdentifierNS is a universal namespace for Entity identifiers.
	IdentifierNS() string

	// SchemaNS is a universal namespace for concept Type identifiers.
	SchemaNS() string

	// Types returns all supported Entity types.
	Types() []*Type

	// Properties returns all supported Properties for Entities with the Type ID given.
	Properties(typeID string) []*Property

	// GetEntity returns the Entity matching the provided ID.
	GetEntity(entityID string) (*Entity, bool)

	// Query entitities for a match.
	Query(q *QueryRequest) (*QueryResponse, error)

	// QueryPrefix searches entitities for a prefix match.
	QueryPrefix(text string, limit int) []*Entity
}

// QueryRequest describes a Reconciliation Query request.
type QueryRequest struct {
	// ID to refer to the query.
	ID string `json:"id"`

	// Text is the search text to query for.
	Text string `json:"query"`

	// Types lists the Type IDs to query over.
	Type string `json:"type"`

	// Limit the results to the first N results.
	Limit int `json:"limit"`

	// Properties lists the various property values to query over.
	Properties []*QueryProperty `json:"properties"`

	// Strictness should be set to "any", "all", or "should"
	Strictness string `json:"type_strict,omitempty"`
}

// QueryProperty depicts a query against a property value.
type QueryProperty struct {
	// ID is the property ID.
	ID string `json:"pid"`
	// Values is the list of values to search for in the Property.
	Values []string `json:"v"`
}

// QueryResponse describes a Reconciliation Query reponse.
type QueryResponse struct {
	// ID of the QueryRequest this is responding to.
	ID string `json:"id"`

	// Results lists the Candidates matching the query.
	Results []*Candidate `json:"result"`
}

// Candidate describes a Reconciliation Query candidate entity.
type Candidate struct {
	// ID of the candidate entity.
	ID string `json:"id"`

	// Name of the candidate entity.
	Name string `json:"name"`

	// Types of the candidate entity.
	Types []*Type `json:"type"`

	// Score indicates how good the match is. Higher is better.
	Score float64 `json:"score"`

	// Match indicates if this is a "good" match or not.
	Match bool `json:"match"`
}
