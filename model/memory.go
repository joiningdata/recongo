package model

import (
	"log"
	"sort"
	"strings"
)

// MemorySource represents a data source entirely in memory.
type MemorySource struct {
	name                string
	identifierNamespace string
	schemaNamespace     string
	viewURL             string

	// maps from Entity ID to Entity for all known entities.
	entities map[string]*Entity

	// maps from Entity Type ID to Entity ID list all known entities.
	entitiesByType map[string][]string

	// maps from Entity Type ID to Type for all supported types.
	types map[string]*Type

	// maps from Entity Type ID to a Property list for all supported entity types.
	properties map[string][]*Property
}

// ensure it implements the interface
var _ Source = &MemorySource{}

// Name of the data Source.
func (s *MemorySource) Name() string {
	return s.name
}

// IdentifierNS is a universal namespace for Entity identifiers.
func (s *MemorySource) IdentifierNS() string {
	return s.identifierNamespace
}

// SchemaNS is a universal namespace for concept Type identifiers.
func (s *MemorySource) SchemaNS() string {
	return s.schemaNamespace
}

// Types returns all supported Entity types.
func (s *MemorySource) Types() []*Type {
	var res []*Type
	for _, x := range s.types {
		res = append(res, x)
	}
	return res
}

// Properties returns all supported Properties for Entities with the Type ID given.
func (s *MemorySource) Properties(typeID string) []*Property {
	var res []*Property
	for _, x := range s.properties[typeID] {
		res = append(res, x)
	}
	return res
}

// GetEntity returns the Entity matching the provided ID.
func (s *MemorySource) GetEntity(entityID string) (*Entity, bool) {
	e, ok := s.entities[entityID]
	return e, ok
}

// Query entitities for a match.
func (s *MemorySource) Query(q *QueryRequest) (*QueryResponse, error) {
	res := &QueryResponse{
		ID: q.ID,
	}
	log.Println(q)

	// fast-track exact ID matches
	if e, ok := s.entities[q.Text]; ok {
		log.Println("one-shot:", e)

		res.Results = append(res.Results, &Candidate{
			ID:    e.ID,
			Name:  e.Name,
			Types: e.Types,
			Score: 100.0,
			Match: true,
		})
		return res, nil
	}

	low := strings.ToLower(q.Text)

	for _, e := range s.entities {
		score := 0.0
		if strings.ToLower(e.ID) == low {
			score = 95.0
		} else if strings.Contains(strings.ToLower(e.Name), low) {
			// essentially recall since there's no mismatch to low
			score = float64(len(low)*100) / float64(len(e.Name))
		}
		if e.ID == "4336" {
			log.Println(e.ID, e.Name, low, score)
		}
		if q.Type != "" {
			for _, et := range e.Types {
				if et.ID == q.Type {
					score += 10.0
					break
				}
			}
		}

		// TODO: score properties also

		if score > 0.0 {
			res.Results = append(res.Results, &Candidate{
				ID:    e.ID,
				Name:  e.Name,
				Types: e.Types,
				Score: score,
				Match: score > 80.0,
			})
		}
	}

	sort.Slice(res.Results, func(i, j int) bool {
		return res.Results[i].Score > res.Results[j].Score
	})
	if q.Limit == 0 {
		q.Limit = 25
	}
	if len(res.Results) > q.Limit {
		res.Results = res.Results[:q.Limit]
	}
	return res, nil
}

// QueryPrefix searches entitities for a prefix match.
func (s *MemorySource) QueryPrefix(text string, limit int) []*Entity {
	log.Println("prefix: ", text, limit)
	// fast-track exact ID matches
	if e, ok := s.entities[text]; ok {
		log.Println("prefix one-shot:", e)
		return []*Entity{e}
	}

	var result []*Entity
	low := strings.ToLower(text)
	for _, e := range s.entities {
		if strings.HasPrefix(strings.ToLower(e.Name), low) {
			result = append(result, e)

			if len(result) >= limit {
				break
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name > result[j].Name
	})
	return result
}

// ViewURL returns the template for a View URL.
func (s *MemorySource) ViewURL() string {
	return s.viewURL
}
