// generic database/sql support interfaces.
//   add the drivers / implementations separately
// primarily, I dont want to have to recompile sqlite
// support all the time if testing stuff in memory...
//

package model

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	// note: must build with "fts5" build tag!
	// e.g. go build --tags "fts5" .
	_ "github.com/mattn/go-sqlite3"
)

// DatabaseSource represents a data source in a database.
type DatabaseSource struct {
	db         *sql.DB
	driverName string

	name                string
	identifierNamespace string
	schemaNamespace     string
	viewURL             string

	// maps from Entity ID to Entity for all cached entities.
	//entities *cache.Cache

	// cache maps from Entity Type ID to Type for all supported types.
	types map[string]*Type

	// cache maps from Entity Type ID to a Property list for all supported entity types.
	properties map[string][]*Property
}

// ensure it implements the interface
var _ Source = &DatabaseSource{}

// Name of the data Source.
func (s *DatabaseSource) Name() string {
	return s.name
}

// IdentifierNS is a universal namespace for Entity identifiers.
func (s *DatabaseSource) IdentifierNS() string {
	return s.identifierNamespace
}

// SchemaNS is a universal namespace for concept Type identifiers.
func (s *DatabaseSource) SchemaNS() string {
	return s.schemaNamespace
}

// Types returns all supported Entity types.
func (s *DatabaseSource) Types() []*Type {
	var res []*Type
	for _, x := range s.types {
		res = append(res, x)
	}
	return res
}

// Properties returns all supported Properties for Entities with the Type ID given.
func (s *DatabaseSource) Properties(typeID string) []*Property {
	var res []*Property
	for _, x := range s.properties[typeID] {
		res = append(res, x)
	}
	return res
}

// GetEntity returns the Entity matching the provided ID.
func (s *DatabaseSource) GetEntity(entityID EntityID) (*Entity, bool) {
	// FIXME: use Type in the query too
	ents, _ := s.getExactIDMatches(entityID.ID())
	for _, e := range ents {
		for _, t := range e.Types {
			if t.ID == entityID.Type() {
				e.Properties, _ = s.getEntityProps(entityID)
				return e, true
			}
		}
	}
	return nil, false
}

func (s *DatabaseSource) getEntityProps(eid EntityID) (map[string]interface{}, error) {
	rows, err := s.doQuery("entity_property_values", eid.Type(), eid.ID())
	if err != nil {
		return nil, err
	}

	res := make(map[string]interface{})
	for rows.Next() {
		var propName, propValue string
		err = rows.Scan(&propName, &propValue)
		if err != nil {
			rows.Close()
			return nil, err
		}
		res[propName] = propValue
	}
	return res, rows.Close()
}

func (s *DatabaseSource) getExactIDMatches(id string) ([]*Entity, bool) {
	eTypes := ""
	rows, err := s.doQuery("entity_by_id", id)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, false
		}
		log.Println(err)
		return nil, false
	}

	var res []*Entity
	for rows.Next() {
		e := &Entity{}
		err = rows.Scan(&e.ID, &e.Name, &e.Description, &eTypes)
		for i, tid := range strings.Split(eTypes, ",") {
			if i == 0 {
				e.ID = EntityID(tid + ":" + string(e.ID))
			}
			e.Types = append(e.Types, s.types[tid])
		}
		res = append(res, e)
	}
	rows.Close()

	return res, len(res) > 0
}

// Query entitities for a match.
func (s *DatabaseSource) Query(q *QueryRequest) (*QueryResponse, error) {
	if q.Limit == 0 {
		q.Limit = 25
	}
	res := &QueryResponse{
		ID:      q.ID,
		Results: make([]*Candidate, 0, q.Limit),
	}
	log.Println(q)

	// fast-track exact ID matches
	if ents, ok := s.getExactIDMatches(q.Text); ok {
		log.Println("one-shot:", ents)
		for _, e := range ents {
			res.Results = append(res.Results, &Candidate{
				ID:    e.ID,
				Name:  e.Name,
				Types: e.Types,
				Score: 100.0,
				Match: true,
			})
		}
		return res, nil
	}

	var rows *sql.Rows
	var err error
	if len(q.Properties) > 0 {
		rows, err = s.doQuery("entity_search_by_props", q.Text, q.Properties)
	} else {
		rows, err = s.doQuery("entity_search", q.Text)
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return res, nil
		}

		return nil, err
	}
	scoreScale := 0.0
	for rows.Next() {
		c := &Candidate{}
		cTypes := ""
		err = rows.Scan(&c.ID, &c.Name, &cTypes, &c.Score)
		if err != nil {
			rows.Close()
			return nil, err
		}
		hitType := (q.Type == "")
		for i, tid := range strings.Split(cTypes, ",") {
			if i == 0 {
				c.ID = EntityID(tid + ":" + string(c.ID))
			}
			c.Types = append(c.Types, s.types[tid])
			if tid == q.Type {
				hitType = true
			}
		}
		if !hitType {
			continue
		}
		if scoreScale == 0.0 {
			s1 := float64(len(q.Text)) / float64(len(c.ID))
			s2 := float64(len(q.Text)) / float64(len(c.Name))
			if s2 > s1 {
				s1 = s2
			}
			scoreScale = (s1 * 100.0) / c.Score
		}
		c.Score = c.Score * scoreScale

		c.Match = c.Score > 80.0
		res.Results = append(res.Results, c)
		if len(res.Results) == q.Limit {
			break
		}
	}
	rows.Close()

	return res, nil
}

// QueryPrefix searches entitities for a prefix match.
func (s *DatabaseSource) QueryPrefix(text string, limit int) []*Entity {
	log.Println("prefix: ", text, limit)
	// fast-track exact ID matches
	if ents, ok := s.getExactIDMatches(text); ok {
		log.Println("prefix one-shot:", ents)
		return ents
	}

	var result []*Entity

	rows, err := s.doQuery("entity_by_prefix", text)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Println(err)
		}
		return result
	}
	for rows.Next() {
		e := &Entity{}
		eTypes := ""
		err = rows.Scan(&e.ID, &e.Name, &e.Description, &eTypes)
		if err != nil {
			rows.Close()
			log.Println(err)
			return nil
		}

		for i, tid := range strings.Split(eTypes, ",") {
			if i == 0 {
				e.ID = EntityID(tid + ":" + string(e.ID))
			}
			e.Types = append(e.Types, s.types[tid])
		}
		result = append(result, e)
		if len(result) == limit {
			break
		}
	}
	rows.Close()

	return result
}

// ViewURL returns the template for a View URL.
func (s *DatabaseSource) ViewURL() string {
	return s.viewURL
}

////////////////

var _queries = map[string]map[string]string{
	"all": map[string]string{
		// list all metadata (key, value) pairs
		"metadata": "SELECT meta_key, meta_value FROM recongo_metadata",

		// list all entity types (id, name, description, url template)
		// only first two are required, rest must be non-null but blank is ok
		"types": "SELECT type_id, type_name, COALESCE(type_description,''), type_url FROM recongo_types",

		// list all property types (id, name, description)
		// only first two are required, description must be non-null but blank is ok
		"properties": "SELECT prop_id, prop_name, COALESCE(prop_description,'') FROM recongo_properties",

		// list all pairs of entity id-property id combinations
		"properties_by_type": "SELECT prop_id, type_id FROM recongo_props2types",
	},
	"sqlite3": map[string]string{
		// find an entity with a specific id
		"entity_by_id": `SELECT ent_id, ent_name, COALESCE(ent_description,''), ent_types FROM recongo_entities
			WHERE ent_id=?1`,

		// find entities with a specific prefix
		"entity_by_prefix": `SELECT ent_id, ent_name, COALESCE(ent_description,''), ent_types FROM recongo_entities
			WHERE (ent_id LIKE ?1||'%' OR ent_name LIKE ?1||'%')
			ORDER BY ent_name, ent_id`,

		// full-text search entities for a text query
		"entity_search": `SELECT ent_id, ent_name, ent_types, bm25(recongo_entities_fts) as score
			FROM recongo_entities_fts WHERE recongo_entities_fts MATCH ?1||'*'
			ORDER BY score`,
		// entity_search_by_props adds additional joins for property filters

		// find all properties and values for a entity id
		"entity_property_values": `SELECT prop_id, prop_value FROM recongo_entity_properties
			WHERE ent_types=?1 AND ent_id=?2 ORDER BY prop_id, prop_value`,
	},
}

func (s *DatabaseSource) doQuery(qname string, args ...interface{}) (*sql.Rows, error) {
	query, ok := _queries[s.driverName][qname]
	if !ok {
		query = _queries["all"][qname]
	}
	// special case, do a entity_search but also filter by property values
	if qname == "entity_search_by_props" {
		if s.driverName == "sqlite3" {
			q1 := `SELECT a.ent_id, a.ent_name, a.ent_types, bm25(recongo_entities_fts) as score
			FROM recongo_entities_fts a `
			q2 := `WHERE recongo_entities_fts MATCH ? `
			q3 := `ORDER BY score`

			props, ok := args[1].([]*QueryProperty)
			if !ok {
				log.Printf("%T", args[1])
				return nil, fmt.Errorf("invalid property set")
			}
			newargs := make([]interface{}, 1, len(props)+1)
			newargs[0] = args[0]
			for i, pd := range props {
				ta := string([]rune{'b' + rune(i)})
				q1 += ", recongo_entity_properties " + ta + " "
				q2 += "  AND a.ent_id=" + ta + ".ent_id AND " + ta + ".prop_id=? AND " + ta + ".prop_value=? "
				switch px := pd.Value.(type) {
				case string:
					newargs = append(newargs, pd.ID, px)
				case map[string]interface{}:
					eid := EntityID(px["id"].(string))
					newargs = append(newargs, pd.ID, eid.ID())
				default:
					newargs = append(newargs, pd.ID, pd.Value)
				}
			}
			query = q1 + q2 + q3
			args = newargs
		}
	}
	//log.Println(query, args)
	return s.db.Query(query, args...)
}

func dbOpen(driverName, connstring string) (Source, error) {

	db, err := sql.Open(driverName, connstring)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	d := &DatabaseSource{
		db:         db,
		driverName: driverName,
		types:      make(map[string]*Type),
		properties: make(map[string][]*Property),
	}

	/////////
	// load metadata first
	rows, err := d.doQuery("metadata")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		key, val := "", ""
		err = rows.Scan(&key, &val)
		if err != nil {
			rows.Close()
			return nil, err
		}
		switch key {
		case "name":
			d.name = val
		case "identifierNamespace":
			d.identifierNamespace = val
		case "schemaNamespace":
			d.schemaNamespace = val
		case "view_url":
			d.viewURL = val
		}
	}
	rows.Close()

	////////////
	// load all the entity types
	rows, err = d.doQuery("types")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		t := &Type{}
		err = rows.Scan(&t.ID, &t.Name, &t.Description, &t.ViewURL)
		if err != nil {
			rows.Close()
			return nil, err
		}
		d.types[t.ID] = t
	}
	rows.Close()

	// load a mapping from propID to all entity types
	pairMap := make(map[string][]string)
	rows, err = d.doQuery("properties_by_type")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		propID, typeID := "", ""
		err = rows.Scan(&propID, &typeID)
		if err != nil {
			rows.Close()
			return nil, err
		}
		pairMap[propID] = append(pairMap[propID], typeID)
	}
	rows.Close()

	rows, err = d.doQuery("properties")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		p := &Property{}
		err = rows.Scan(&p.ID, &p.Name, &p.Description)
		if err != nil {
			rows.Close()
			return nil, err
		}
		// defined, but not used?
		if ents, ok := pairMap[p.ID]; ok {
			for _, eid := range ents {
				d.properties[eid] = append(d.properties[eid], p)
			}
		}
	}
	rows.Close()

	return d, nil
}
