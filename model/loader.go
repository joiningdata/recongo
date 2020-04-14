package model

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"log"
	"os"
	"strings"
)

// Load a data source from a flat file.
// Format is tab-separated values in 4 columns with a 1-line header.
//
// Header:
//    0: Identifier Namespace URI
//    1: Name of the data source
//    2: Schema Namespace URI
//    3: JSON list of types [{id: "", name: "", description: "", url: "%s"}, ...]
// Properties:
//    0: Property ID
//    1: Name of the Property
//    2: comma-separated list of "property" + Entity Type IDs it applies to
//    3: JSON object of property settings {description: "", ...}
// Entities:
//    0: Entity ID
//    1: Entity Name
//    2: comma-separated list of Entity Type IDs
//    3: JSON object of properties {description: "", ...}
//
func Load(filename string) (Source, error) {
	if strings.Contains(filename, "sqlite") {
		return dbOpen("sqlite3", filename)
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	src := &MemorySource{
		entities:   make(map[string][]*Entity),
		types:      make(map[string]*Type),
		properties: make(map[string][]*Property),
	}
	defaultType := ""
	var s *bufio.Scanner
	if strings.HasSuffix(filename, ".gz") {
		fz, err := gzip.NewReader(f)
		if err == nil {
			s = bufio.NewScanner(fz)
		} else {
			log.Println(err)
			s = bufio.NewScanner(f)
		}
	} else {
		s = bufio.NewScanner(f)
	}
	for s.Scan() {
		row := strings.SplitN(s.Text(), "\t", 4)
		if src.name == "" {
			src.name = row[1]
			src.identifierNamespace = row[0]
			src.schemaNamespace = row[2]

			if row[3] == "" {
				defaultType = "item"
			} else {
				tx := []*Type{}
				err = json.Unmarshal([]byte(row[3]), &tx)
				if err != nil {
					return nil, err
				}
				for _, x := range tx {
					src.types[x.ID] = x
				}
				if len(tx) >= 1 {
					defaultType = tx[0].ID
					src.viewURL = tx[0].ViewURL
				}
			}
			continue
		}

		if len(row) != 4 || row[3] == "" {
			panic("invalid input")
		}

		props := make(map[string]interface{})
		typeIDs := strings.Split(row[2], ",")
		if typeIDs[0] == "" {
			typeIDs[0] = defaultType
		}

		if row[3] != "{}" {
			err = json.Unmarshal([]byte(row[3]), &props)
			if err != nil {
				return nil, err
			}
		}

		if len(typeIDs) > 1 {
			isProp := false
			for _, tid := range typeIDs {
				if tid == "property" {
					isProp = true
				}
			}
			if isProp {
				p := &Property{
					ID:   row[0],
					Name: row[1],
				}
				if d, ok := props["description"]; ok {
					p.Description = d.(string)
				}
				for _, etype := range typeIDs {
					if etype == "property" {
						continue
					}
					src.properties[etype] = append(src.properties[etype], p)
				}
				continue
			}
		}

		e := &Entity{
			ID:   EntityID(row[0]),
			Name: row[1],
		}
		for i, tid := range typeIDs {
			if i == 0 {
				e.ID = EntityID(tid + ":" + row[0])
			}
			e.Types = append(e.Types, src.types[tid])
		}

		if len(props) > 0 {
			if d, ok := props["description"]; ok {
				e.Description = d.(string)
			}
			e.Properties = props
		}
		src.entities[row[0]] = append(src.entities[row[0]], e)
	}

	log.Printf("loaded %d entities from '%s'. ", len(src.entities), filename)
	return src, nil
}
