package model

import (
	"bufio"
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
//    3: JSON list of types [{id: "", name: "", description: ""}, ...]
// Entities:
//    0: Entity ID
//    1: Entity Name
//    2: comma-separated list of entity Type IDs
//    3: JSON object of properties [{description: "", ...}, ...]
//
func Load(filename string) (Source, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	src := &MemorySource{
		entities:       make(map[string]*Entity),
		entitiesByType: make(map[string][]string),
		types:          make(map[string]*Type),
		properties:     make(map[string][]*Property),
	}
	defaultType := ""
	s := bufio.NewScanner(f)
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
				if len(tx) == 1 {
					defaultType = tx[0].ID
				}
			}
			continue
		}

		e := &Entity{
			ID:   row[0],
			Name: row[1],
		}
		if row[2] == "" && defaultType != "" {
			e.Types = append(e.Types, src.types[defaultType])
		} else if !strings.Contains(row[2], ",") {
			e.Types = append(e.Types, src.types[row[2]])
		} else {
			for _, tid := range strings.Split(row[2], ",") {
				e.Types = append(e.Types, src.types[tid])
			}
		}

		if row[3] != "" && row[3] != "{}" {
			props := make(map[string]interface{})
			err = json.Unmarshal([]byte(row[3]), &props)
			if err != nil {
				return nil, err
			}
			if d, ok := props["description"]; ok {
				e.Description = d.(string)
			}
			// TODO: store the other properties too
		}
		src.entities[e.ID] = e
		for _, t := range e.Types {
			src.entitiesByType[t.ID] = append(src.entitiesByType[t.ID], e.ID)
		}
	}

	log.Printf("loaded %d entities from '%s'. ", len(src.entities), filename)
	return src, nil
}
