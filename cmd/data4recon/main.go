package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"

	// sqlite database drivers
	_ "github.com/mattn/go-sqlite3"
)

type inputConfig struct {
	Name                string            `json:"name"`
	IdentifierNamespace string            `json:"identifier_namespace"`
	SchemaNamespace     string            `json:"schema_namespace"`
	Properties          map[string]string `json:"property_names"`
	ViewURL             string            `json:"view_url"`

	Files []FileConfig `json:"files"`
}

// FileConfig describes the configuration for a data file.
type FileConfig struct {
	// ID of the Type of data in this file.
	ID string `json:"id"`

	// Name of the Type of data in this file.
	Name string `json:"name"`

	// Description of the data type for this file.
	Description string `json:"description"`

	// Filename that contains the data (CSV or tab-delimited)
	Filename string `json:"filename"`

	// Properties maps each 0-based column of the file to a Property ID or blank.
	Properties map[int]string `json:"column2property"`
}

func showHelp() {
	cfgset := inputConfig{}

	fmt.Fprintln(os.Stderr, "First argument should be a json file with a list of input file configurations.")
	cfgset.Properties = map[string]string{
		"another_property": "another property defined on the item",
	}
	cfgset.Files = make([]FileConfig, 2)
	cfgset.Files[0].Properties = map[int]string{1: "id", 2: "name", 0: "tax_id", 9: "description", 5: "another_property"}

	raw, _ := json.MarshalIndent(cfgset, "", "  ")
	fmt.Println(string(raw))
	os.Exit(1)
}

func loadConfig(fn string) (*inputConfig, error) {
	cfgset := &inputConfig{}

	f, err := os.Open(fn)
	log.Println(flag.Arg(0))
	if err != nil {
		return nil, err
	}
	err = json.NewDecoder(f).Decode(&cfgset)
	if err != nil {
		return nil, err
	}

	return cfgset, f.Close()
}

func getReader(fn string) (*csv.Reader, error) {
	fx, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	var fr io.ReadCloser = fx
	if strings.HasSuffix(strings.ToLower(fn), ".gz") {
		fr, err = gzip.NewReader(fx)
		if err != nil {
			fx.Close()
			return nil, err
		}
		fn = fn[:len(fn)-3] // remove the ".gz"
	}

	r := csv.NewReader(fr)
	if !strings.HasSuffix(strings.ToLower(fn), "csv") {
		// if it doesn't end with csv assume it's tab-delimited
		r.Comma = '\t'
		r.LazyQuotes = true
	}
	return r, nil
}

var seps = regexp.MustCompile("[_. -]+")

func main() {
	outname := flag.String("o", "-", "output to `filename(.txt|.sqlite)`")
	dryRun := flag.Bool("p", false, "`pretend` to do the parsing (aka dry run)")
	flag.Parse()

	if flag.NArg() == 0 {
		showHelp()
	}

	cfgset, err := loadConfig(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}

	fout, err := ioutil.TempFile("", "data4recon.*.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		//log.Println(fout.Name())
		fout.Close()
		os.Remove(fout.Name())
	}()
	propSet := make(map[string]map[string]struct{})
	var out [4]string

	haveTypes := make(map[string]struct{})
	typeSet := make([]map[string]string, len(cfgset.Files))
	for i, fc := range cfgset.Files {
		if _, ok := haveTypes[fc.ID]; !ok {
			haveTypes[fc.ID] = struct{}{}
			typeSet[i] = map[string]string{
				"id":          fc.ID,
				"name":        fc.Name,
				"description": fc.Description,
				"url":         cfgset.ViewURL,
			}
		}

		log.Printf("Reading data from: '%s'...", fc.Filename)
		r, err := getReader(fc.Filename)
		if err != nil {
			log.Fatal(err)
		}

		out[2] = fc.ID

		header, err := r.Read()
		if err != nil {
			log.Fatal(err)
		}
		maxh := 0
		for _, h := range header {
			if len(h) > maxh {
				maxh = len(h)
			}
		}
		for i, h := range header {
			log.Printf("Column %3d. %*s ==> '%s'", i, -maxh, h, fc.Properties[i])
		}
		if *dryRun {
			continue
		}

		nrec := 0
		fmt.Fprint(os.Stderr, "Reading data...\n")
		rec, err := r.Read()
		for err == nil {
			nrec++
			fmt.Fprintf(os.Stderr, "  %10d\r", nrec)
			os.Stderr.Sync()

			props := make(map[string]string)
			for i, propName := range fc.Properties {
				switch propName {
				case "":
					continue
				case "id":
					out[0] = rec[i]
				case "name":
					out[1] = rec[i]
				default:
					if rec[i] != "" && rec[i] != "-" {
						props[propName] = rec[i]
					}
				}
			}
			if len(props) == 0 {
				out[3] = "{}"
			} else {
				raw, _ := json.Marshal(props)
				out[3] = string(raw)
			}
			fmt.Fprintln(fout, strings.Join(out[:], "\t"))

			for propName := range props {
				if _, ok := propSet[propName]; !ok {
					propSet[propName] = make(map[string]struct{})
				}
				propSet[propName][out[2]] = struct{}{}
			}

			rec, err = r.Read()
		}
		fmt.Fprint(os.Stderr, "\n  Done.\n")

		if err != nil {
			log.Println(err)
		}
	}

	fout.Seek(0, io.SeekStart)
	s := bufio.NewScanner(fout)

	///// everything now being sent to output

	if strings.Contains(*outname, "sqlite") {
		err := outputToSqlite(*outname, typeSet, cfgset, propSet, s)
		if err != nil {
			log.Println(err)
		}
		return
	}

	var dest io.WriteCloser = os.Stdout
	if *outname != "-" {
		f, err := os.Create(*outname)
		if err != nil {
			log.Fatal(err)
		}
		dest = f
	}

	err = outputToFlatfile(dest, typeSet, cfgset, propSet, s)
	if err != nil {
		log.Fatal(err)
	}
}

func outputToFlatfile(dest io.WriteCloser, typeSet []map[string]string, cfgset *inputConfig,
	propSet map[string]map[string]struct{}, s *bufio.Scanner) error {

	typesjson, _ := json.Marshal(typeSet)
	fmt.Fprintf(dest, "%s\t%s\t%s\t%s\n", cfgset.IdentifierNamespace, cfgset.Name,
		cfgset.SchemaNamespace, string(typesjson))

	var out [4]string
	out[3] = "{}"

	for propName, ents := range propSet {
		out[0] = propName
		out[1] = strings.Title(strings.TrimSpace(seps.ReplaceAllString(propName, " ")))
		for etype := range ents {
			out[2] = "property," + etype
			fmt.Fprintln(dest, strings.Join(out[:], "\t"))
		}
	}

	for s.Scan() {
		fmt.Fprintln(dest, s.Text())
	}
	return dest.Close()
}

func outputToSqlite(filename string, typeSet []map[string]string, cfgset *inputConfig,
	propSet map[string]map[string]struct{}, s *bufio.Scanner) error {
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return err
	}
	defer db.Close()
	for _, ddl := range schema {
		_, err = db.Exec(ddl)
		if err != nil {
			return err
		}
	}
	////
	// add in the global metadata
	_, err = db.Exec("INSERT INTO recongo_metadata (meta_key, meta_value) VALUES (?,?);",
		"name", cfgset.Name)
	if err != nil {
		return err
	}
	_, err = db.Exec("INSERT INTO recongo_metadata (meta_key, meta_value) VALUES (?,?);",
		"identifierNamespace", cfgset.IdentifierNamespace)
	if err != nil {
		return err
	}
	_, err = db.Exec("INSERT INTO recongo_metadata (meta_key, meta_value) VALUES (?,?);",
		"schemaNamespace", cfgset.SchemaNamespace)
	if err != nil {
		return err
	}
	_, err = db.Exec("INSERT INTO recongo_metadata (meta_key, meta_value) VALUES (?,?);",
		"view_url", cfgset.ViewURL)
	if err != nil {
		return err
	}

	for _, t := range typeSet {
		_, err = db.Exec("INSERT INTO recongo_types (type_id,type_name,type_description,type_url) VALUES (?,?,?,?);",
			t["id"], t["name"], t["description"], t["url"])
		if err != nil {
			return err
		}
	}

	for propID, etypes := range propSet {
		fancyName := strings.Title(strings.TrimSpace(seps.ReplaceAllString(propID, " ")))
		_, err = db.Exec("INSERT INTO recongo_properties (prop_id,prop_name) VALUES (?,?);",
			propID, fancyName)
		if err != nil {
			return err
		}

		for typeID := range etypes {
			_, err = db.Exec("INSERT INTO recongo_props2types (prop_id,type_id) VALUES (?,?);",
				propID, typeID)
			if err != nil {
				return err
			}

		}
	}

	////
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO recongo_entities (ent_id, ent_name, ent_types, ent_description)
	VALUES (?,?,?,?);`)
	if err != nil {
		tx.Rollback()
		return err
	}
	stmt2, err := tx.Prepare(`INSERT INTO recongo_entity_properties (ent_types, ent_id, prop_id, prop_value)
		VALUES (?,?,?,?);`)
	if err != nil {
		tx.Rollback()
		return err
	}

	fmt.Fprint(os.Stderr, "Saving to database...\n")
	nrec := 0
	x := make(map[string]string, 20)
	for s.Scan() {
		nrec++
		fmt.Fprintf(os.Stderr, "  %10d\r", nrec)
		os.Stderr.Sync()

		rec := strings.Split(s.Text(), "\t")
		desc := ""
		if rec[3] != "{}" {
			json.Unmarshal([]byte(rec[3]), &x)
			if d, ok := x["description"]; ok {
				desc = d
				delete(x, "description")
			}
			if len(x) > 0 {
				for propID, propVal := range x {
					_, err = stmt2.Exec(rec[2], rec[0], propID, propVal)
					if err != nil {
						stmt.Close()
						stmt2.Close()
						tx.Rollback()
						return err
					}
					delete(x, propID)
				}
				// recongo_entity_properties
			}
		}
		_, err = stmt.Exec(rec[0], rec[1], rec[2], desc)
		if err != nil {
			stmt.Close()
			stmt2.Close()
			tx.Rollback()
			return err
		}
	}
	stmt.Close()
	stmt2.Close()
	_, err = tx.Exec("INSERT INTO recongo_entities_fts(recongo_entities_fts) VALUES ('rebuild');")
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

var schema = []string{
	`CREATE TABLE recongo_metadata (
		meta_key varchar primary key,
		meta_value varchar
	);`,

	`CREATE TABLE recongo_types (
		type_id varchar primary key,
		type_name varchar,
		type_description varchar,
		type_url varchar
	);`,

	`CREATE TABLE recongo_properties (
		prop_id varchar primary key,
		prop_name varchar,
		prop_description varchar
	);`,

	`CREATE TABLE recongo_props2types (
		prop_id varchar references recongo_properties (prop_id),
		type_id vachar references recongo_types (type_id),
		primary key (prop_id, type_id)
	);`,

	`CREATE TABLE recongo_entities (
		ent_types varchar, -- comma-separated list of type_ids
		ent_id varchar,
		ent_name varchar,
		ent_description varchar,
		primary key(ent_id, ent_types)
	);`,

	`CREATE TABLE recongo_entity_properties (
		ent_types varchar,
		ent_id varchar,
		prop_id varchar,
		prop_value varchar,
		primary key (ent_types,ent_id,prop_id,prop_value)
	)`,

	`CREATE VIRTUAL TABLE recongo_entities_fts USING fts5
		(ent_id, ent_name, ent_description, ent_types, content=recongo_entities);`,
}
