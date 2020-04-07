package main

import (
	"bufio"
	"compress/gzip"
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

func main() {
	seps := regexp.MustCompile("[_. -]+")

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

		rec, err := r.Read()
		for err == nil {
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
		if err != nil {
			log.Println(err)
		}
	}

	fout.Seek(0, io.SeekStart)

	typesjson, _ := json.Marshal(typeSet)
	fmt.Printf("%s\t%s\t%s\t%s\n", cfgset.IdentifierNamespace, cfgset.Name,
		cfgset.SchemaNamespace, string(typesjson))

	out[3] = "{}"

	for propName, ents := range propSet {
		out[0] = propName
		out[1] = strings.Title(strings.TrimSpace(seps.ReplaceAllString(propName, " ")))
		for etype := range ents {
			out[2] = "property," + etype
			fmt.Println(strings.Join(out[:], "\t"))
		}
	}

	s := bufio.NewScanner(fout)
	for s.Scan() {
		fmt.Println(s.Text())
	}
}
