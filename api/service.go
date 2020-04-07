package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/joiningdata/recongo/model"
)

// Service provides an implementation of the Reconciliation Service API.
type Service struct {
	// embedded servemux allows Service to act as one also
	*http.ServeMux

	manifest *Manifest
	source   model.Source
}

// helper to package JSON response (optional JSONP) content with CORS header
func handleJSONP(w http.ResponseWriter, r *http.Request, payload interface{}) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	callback := r.URL.Query().Get("callback")
	if callback != "" {
		w.Header().Set("Content-Type", "application/javscript")
		fmt.Fprint(w, "/**/"+callback+"(")
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
	err := json.NewEncoder(w).Encode(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	if callback != "" {
		fmt.Fprintln(w, ");")
	}
}

func (s *Service) suggestEntity(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	results := s.source.QueryPrefix(prefix, 25)
	handleJSONP(w, r, map[string]interface{}{"result": results})
}

func (s *Service) suggestType(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	low := strings.ToLower(prefix)
	types := s.source.Types()
	results := []*model.Type{}
	for _, t := range types {
		if strings.HasPrefix(strings.ToLower(t.Name), low) {
			results = append(results, t)
		}
	}
	if len(results) == 0 {
		for _, t := range types {
			if strings.Contains(strings.ToLower(t.Name), low) {
				results = append(results, t)
			}
		}
	}
	handleJSONP(w, r, map[string]interface{}{"result": results})
}

func (s *Service) suggestProps(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	low := strings.ToLower(prefix)
	hits := make(map[string]*model.Property)

	types := s.source.Types()
	for _, t := range types {
		props := s.source.Properties(t.ID)
		for _, p := range props {
			if _, ok := hits[p.ID]; ok {
				continue
			}
			if strings.HasPrefix(strings.ToLower(p.Name), low) {
				hits[p.ID] = p
			}
		}
	}
	if len(hits) == 0 {
		for _, t := range types {
			props := s.source.Properties(t.ID)
			for _, p := range props {
				if _, ok := hits[p.ID]; ok {
					continue
				}
				if strings.Contains(strings.ToLower(p.Name), low) {
					hits[p.ID] = p
				}
			}
		}
	}
	results := make([]*model.Property, 0, len(hits))
	for _, p := range hits {
		results = append(results, p)
	}
	handleJSONP(w, r, map[string]interface{}{"result": results})
}

func (s *Service) listProperties(w http.ResponseWriter, r *http.Request) {
	resp := struct {
		Limit      int               `json:"limit"`
		Type       string            `json:"type"`
		Properties []*model.Property `json:"properties"`
	}{}
	resp.Type = r.URL.Query().Get("type")
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		x, err := strconv.ParseInt(limitStr, 10, 64)
		if err == nil {
			resp.Limit = int(x)
		}
	}

	resp.Properties = s.source.Properties(resp.Type)

	handleJSONP(w, r, resp)
}

func (s *Service) reconHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Query().Get("queries") == "" {
		// no queries over GET, send the manifest instead
		handleJSONP(w, r, s.manifest)
		return
	}

	// query can be sent over GET or POST
	var queries map[string]*model.QueryRequest
	if r.Method == http.MethodGet {
		qbytes := []byte(r.URL.Query().Get("queries"))
		err := json.Unmarshal(qbytes, &queries)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		err := r.ParseForm()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		qbytes := []byte(r.Form.Get("queries"))
		err = json.Unmarshal(qbytes, &queries)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// collect results for each query
	type ResultSet struct {
		R []*model.Candidate `json:"result"`
	}
	results := make(map[string]*ResultSet, len(queries))
	for qid, q := range queries {
		q.ID = qid
		resp, err := s.source.Query(q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		results[resp.ID] = &ResultSet{R: resp.Results}
	}

	handleJSONP(w, r, results)
}

// NewService returns a new service provider bound to the specified url and
// prefix, which serves reconciliation request for the given data source.
func NewService(urlRoot, prefix string, src model.Source) *Service {

	m := &Manifest{
		Versions:        []string{"0.1", "0.2"},
		Name:            src.Name(),
		IdentifierSpace: src.IdentifierNS(),
		SchemaSpace:     src.SchemaNS(),
		DefaultTypes:    src.Types(),

		Suggest: &Suggest{
			Entity: &ServiceDefinition{
				ServiceURL:  urlRoot + prefix,
				ServicePath: "/auto/entities",
			},
			Type: &ServiceDefinition{
				ServiceURL:  urlRoot + prefix,
				ServicePath: "/auto/types",
			},
			Property: &ServiceDefinition{
				ServiceURL:  urlRoot + prefix,
				ServicePath: "/auto/properties",
			},
		},

		Extend: &Extend{
			ProposeProperties: &ServiceDefinition{
				ServiceURL:  urlRoot + prefix,
				ServicePath: "/properties",
			},
			//PropertySettings: []*PropertySetting{},
		},
	}

	if vu := src.ViewURL(); vu != "" {
		m.View = &View{
			URL: URLTemplate(vu),
		}
	}

	s := &Service{
		ServeMux: http.NewServeMux(),
		manifest: m,
		source:   src,
	}
	s.HandleFunc(prefix, s.reconHandler)
	s.HandleFunc(prefix+"/auto/entities", s.suggestEntity)
	s.HandleFunc(prefix+"/auto/types", s.suggestType)
	s.HandleFunc(prefix+"/auto/properties", s.suggestProps)
	s.HandleFunc(prefix+"/properties", s.listProperties)
	return s
}
