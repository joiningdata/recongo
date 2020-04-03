package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/joiningdata/recongo/api"
	"github.com/joiningdata/recongo/model"
)

func main() {
	publicURL := flag.String("h", "https://example.com", "public-accessible address root")
	prefix := flag.String("p", "/api", "URL `prefix` to serve requests from")
	addr := flag.String("i", ":8080", "`port:address` to listen for http requests")
	flag.Parse()

	src, err := model.Load(flag.Arg(0))
	if err != nil {
		log.Fatal(flag.Arg(0), err)
	}

	service := api.NewService(*publicURL, *prefix, src)

	err = http.ListenAndServe(*addr, service)
	if err != nil {
		log.Fatal(err)
	}
}
