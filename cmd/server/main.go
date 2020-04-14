package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"runtime/pprof"
	"sync"

	"github.com/joiningdata/recongo/api"
	"github.com/joiningdata/recongo/model"
)

func main() {
	publicURL := flag.String("h", "http://127.0.0.1:8080", "public-accessible address root")
	prefix := flag.String("p", "/api", "URL `prefix` to serve requests from")
	addr := flag.String("i", ":8080", "`port:address` to listen for http requests")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	src, err := model.Load(flag.Arg(0))
	if err != nil {
		log.Fatal(flag.Arg(0), err)
	}

	wg := sync.WaitGroup{}

	service := api.NewService(*publicURL, *prefix, src)
	service.HandleFunc("/quit", func(w http.ResponseWriter, r *http.Request) {
		wg.Done()
	})

	wg.Add(1)
	go func() {
		log.Println("Listening at " + *publicURL + *prefix)
		log.Println("  Load " + *publicURL + "/quit to force a clean shutdown")
		err = http.ListenAndServe(*addr, service)
		if err != nil {
			log.Fatal(err)
		}
		wg.Done()
	}()

	wg.Wait()
}
