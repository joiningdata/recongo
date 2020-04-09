
all:
	go build --tags "fts5 json" ./cmd/server
	go build --tags "fts5 json" ./cmd/data4recon
