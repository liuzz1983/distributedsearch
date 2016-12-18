package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/tidwall/finn"
)

func main() {
	var port int
	var backend string
	var durability string
	var consistency string
	var loglevel string
	var join string
	var dir string

	flag.IntVar(&port, "p", 7481, "Bind port")
	flag.StringVar(&backend, "backend", "fastlog", "Raft log backend [fastlog,bolt,inmem]")
	flag.StringVar(&durability, "durability", "medium", "Log durability [low,medium,high]")
	flag.StringVar(&consistency, "consistency", "medium", "Raft consistency [low,medium,high]")
	flag.StringVar(&loglevel, "loglevel", "notice", "Log level [quiet,warning,notice,verbose,debug]")
	flag.StringVar(&dir, "dir", "data", "Data directory")
	flag.StringVar(&join, "join", "", "Join a cluster by providing an address")
	flag.Parse()

	var opts finn.Options

	switch strings.ToLower(backend) {
	default:
		log.Fatalf("invalid backend '%v'", backend)
	case "fastlog":
		opts.Backend = finn.FastLog
	case "bolt":
		opts.Backend = finn.Bolt
	case "inmem":
		opts.Backend = finn.InMem
	}
	switch strings.ToLower(durability) {
	default:
		log.Fatalf("invalid durability '%v'", durability)
	case "low":
		opts.Durability = finn.Low
	case "medium":
		opts.Durability = finn.Medium
	case "high":
		opts.Durability = finn.High
	}
	switch strings.ToLower(consistency) {
	default:
		log.Fatalf("invalid consistency '%v'", consistency)
	case "low":
		opts.Consistency = finn.Low
	case "medium":
		opts.Consistency = finn.Medium
	case "high":
		opts.Consistency = finn.High
	}
	switch strings.ToLower(loglevel) {
	default:
		log.Fatalf("invalid loglevel '%v'", loglevel)
	case "quiet":
		opts.LogOutput = ioutil.Discard
	case "warning":
		opts.LogLevel = finn.Warning
	case "notice":
		opts.LogLevel = finn.Notice
	case "verbose":
		opts.LogLevel = finn.Verbose
	case "debug":
		opts.LogLevel = finn.Debug
	}
	n, err := finn.Open(dir, fmt.Sprintf(":%d", port), join, NewClone(), &opts)
	if err != nil {
		if opts.LogOutput == ioutil.Discard {
			log.Fatal(err)
		}
	}
	defer n.Close()
	select {}
}
