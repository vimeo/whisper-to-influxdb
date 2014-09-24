package main

import (
	"flag"
	"fmt"
	"github.com/influxdb/influxdb/client"
	"github.com/kisielk/whisper-go/whisper"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var whisperDir string
var influxWorkers, whisperWorkers int
var from, until uint
var fromTime, untilTime uint32
var influxWorkersWg, whisperWorkersWg sync.WaitGroup
var whisperFiles chan string
var influxSeries chan *abstractSerie

var influxHost, influxUser, influxPass, influxDb string
var influxPort uint

var influxClient *client.Client

func dieIfErr(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}
}

func influxWorker() {
	for abstractSerie := range influxSeries {
		influxPoints := make([][]interface{}, len(abstractSerie.Points), len(abstractSerie.Points))
		for i, abstractPoint := range abstractSerie.Points {
			influxPoint := make([]interface{}, 3, 3)
			influxPoint[0] = abstractPoint.Timestamp
			influxPoint[1] = 1
			influxPoint[2] = abstractPoint.Value

			influxPoints[i] = influxPoint
		}
		influxSerie := client.Series{
			Name:    "whisper_import." + abstractSerie.Name,
			Columns: []string{"time", "sequence_number", "value"},
			Points:  influxPoints,
		}
		fmt.Println("committing", abstractSerie.Name)
		toCommit := []*client.Series{&influxSerie}
		err := influxClient.WriteSeries(toCommit)
		dieIfErr(err)
		fmt.Println("committed", abstractSerie.Name)

	}
	influxWorkersWg.Done()
}

type abstractSerie struct {
	Name   string
	Points []whisper.Point
}

func whisperWorker() {
	for path := range whisperFiles {
		fmt.Println("whisper loading", path)
		w, err := whisper.Open(path)
		if err != nil {
			log.Fatal(err)
		}
		_, points, err := w.FetchUntil(fromTime, untilTime)
		if err != nil {
			log.Fatal(err)
		}
		basename := strings.TrimSuffix(path[len(whisperDir):], ".wsp")
		name := strings.Replace(basename, "/", ".", -1)
		serie := &abstractSerie{name, points}
		influxSeries <- serie
	}
	whisperWorkersWg.Done()
}

func process(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if !strings.HasSuffix(path, ".wsp") {
		return nil
	}

	whisperFiles <- path
	return nil
}

func init() {
	whisperFiles = make(chan string)
	influxSeries = make(chan *abstractSerie)
}

func main() {
	now := uint(time.Now().Unix())
	yesterday := uint(time.Now().Add(-24 * time.Hour).Unix())

	flag.StringVar(&whisperDir, "whisperDir", "/opt/graphite/storage/whisper/", "location where all whisper files are stored")
	flag.IntVar(&influxWorkers, "influxWorkers", 10, "specify how many influx workers")
	flag.IntVar(&whisperWorkers, "whisperWorkers", 10, "specify how many whisper workers")
	flag.UintVar(&from, "from", yesterday, "Unix epoch time of the beginning of the requested interval. (default: 24 hours ago)")
	flag.UintVar(&until, "until", now, "Unix epoch time of the end of the requested interval. (default: now)")
	flag.StringVar(&influxHost, "influxHost", "localhost", "influxdb host")
	flag.UintVar(&influxPort, "influxPort", 8086, "influxdb port")
	flag.StringVar(&influxUser, "influxUser", "graphite", "influxdb user")
	flag.StringVar(&influxPass, "influxPass", "graphite", "influxdb pass")
	flag.StringVar(&influxDb, "influxDb", "graphite", "influxdb database")

	flag.Parse()
	fromTime = uint32(from)
	untilTime = uint32(until)

	cfg := &client.ClientConfig{
		Host:     fmt.Sprintf("%s:%d", influxHost, influxPort),
		Username: influxUser,
		Password: influxPass,
		Database: influxDb,
	}

	var err error
	influxClient, err = client.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= influxWorkers; i++ {
		influxWorkersWg.Add(1)
		go influxWorker()
	}
	for i := 1; i <= whisperWorkers; i++ {
		whisperWorkersWg.Add(1)
		go whisperWorker()
	}

	err = filepath.Walk(whisperDir, process)
	dieIfErr(err)
	close(whisperFiles)
	whisperWorkersWg.Wait()
	influxWorkersWg.Wait()
}
