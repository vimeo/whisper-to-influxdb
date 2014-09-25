package main

import (
	"flag"
	"fmt"
	"github.com/influxdb/influxdb/client"
	"github.com/kisielk/whisper-go/whisper"
	"github.com/rcrowley/go-metrics"
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

var whisperReadTimer metrics.Timer
var influxWriteTimer metrics.Timer

var skipUntil string
var skipCounter uint64

var influxPrefix string
var include, exclude string
var verbose bool

var statsInterval uint

func seriesString(s *client.Series) string {
	return fmt.Sprintf("InfluxDB series '%s' (%d points)", s.Name, len(s.Points))
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
			Name:    influxPrefix + abstractSerie.Name,
			Columns: []string{"time", "sequence_number", "value"},
			Points:  influxPoints,
		}
		pre := time.Now()
		toCommit := []*client.Series{&influxSerie}
		err := influxClient.WriteSeriesWithTimePrecision(toCommit, client.Second)
		duration := time.Since(pre)
		if err != nil {
			log.Fatalf("Failed to write %s\n%s\nOperation took %v\n", seriesString(&influxSerie), err.Error(), duration)
		}
		if verbose {
			fmt.Println("committed", seriesString(&influxSerie))
		}
		influxWriteTimer.Update(duration)

	}
	influxWorkersWg.Done()
}

type abstractSerie struct {
	Name   string
	Points []whisper.Point
}

func whisperWorker() {
	for path := range whisperFiles {
		fd, err := os.Open(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Failed to open whisper file '%s'\n%s\n", path, err.Error())
			continue
		}
		w, err := whisper.OpenWhisper(fd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Failed to open whisper file '%s'\n%s\n", path, err.Error())
			continue
		}
		pre := time.Now()
		_, points, err := w.FetchUntil(fromTime, untilTime)
		duration := time.Since(pre)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Failed to read file '%s' from %d to %d, skipping\n%s\nOperation took %v\n", path, fromTime, untilTime, err.Error(), duration)
			continue
		}
		whisperReadTimer.Update(duration)
		basename := strings.TrimSuffix(path[len(whisperDir):], ".wsp")
		name := strings.Replace(basename, "/", ".", -1)
		serie := &abstractSerie{name, points}
		influxSeries <- serie
	}
	whisperWorkersWg.Done()
}

func process(path string, info os.FileInfo, err error) error {
	// skipuntil can be "", in normal operation, or because we resumed operation.
	// if it's != "", it means user requested skipping and we haven't hit that entry yet
	if path == skipUntil {
		skipUntil = ""
		fmt.Printf("found '%s', disabling skipping.  skipped %d files\n", path, skipCounter)
	}
	if err != nil {
		return err
	}
	if !strings.HasSuffix(path, ".wsp") {
		return nil
	}
	if exclude != "" && strings.Contains(path, exclude) {
		return nil
	}
	if !strings.Contains(path, include) {
		return nil
	}

	if skipUntil != "" {
		skipCounter += 1
		return nil
	}

	whisperFiles <- path
	return nil
}

func init() {
	whisperFiles = make(chan string)
	influxSeries = make(chan *abstractSerie)

	whisperReadTimer = metrics.NewTimer()
	influxWriteTimer = metrics.NewTimer()
	metrics.Register("whisper_read", whisperReadTimer)
	metrics.Register("influx_write", influxWriteTimer)
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
	flag.StringVar(&skipUntil, "skipUntil", "", "absolute path of a whisper file from which to resume processing")
	flag.StringVar(&influxPrefix, "influxPrefix", "whisper_import.", "prefix this string to all imported data")
	flag.StringVar(&include, "include", "", "only process whisper files whose filename contains this string (\"\" is a no-op, and matches everything")
	flag.StringVar(&exclude, "exclude", "", "don't process whisper files whose filename contains this string (\"\" disables the filter, and matches nothing")
	flag.BoolVar(&verbose, "verbose", false, "verbose output")
	flag.UintVar(&statsInterval, "statsInterval", 10, "interval to display stats. by default 10 seconds.")

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

	// i wish there was a way to enforce that logs gets displayed right before we quit
	go metrics.LogCompact(metrics.DefaultRegistry, time.Duration(statsInterval)*time.Second, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	for i := 1; i <= influxWorkers; i++ {
		influxWorkersWg.Add(1)
		go influxWorker()
	}
	for i := 1; i <= whisperWorkers; i++ {
		whisperWorkersWg.Add(1)
		go whisperWorker()
	}

	err = filepath.Walk(whisperDir, process)
	if err != nil {
		log.Fatal(err.Error())
	}
	if verbose {
		fmt.Println("fileWalk is done. closing channel")
	}
	close(whisperFiles)
	if verbose {
		fmt.Println("waiting for whisperworkers to finish")
	}
	whisperWorkersWg.Wait()
	close(influxSeries)
	if verbose {
		fmt.Println("waiting for influxworkers to finish")
	}
	influxWorkersWg.Wait()
}
