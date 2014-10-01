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
var foundFiles chan string
var finishedFiles chan string
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
var all bool

var statsInterval uint
var exit chan int

func seriesString(s *client.Series) string {
	return fmt.Sprintf("InfluxDB series '%s' (%d points)", s.Name, len(s.Points))
}

// needed to keep track of what's the next file in line that needs processing
// because the workers can finish out of order, relative to the order
// of the filesystem walk which uses inode order.
// this ensures if you use skipUntil, it resumes from the right pos, without forgetting any
// other files that also needed processing.
func keepOrder() {
	type inProgress struct {
		Path string
		Next *inProgress
	}
	var firstInProgress *inProgress
	// we keep a list, the InProgress list, like so : A-B-C-D-E-F
	// the order of that list, is the inode/filesystem order

	for {
		select {
		case found := <-foundFiles:
			// add item to end of linked list
			i := &inProgress{
				Path: found,
			}
			if firstInProgress == nil {
				firstInProgress = i
			} else {
				var cur *inProgress
				for cur = firstInProgress; cur.Next != nil; cur = cur.Next {
				}
				cur.Next = i
			}
		case finished := <-finishedFiles:
			// firstInProgress will always be non-nil, because the file must have been added before it finished
			// in the list above,
			// when B completes, strip it out of the list (link to A to C)
			// when A completes, delete A, update first to point to B
			var prev *inProgress
			for cur := firstInProgress; cur != nil; cur = cur.Next {
				if cur.Path == finished {
					if prev != nil {
						prev.Next = cur.Next
					} else {
						firstInProgress = cur.Next
					}
					break
				}
				prev = cur
			}
		case code := <-exit:
			if firstInProgress != nil {
				fmt.Println("the next file that needed processing was", firstInProgress.Path, "you can resume from there")
			}
			os.Exit(code)
		}
	}
}

func influxWorker() {
	for abstractSerie := range influxSeries {
		influxPoints := make([][]interface{}, len(abstractSerie.Points), len(abstractSerie.Points))
		// TODO: if there are no points, we can just break out
		for i, abstractPoint := range abstractSerie.Points {
			influxPoint := make([]interface{}, 3, 3)
			influxPoint[0] = abstractPoint.Timestamp
			influxPoint[1] = 1
			influxPoint[2] = abstractPoint.Value

			influxPoints[i] = influxPoint
		}
		basename := strings.TrimSuffix(abstractSerie.Path[len(whisperDir):], ".wsp")
		name := strings.Replace(basename, "/", ".", -1)
		influxSerie := client.Series{
			Name:    influxPrefix + name,
			Columns: []string{"time", "sequence_number", "value"},
			Points:  influxPoints,
		}
		pre := time.Now()
		toCommit := []*client.Series{&influxSerie}
		err := influxClient.WriteSeriesWithTimePrecision(toCommit, client.Second)
		duration := time.Since(pre)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write %s: %s (operation took %v)\n", seriesString(&influxSerie), err.Error(), duration)
			exit <- 2
		}
		if verbose {
			fmt.Println("committed", seriesString(&influxSerie))
		}
		influxWriteTimer.Update(duration)
		finishedFiles <- abstractSerie.Path

	}
	influxWorkersWg.Done()
}

type abstractSerie struct {
	Path   string // used to keep track of ordering of processing
	Points []whisper.Point
}

func whisperWorker() {
	for path := range whisperFiles {
		fd, err := os.Open(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Failed to open whisper file '%s': %s\n", path, err.Error())
			continue
		}
		w, err := whisper.OpenWhisper(fd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Failed to open whisper file '%s': %s\n", path, err.Error())
			continue
		}
		pre := time.Now()

		var duration time.Duration
		var points []whisper.Point
		if all {
			numTotalPoints := uint32(0)
			for i := range w.Header.Archives {
				numTotalPoints += w.Header.Archives[i].Points
			}
			points = make([]whisper.Point, 0, numTotalPoints)
			// iterate in backwards archive order (low res to high res)
			// so that if you write points of multiple archives to the same series, the high res ones will overwrite the low res ones
			for i := len(w.Header.Archives) - 1; i >= 0; i-- {
				allPoints, err := w.DumpArchive(i)
				if err != nil {
					fmt.Fprintf(os.Stderr, "ERROR: Failed to read archive %d in '%s', skipping: %s\n", i, path, err.Error())
					continue
				}
				for _, point := range allPoints {
					// we have to filter out the "None" records (where we didn't fill in data) explicitly here!
					if point.Timestamp != 0 {
						points = append(points, point)
					}
				}
			}
			duration = time.Since(pre)
		} else {
			// not sure how it works, but i've emperically verified that this ignores null records, which is what we want
			// i.e. if whisper has a slot every minute, but you only have data every 3 minutes, we'll only process those records
			_, points, err = w.FetchUntil(fromTime, untilTime)
			duration = time.Since(pre)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: Failed to read file '%s' from %d to %d, skipping: %s (operation took %v)\n", path, fromTime, untilTime, err.Error(), duration)
				continue
			}
		}

		whisperReadTimer.Update(duration)
		serie := &abstractSerie{path, points}
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

	foundFiles <- path
	whisperFiles <- path
	return nil
}

func init() {
	whisperFiles = make(chan string)
	influxSeries = make(chan *abstractSerie)
	foundFiles = make(chan string)
	finishedFiles = make(chan string)
	exit = make(chan int)

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
	flag.UintVar(&from, "from", yesterday, "Unix epoch time of the beginning of the requested interval. (default: 24 hours ago). ignored if all=true")
	flag.UintVar(&until, "until", now, "Unix epoch time of the end of the requested interval. (default: now). ignored if all=true")
	flag.StringVar(&influxHost, "influxHost", "localhost", "influxdb host")
	flag.UintVar(&influxPort, "influxPort", 8086, "influxdb port")
	flag.StringVar(&influxUser, "influxUser", "graphite", "influxdb user")
	flag.StringVar(&influxPass, "influxPass", "graphite", "influxdb pass")
	flag.StringVar(&influxDb, "influxDb", "graphite", "influxdb database")
	flag.StringVar(&skipUntil, "skipUntil", "", "absolute path of a whisper file from which to resume processing")
	flag.StringVar(&influxPrefix, "influxPrefix", "", "prefix this string to all imported data")
	flag.StringVar(&include, "include", "", "only process whisper files whose filename contains this string (\"\" is a no-op, and matches everything")
	flag.StringVar(&exclude, "exclude", "", "don't process whisper files whose filename contains this string (\"\" disables the filter, and matches nothing")
	flag.BoolVar(&verbose, "verbose", false, "verbose output")
	flag.BoolVar(&all, "all", false, "copy all data from all archives, as opposed to just querying the timerange from the best archive")
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
	go metrics.Log(metrics.DefaultRegistry, time.Duration(statsInterval)*time.Second, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	for i := 1; i <= influxWorkers; i++ {
		influxWorkersWg.Add(1)
		go influxWorker()
	}
	for i := 1; i <= whisperWorkers; i++ {
		whisperWorkersWg.Add(1)
		go whisperWorker()
	}

	go keepOrder()

	err = filepath.Walk(whisperDir, process)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		exit <- 2
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
	if verbose {
		fmt.Println("all done. exiting")
	}
}
