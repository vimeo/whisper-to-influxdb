tool to import whisper data into InfluxDB

It works!
There's 2 modes of operation:

* standard: copies the from-to timerange from the best fitting whisper archive (the one with highest resolution that covers the range)
* if you specify `-all`, ignores from/to, and copies all data from all archives.  currently it copies from lowest resolution (longest range) to higher resolution (shorter range), because they overlap and so that the records from more accurate archives overwrite earlier, less accurate records with the same timestamp.  Later, this can be extended to make the points from different archives go to different series, which is how you implement retention policies in InfluxDB.

options:

```
usage of whisper-to-influxdb:
  -all=false: copy all data from all archives, as opposed to just querying the timerange from the best archive
  -exclude="": don't process whisper files whose filename contains this string ("" disables the filter, and matches nothing
  -from=1412110823: Unix epoch time of the beginning of the requested interval. (default: 24 hours ago). ignored if all=true
  -include="": only process whisper files whose filename contains this string ("" is a no-op, and matches everything
  -influxDb="graphite": influxdb database
  -influxHost="localhost": influxdb host
  -influxPass="graphite": influxdb pass
  -influxPort=8086: influxdb port
  -influxPrefix="": prefix this string to all imported data
  -influxUser="graphite": influxdb user
  -influxWorkers=10: specify how many influx workers
  -skipUntil="": absolute path of a whisper file from which to resume processing
  -statsInterval=10: interval to display stats. by default 10 seconds.
  -until=1412197223: Unix epoch time of the end of the requested interval. (default: now). ignored if all=true
  -verbose=false: verbose output
  -whisperDir="/opt/graphite/storage/whisper/": location where all whisper files are stored
  -whisperWorkers=10: specify how many whisper workers
```
