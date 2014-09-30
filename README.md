tool to import whisper data into InfluxDB

it works, but remember,  
the most fitting resolution is chosen depending on the requested timerange,
so instead of importing everything ever at once, consider importing the timeranges separately
(there should be a nicer way to programmatically do this)


options:

```
usage of whisper-to-influxdb:
  -from=1234: Unix epoch time of the beginning of the requested interval. (default: 24 hours ago)
  -until=12341411679619: Unix epoch time of the end of the requested interval. (default: now)
  -include="": only process whisper files whose filename contains this string ("" is a no-op, and matches everything
  -exclude="": don't process whisper files whose filename contains this string ("" disables the filter, and matches nothing
  -influxDb="graphite": influxdb database
  -influxHost="localhost": influxdb host
  -influxPass="graphite": influxdb pass
  -influxPort=8086: influxdb port
  -influxPrefix="": prefix this string to all imported data
  -influxUser="graphite": influxdb user
  -skipUntil="": absolute path of a whisper file from which to resume processing
  -statsInterval=10: interval to display stats. by default 10 seconds.
  -verbose=false: verbose output
  -whisperDir="/opt/graphite/storage/whisper/": location where all whisper files are stored
  -influxWorkers=10: specify how many influx workers
  -whisperWorkers=10: specify how many whisper workers
```
