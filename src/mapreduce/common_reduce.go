package mapreduce

import (
    "os"
    "encoding/json"
    "sort"
    "log"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
    keyToValues := make(map[string][]string)
    decodeF := func(decoder *json.Decoder) {
        for decoder.More() {
            var kv KeyValue
            err := decoder.Decode(&kv)
            if err != nil {
                log.Fatalf("decoder decode fail, err: %s", err)
            }
            if _, ok := keyToValues[kv.Key]; !ok {
                keyToValues[kv.Key] = make([]string, 0)
            }
            keyToValues[kv.Key] = append(keyToValues[kv.Key], kv.Value)
        }
    }
    for mapTask := 0; mapTask < nMap; mapTask++ {
        inFile := reduceName(jobName, mapTask, reduceTask)
        in, err := os.Open(inFile)
        if err != nil {
            log.Fatalf("open infile fail, infile: %s, err: %s", inFile, err)
        }
        defer in.Close()
        decodeF(json.NewDecoder(in))
    }

    keys := make([]string, len(keyToValues))
    idx := 0
    for k,_ := range keyToValues {
        keys[idx] = k
        idx++
    }
    sort.Strings(keys)

    out, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
    if err != nil {
        log.Fatalf("open outfile fail, outfile: %s, err: %s", outFile, err)
    }
    defer out.Close()
    log.Printf("output file: %s", outFile)
    encoder := json.NewEncoder(out)
    for _, key := range(keys) {
        if _, ok := keyToValues[key]; !ok {
            continue
        }
        retValue := reduceF(key, keyToValues[key])
        err := encoder.Encode(KeyValue{key, retValue})
        if err != nil {
            log.Fatalf("encode json string fail, err: %s", err)
        }
    }
}
