package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	AQLib "github.com/00pauln00/aqi-monitor/lib"
	PumiceDBServer "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceserver"
	log "github.com/sirupsen/logrus"
	PumiceDBCommon "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
    PumiceDBFunc "github.com/00pauln00/niova-pumicedb/go/pkg/pumicefunc/server"
)

/*
#include <stdlib.h>
#include <string.h>
*/
// import "C"

var seqno = 0
var raftUuid string
var peerUuid string
var logDir string

// Use the default column family
var colmfamily = "PMDBTS_CF"

type AQServer struct {
	raftUuid       string
	peerUuid       string
	columnFamilies string
	pso            *PumiceDBServer.PmdbServerObject
}

func main() {

	//Print help message.
	if len(os.Args) == 1 || os.Args[1] == "-help" || os.Args[1] == "-h" {
		fmt.Println("Positional Arguments: \n		'-r' - RAFT UUID \n		'-u' - PEER UUID")
		fmt.Println("Optional Arguments: \n		'-l' - Log Dir Path \n		-h, -help")
		fmt.Println("covid_app_server -r <RAFT UUID> -u <PEER UUID> -l <log directory>")
		os.Exit(0)
	}

	aq := parseArgs()

	//Create log directory if not Exist.
	makeDirectoryIfNotExists()

	//Create log file.
	initLogger(aq)

	log.Info("Raft UUID: %s", aq.raftUuid)
	log.Info("Peer UUID: %s", aq.peerUuid)

	/*
	   Initialize the internal pmdb-server-object pointer.
	   Assign the Directionary object to PmdbAPI so the apply and
	   read callback functions can be called through pmdb common library
	   functions.
	*/
	aq.pso = &PumiceDBServer.PmdbServerObject{
		ColumnFamilies: []string{colmfamily},
		RaftUuid:       aq.raftUuid,
		PeerUuid:       aq.peerUuid,
		PmdbAPI:        aq.pso.PmdbAPI, //aq
	}

	// Start the pmdb server
	err := aq.pso.Run()

	if err != nil {
		log.Error(err)
	}
}



func parseArgs() *AQServer {
	aq := &AQServer{}

	flag.StringVar(&aq.raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&aq.peerUuid, "u", "NULL", "peer uuid")
	flag.StringVar(&logDir, "l", "/tmp/AirQualityAPPLog", "log dir")

	flag.Parse()

	return aq
}

/*If log directory is not exist it creates directory.
  and if dir path is not passed then it will create
  log file in "/tmp/covidAppLog" path.
*/
func makeDirectoryIfNotExists() error {

	if _, err := os.Stat(logDir); os.IsNotExist(err) {

		return os.Mkdir(logDir, os.ModeDir|0755)
	}

	return nil
}

//Create logfile for each peer.
func initLogger(aq *AQServer) {

	var filename string = logDir + "/" + aq.peerUuid + ".log"

	fmt.Println("logfile:", filename)

	//Create the log file if doesn't exist. And append to it if it already exists.
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	Formatter := new(log.TextFormatter)

	//Set Timestamp format for logfile.
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)

	if err != nil {
		// Cannot open log file. Logging to stderr
		fmt.Println(err)
	} else {
		log.SetOutput(f)
	}
}

func (aq *AQServer) Init(initArgs *PumiceDBServer.PmdbCbArgs) {
	return
}

func (aq *AQServer) WritePrep(wrPrepArgs *PumiceDBServer.PmdbCbArgs) int64 {
	return 0
}


func (aq *AQServer) Apply(applyArgs *PumiceDBServer.PmdbCbArgs) int64 {

	log.Info("AirQuality_Data app server: Apply request received")
	/* Decode the input buffer into structure format */
	applyAQ := &AQLib.AirInfo{}

	decodeErr := aq.pso.DecodeApplicationReq(applyArgs.Payload, applyAQ)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}

	log.Info("Key passed by client: ", applyAQ.Location)

	//length of key.
	keyLength := len(applyAQ.Location)

	//Lookup the key first
	prevResult, err := aq.pso.LookupKey(applyAQ.Location,
		int64(keyLength), colmfamily)

	log.Info("Previous values of the AirQualityData: ", prevResult)

	if err == nil{
		//updation
	}

	//format for the airquality data inserted into the pumicedb
	aqDataVal := fmt.Sprintf(
		"%.6f %.6f %s %v",
		applyAQ.Latitude,
		applyAQ.Longitude,
		applyAQ.Timestamp.Format(time.RFC3339), // ISO format
		applyAQ.Pollutants,
	)

	aqDataLen:= len(aqDataVal)

	log.Info("Current aqData values: ", aqDataVal)
	log.Info("Write the KeyValue by calling PmdbWriteKV")

	log.Info("Write the KeyValue by calling PmdbWriteKV")

	rc := aq.pso.WriteKV(applyArgs.UserID, applyArgs.PmdbHandler,
		applyAQ.Location,
		int64(keyLength), aqDataVal,
		int64(aqDataLen), colmfamily)

	return int64(rc)
	
}


func (aq *AQServer) Read(readArgs *PumiceDBServer.PmdbCbArgs) int64 {

	log.Info("AQ App: Read request received")
	//Decode the request structure sent by client.
	reqStruct :=  &AQLib.AirInfo{}
	decodeErr := aq.pso.DecodeApplicationReq(readArgs.Payload, reqStruct)

	if decodeErr != nil {
		log.Error("Failed to decode the read request")
		return -1
	}

	log.Info("Key passed by client: ", reqStruct.Location)

	keyLen := len(reqStruct.Location)
	log.Info("Key length: ", keyLen)


	/* Pass the work as key to PmdbReadKV and get the value from pumicedb */
	readRsult, readErr := aq.pso.ReadKV(readArgs.UserID, reqStruct.Location,
		int64(keyLen), colmfamily)

	var splitValues []string

	if readErr == nil {
		//split space separated values.
		splitValues = strings.Split(string(readRsult), " ")
	}
	
	lat, _ := strconv.ParseFloat(splitValues[0],64)
	lon, _ := strconv.ParseFloat(splitValues[1],64)
	ts,_:= time.Parse(time.RFC3339, splitValues[2])

	pollutants:= make(map[string]float64)

	pollStr:= strings.TrimPrefix(splitValues[3], "map[")
	pollStr = strings.TrimSuffix(splitValues[3], "]")
	
	for _, kv:= range strings.Split(pollStr, " "){
		parts:= strings.Split(kv, ":")
		if(len(parts) == 2){
			val, _ := strconv.ParseFloat(parts[1],64)
			pollutants[parts[0]] = val
		}
	}


	resultAQ:= AQLib.AirInfo{
		Location: reqStruct.Location,
		Latitude: lat,
		Longitude: lon,
		Timestamp: ts,
		Pollutants: pollutants,
	}

	//Copy the encoded result in replyBuffer
	replySize, copyErr := aq.pso.CopyDataToBuffer(resultAQ,
		readArgs.ReplyBuf)
	if copyErr != nil {
		log.Error("Failed to Copy result in the buffer: %s", copyErr)
		return -1
	}

	log.Info("Reply size: ", replySize)

	return replySize



}
