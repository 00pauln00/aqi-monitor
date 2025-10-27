package main

import (
	AQLib "github.com/00pauln00/aqi-monitor/lib"
	"fmt"
	"os"
	"flag"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	PumiceDBClient "github.com/00pauln00/niova-pumicedb/go/pkg/pumiceclient"
	PumiceDBCommon "github.com/00pauln00/niova-pumicedb/go/pkg/pumicecommon"
	"time"
	"errors"
	"strings"
	"strconv"
	"bytes"
    "encoding/gob"
    "encoding/json"
    "io/ioutil"


)

var (
	raftUuid      string
	clientUuid    string
	jsonFilePath  string
	cmd           string
	rwMap         map[string]map[string]string
	keyRncuiMap   map[string]string
	writeMultiMap map[*AQLib.AirInfo]string //without *
)

type aqData struct{
	Operation string
	Status    int
	Timestamp string
	Data      map[string]map[string]string
}



func main(){
	//Print help message.
	if len(os.Args) == 1 || os.Args[1] == "-help" || os.Args[1] == "-h" {
		fmt.Println("Positional Arguments: \n		'-r' - RAFT UUID \n		'-u' - CLIENT UUID")
		fmt.Println("optional Arguments: \n		'-l' - Json and Log File Path \n		-h, -help")
		fmt.Println("covid_app_client -r <RAFT UUID> -u <CLIENT UUID> -l <log directory>")
		os.Exit(0)
	}

	//Parse the cmdline parameter
	parseArgs()

	//Create log directory if not Exist.
	makeDirectoryIfNotExists()

	//Create log file.
	initLogger()

	log.Info("Raft UUID: ", raftUuid)
	log.Info("Client UUID: ", clientUuid)
	log.Info("Outfile Path: ", jsonFilePath)

	//Create new client object.
	clientObj := PumiceDBClient.PmdbClientNew(raftUuid, clientUuid)
	if clientObj == nil {
		return
	}
	//Start the client
	clientObj.Start()
	defer clientObj.Stop()

	//Wait for client to boot up.
	time.Sleep(5 * time.Second)

	fmt.Println("=================Format to pass write-read entries================")
	fmt.Println("Single write format ==> WriteOne#rncui#key#Val0#Val1#Val2#outfile_name")
	fmt.Println("Single read format ==> ReadOne#key#rncui#outfile_name")
	fmt.Println("Multiple write format ==> WriteMulti#csvfile.csv#outfile_name")
	fmt.Println("Multiple read format ==> ReadMulti#outfile_name")
	fmt.Println("Get Leader format ==> GetLeader#outfile_name")

	fmt.Print("Enter Operation(WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ GetLeader/ exit): ")

	//Get console input string
	var str string
	//Split the input string.
	input, _ := getInput(str)
	ops := input[0]

	//Create and Initialize map for write-read outfile.
	rwMap = make(map[string]map[string]string)
	//Create and Initialize the map for WriteMulti
	writeMultiMap = make(map[*AQLib.AirInfo]string)

	///Create temporary UUID
	tempUuid := uuid.New()
	tempUuidStr := tempUuid.String()



	var opIface Operation

	switch ops {
	case "WriteOne":
		opIface = &wrOne{
			op: opInfo{
				outfileUuid:  tempUuidStr,
				jsonFileName: input[6],
				key:          input[2],
				rncui:        input[1],
				inputStr:     input,
				cliObj:       clientObj,
			},
		}
	case "ReadOne":
		opIface = &rdOne{
			op: opInfo{
				outfileUuid:  tempUuidStr,
				jsonFileName: input[3],
				key:          input[1],
				rncui:        input[2],
				inputStr:     input,
				cliObj:       clientObj,
			},
		}
	case "WriteMulti":
		opIface = &wrMul{
			csvFile: input[1],
			op: opInfo{
				outfileUuid:  tempUuidStr,
				jsonFileName: input[2],
				inputStr:     input,
				cliObj:       clientObj,
			},
		}
	case "ReadMulti":
		opIface = &rdMul{
			op: opInfo{
				outfileUuid:  tempUuidStr,
				jsonFileName: input[1],
				inputStr:     input,
				cliObj:       clientObj,
			},
		}
	case "GetLeader":
		opIface = &getLeader{
			op: opInfo{
				jsonFileName: input[1],
				inputStr:     input,
				cliObj:       clientObj,
			},
		}
	default:
		fmt.Println("\nEnter valid Operation: WriteOne/ReadOne/WriteMulti/ReadMulti/GetLeader/exit")
	}

	prepErr := opIface.prepare()
	if prepErr != nil {
		log.Error("error to call prepare() method")
		os.Exit(0)
	}
	execErr := opIface.exec()
	if execErr != nil {
		log.Error("error to call exec() method")
		os.Exit(0)
	}
	compErr := opIface.complete()
	if compErr != nil {
		log.Error("error to call complete() method")
		os.Exit(0)
	}
}

//Positional Arguments.
func parseArgs() {

	flag.StringVar(&raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&clientUuid, "u", "NULL", "peer uuid")
	flag.StringVar(&jsonFilePath, "l", "/tmp/AQAppLog", "json outfile path")
	flag.StringVar(&cmd, "c", "NULL", "Command to pass")
	flag.Parse()
}

/*If log directory is not exist it creates directory.
  and if dir path is not passed then it will create
  log file in "/tmp/covidAppLog" path.
*/
func makeDirectoryIfNotExists() error {

	if _, err := os.Stat(jsonFilePath); os.IsNotExist(err) {

		return os.Mkdir(jsonFilePath, os.ModeDir|0755)
	}

	return nil
}


//Create logfile for client.
func initLogger() {

	var filename string = jsonFilePath + "/" + clientUuid + ".log"

	fmt.Println("logfile:", filename)

	//Create the log file if doesn't exist. And append to it if it already exists.i
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	Formatter := new(log.TextFormatter)

	//Set Timestamp format for logfile.
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)

	if err != nil {
		// Cannot open log file. Logging to stderr
		log.Error(err)
	} else {
		log.SetOutput(f)
	}
}


//read console input.
func getInput(keyText string) ([]string, error) {

	// convert CRLF to LF
	keyText = strings.Replace(cmd, "\n", "", -1)

	input := strings.Split(keyText, "#")
	for i := range input {
		input[i] = strings.TrimSpace(input[i])
	}

	if len(input) == 1 {
		return nil, errors.New("delimiter not found")
	}

	return input, nil
}

type opInfo struct {
	outfileUuid  string
	outfileName  string
	jsonFileName string
	key          string
	rncui        string
	inputStr     []string
	aqAppData    	 *AQLib.AirInfo
	cliObj       *PumiceDBClient.PmdbClientObj
}

/*
Structure for WriteOne Operation.
*/
type wrOne struct {
	op   opInfo
	Resp *AQLib.AirInfo
}

/*
Structure for ReadOne Operation.
*/
type rdOne struct {
	op opInfo
}

/*
 Structure for WriteMulti Operation.
*/
type wrMul struct {
	csvFile string
	op      opInfo
}

/*
Structure for ReadMulti Operation.
*/
type rdMul struct {
	multiRead []*AQLib.AirInfo
	rdRncui   []string
	op        opInfo
}

/*
 Structure for GetLeader Operation.
*/
type getLeader struct {
	op       opInfo
	pmdbInfo *PumiceDBCommon.PMDBInfo
}



//Interface for Operation.
// type Operation interface {
// 	prepare() error  //Fill Structure.
// 	exec() error     //Write-Read Operation.
// 	complete() error //Create Output Json File.
// }

//Get timestamp to dump into json outfile.
func getCurrentTime() string {

	//Get Timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	return timestamp
}


//Fill the Json data into map for WriteOne Operation.
func(aq *aqData)  fillWriteOne(wrOneObj *wrOne){
	//Get current time.
	timestamp := getCurrentTime()

	//fill the value into json structure.
	aq.Operation = wrOneObj.op.inputStr[0]
	aq.Timestamp = timestamp
	aq.Data = rwMap

	StatStr := strconv.Itoa(int(aq.Status))
	writeMp := map[string]string{
		"key":    wrOneObj.op.key,
		"Status": StatStr,
	}

	if wrOneObj.Resp != nil {
		writeMp["Location"] = wrOneObj.Resp.Location
		writeMp["Latitude"] = fmt.Sprintf("%f", wrOneObj.Resp.Latitude)
		writeMp["Longitude"] = fmt.Sprintf("%f", wrOneObj.Resp.Longitude)
		writeMp["Timestamp"] = wrOneObj.Resp.Timestamp.Format(time.RFC3339)

		// Add pollutant values
		for pollutant, value := range wrOneObj.Resp.Pollutants {
			writeMp[pollutant] = fmt.Sprintf("%f", value)
		}
	}
}

//Fill the Json data into map for WriteMulti Operation.
func (aq *aqData) fillWriteMulti(wm *wrMul) {

	//Get current time.
	timestamp := getCurrentTime()

	//fill the value into json structure.
	aq.Operation = wm.op.inputStr[0]
	aq.Timestamp = timestamp
	aq.Data = rwMap

	StatStr := strconv.Itoa(int(aq.Status))
	writeMp := map[string]string{
		"key":    wm.op.key,
		"Status": StatStr,
	}

	//fill write request rwMap into a map.
	fillDataToMap(writeMp, wm.op.rncui)
}

//Function to read-write data into map.
func fillDataToMap(mp map[string]string, rncui string) {

	//Fill rwMap into outer map.
	rwMap[rncui] = mp
}

//Fill the Json data into map for ReadOne Operation.
func (aq *aqData) fillReadOne(rdOneObj *rdOne) {

	//Get current time.
	timestamp := getCurrentTime()

	//Fill the value into Json structure.
	aq.Operation = rdOneObj.op.inputStr[0]
	aq.Timestamp = timestamp
	aq.Data = rwMap
}


//prepare function for writeone
func (wrObj *wrOne) prepare() error {
	var err error
	location := wrObj.op.inputStr[2]
	latStr := wrObj.op.inputStr[3]   
	lonStr := wrObj.op.inputStr[4]
	tsStr := wrObj.op.inputStr[5]  


	pollutantStr := ""
	if len(wrObj.op.inputStr) > 6 {
		pollutantStr = wrObj.op.inputStr[6]
	}

	lat, latErr := strconv.ParseFloat(latStr, 64)
	lon, lonErr := strconv.ParseFloat(lonStr, 64)
	if latErr != nil || lonErr != nil {
		return fmt.Errorf("invalid latitude or longitude")
	}

	ts, tsErr := time.Parse(time.RFC3339, tsStr)
	if tsErr != nil {
		return fmt.Errorf("invalid timestamp format, must be RFC3339")
	}
	
	pollutants := make(map[string]float64)
	if pollutantStr != "" {
		for _, kv := range strings.Split(pollutantStr, " ") {
			parts := strings.Split(kv, ":")
			if len(parts) == 2 {
				val, convErr := strconv.ParseFloat(parts[1], 64)
				if convErr == nil {
					pollutants[parts[0]] = val
				}
			}
		}
	}

	wrObj.op.aqAppData =  &AQLib.AirInfo{
		Location:   location,
		Latitude:   lat,
		Longitude:  lon,
		Timestamp:  ts,
		Pollutants: pollutants,
	}

	if wrObj.op.aqAppData == nil {
		err = fmt.Errorf("prepare() method failed for WriteOne")
	}

	return err;

}

/*
  exec() method for  WriteOne to write rwMap
  and dump to json file.
*/
func (wrObj *wrOne) exec() error{
	var errMsg error
	var wrData = &aqData{}
	var replySize int64
	response := make([]byte, 0)

	reqArgs := &PumiceDBClient.PmdbReqArgs{
		Rncui:       wrObj.op.rncui,
		ReqED:       wrObj.op.aqAppData,
		GetResponse: 1,
		ReplySize:   &replySize,
		Response:    &response,
	}

	//Perform write Operation.
	_, err := wrObj.op.cliObj.Put(reqArgs)
	if err != nil {
		errMsg = errors.New("exec() method failed for WriteOne.")
		wrData.Status = -1
		log.Info("Write key-value failed : ", err)
	} else {
		log.Info("Pmdb Write successful!")
		wrData.Status = 0
		errMsg = nil
	}

	if reqArgs.Response != nil && len(*reqArgs.Response) > 0 {
		var decoded AQLib.AirInfo
		buffer := bytes.NewBuffer(*reqArgs.Response)
		dec := gob.NewDecoder(buffer)
		err := dec.Decode(&decoded)
		if err != nil {
			log.Info("Failed to decode response buffer: ", err)
		} else {
			log.Info("Decoded response struct: ", decoded)
			wrObj.Resp = &decoded
		}
	}

	wrData.fillWriteOne(wrObj)

	//Dump structure into json.
	wrObj.op.outfileName = wrData.dumpIntoJson(wrObj.op.outfileUuid)

	return errMsg
}

//Method to dump CovidVaxData structure into json file.
func (aq *aqData) dumpIntoJson(outfileUuid string) string {

	//prepare path for temporary json file.
	tempOutfileName := jsonFilePath + "/" + outfileUuid + ".json"
	file, _ := json.MarshalIndent(aq, "", "\t")
	_ = ioutil.WriteFile(tempOutfileName, file, 0644)

	return tempOutfileName

}