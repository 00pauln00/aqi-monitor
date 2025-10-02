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
	covidData    *AQLib.AirInfo
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