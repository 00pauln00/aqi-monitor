package main

import (
	AQLib "github.com/00pauln00/aqi-monitor/lib"
	"fmt"
	"os"
	"flag"
	log "github.com/sirupsen/logrus"

)

var (
	raftUuid      string
	clientUuid    string
	jsonFilePath  string
	cmd           string
	rwMap         map[string]map[string]string
	keyRncuiMap   map[string]string
	writeMultiMap map[*AQLib.AirInfo]string
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
