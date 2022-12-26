package pretty

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type LogTopic string

const (
	Client  LogTopic = "CLNT"
	Commit  LogTopic = "CMIT"
	Apply   LogTopic = "APLY"
	Drop    LogTopic = "DROP"
	Error   LogTopic = "ERRO"
	Info    LogTopic = "INFO"
	Leader  LogTopic = "LEAD"
	Log     LogTopic = "LOG1"
	Log2    LogTopic = "LOG2"
	Persist LogTopic = "PERS"
	Snap    LogTopic = "SNAP"
	Term    LogTopic = "TERM"
	Test    LogTopic = "TEST"
	Timer   LogTopic = "TIMR"
	Trace   LogTopic = "TRCE"
	Vote    LogTopic = "VOTE"
	Warn    LogTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic LogTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
