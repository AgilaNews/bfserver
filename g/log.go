package g

import (
	"github.com/alecthomas/log4go"
)

var (
	level_map = map[string]log4go.Level{
		"DEBUG": log4go.DEBUG,
		"INFO":  log4go.INFO,
		"ERROR": log4go.ERROR,
	}
)

func init_log() bool {
	var level log4go.Level
	var ok bool
	if level, ok = level_map[Config.Log.Level]; !ok {
		level = log4go.INFO
	}

	//	format := "[%D %t][%L] %M"
	if Config.Log.Console {
		log4go.Global = make(log4go.Logger)
		fl := log4go.NewConsoleLogWriter()
		//		fl.SetFormat(format)
		log4go.AddFilter("stdout", level, fl)
	} else {
		log4go.Global = make(log4go.Logger)
		fl := log4go.NewFileLogWriter(Config.Log.Path, true)
		//		fl.SetFormat(format)
		fl.SetRotateDaily(true)
		fl.SetRotateMaxBackup(Config.Log.MaxKeepDays)

		log4go.AddFilter("log", level, fl)
		log4go.Trace("set log level to %v", level)
	}

	return true
}
