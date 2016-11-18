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

func init_log() {
	var level log4go.Level
	var ok bool
	if level, ok = level_map[Config.Log.Level]; !ok {
		level = log4go.INFO
	}

	if Config.Log.Console {
		log4go.Global.AddFilter("stdout", level, log4go.NewConsoleLogWriter())
	}

	fl := log4go.NewFileLogWriter(Config.Log.Path, false)
	fl.SetFormat("[%D %t][%L] %M")
	log4go.AddFilter("log", level, fl)
	log4go.Info("set log level to %v", level)
}
