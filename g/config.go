package g

import (
	"encoding/json"
	"os"
)

var (
	Config *Configuration
)

type Configuration struct {
	Log struct {
		Path        string `json:"path"`
		Level       string `json:"level"`
		Console     bool   `json:"console"`
		MaxKeepDays int    `json:"max_keep_days"`
	} `json:"log"`
	Persist struct {
		Path             string `json:"path"`
		UseGzip          bool   `json:"use_gzip"`
		ForceDumpSeconds int    `json:"force_dump_seconds"`
	} `json:"persist"`
	Rpc struct {
		BF struct {
			Addr string `json:"addr"`
		} `json:"bf"`
	} `json:"rpc"`
	Gprof struct {
		Enabled bool   `json:"enabled"`
		Addr    string `json:"addr"`
	} `json:"gprof"`
}

func load_conf() bool {
	var conf_file_path string
	var conf_file *os.File
	var err error

	// load config
	value := os.Getenv("RUN_ENV")
	switch value {
	case "rd":
		conf_file_path = "config.rd.json"
	case "sandbox":
		conf_file_path = "config.sandbox.json"
	default:
		value = "online"
		conf_file_path = "config.online.json"
	}

	conf_file_path = "./conf/" + conf_file_path

	if conf_file, err = os.Open(conf_file_path); err != nil {
		panic("load conf file " + conf_file_path + "error")
	}

	decoder := json.NewDecoder(conf_file)

	Config = &Configuration{}
	if err = decoder.Decode(Config); err != nil {
		panic("json config error")
	}

	return true
}
