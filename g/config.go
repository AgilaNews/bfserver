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
		Path    string `json:"path"`
		Level   string `json:"level"`
		Console bool   `json:"console"`
	} `json:"log"`
	Persist struct {
		Path    string `json:"path"`
		UseGzip bool   `json:"use_gzip"`
	} `json:"persist"`
	Rpc struct {
		BF struct {
			Addr string `json:"addr"`
		} `json:"bf"`
	} `json:"rpc"`
}

func load_conf() bool {
	var conf_file_path string
	var conf_file *os.File
	var err error

	// load config
	value := os.Getenv("RUN_ENV")
	switch value {
	case "rd":
		conf_file_path = "config.json.rd"
	case "sandbox":
		conf_file_path = "config.json.sandbox"
	default:
		value = "online"
		conf_file_path = "config.json.online"
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
