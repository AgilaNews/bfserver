package g

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
		Path string `json:"path"`
	} `json:"persist"`
	Rpc struct {
		BF struct {
			Addr string `json:"addr"`
		} `json:"bf"`
	} `json:"rpc"`
}
