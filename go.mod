module github.com/yunnet/plugin-mqtt

go 1.17

replace (
	github.com/Monibuca/engine/v3 v3.3.1 => ../engine
)

require (
	github.com/Monibuca/engine/v3 v3.3.1
	github.com/goiiot/libmqtt v0.9.6
	github.com/tidwall/gjson v1.8.1
)
