package plugin_mqtt

import (
	"fmt"
	. "github.com/Monibuca/engine/v3"
	. "github.com/Monibuca/utils/v3"
	"github.com/goiiot/libmqtt"
	"github.com/tidwall/gjson"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"time"
)


/**
功能：
 1、订阅一个主题
 2、接收到开始推流指令后，执行ffmpeg
 3、切换数据源指令：(1)摄像头数据流 (2)算法数据流
 4、接收到停止推流指令后，停止ffmpeg
*/

var config struct {
 Host string
 Username string
 Password string
 ClientId string
 SourceUrl string
 TargetUrl string
}

var(
	client libmqtt.Client
	options []libmqtt.Option
	switchUrl string
	err    error
)

const C_PID_FILE = "gonne.lock"

func init() {
	InstallPlugin(&PluginConfig{
		Name:   "MQTT",
		Config: &config,
		Run:    run,
	})
}

func run() {
	defer log.Println(config.Host + " mqtt start!")

	switchUrl = config.SourceUrl

	client, err = libmqtt.NewClient(
		// try MQTT 5.0 and fallback to MQTT 3.1.1
		libmqtt.WithVersion(libmqtt.V311, true),

		// enable keepalive (10s interval) with 20% tolerance
		libmqtt.WithKeepalive(10, 1.2),

		// enable auto reconnect and set backoff strategy
		libmqtt.WithAutoReconnect(true),
		libmqtt.WithBackoffStrategy(time.Second, 5*time.Second, 1.2),

		// use RegexRouter for topic routing if not specified
		// will use TextRouter, which will match full text
		libmqtt.WithRouter(libmqtt.NewRegexRouter()),

		libmqtt.WithConnHandleFunc(connHandler),
		libmqtt.WithNetHandleFunc(netHandler),
		libmqtt.WithSubHandleFunc(subHandler),
		libmqtt.WithUnsubHandleFunc(unSubHandler),
		libmqtt.WithPubHandleFunc(pubHandler),
		libmqtt.WithPersistHandleFunc(persistHandler),
	)

	if err != nil {
		// handle client creation error
		panic("hmm, how could it failed")
	}

	// handle every subscribed message (just for example)
	client.HandleTopic(".*", func(client libmqtt.Client, topic string, qos libmqtt.QosLevel, msg []byte) {
		handleData(topic, string(msg))
	})

	options = append(options, libmqtt.WithConnPacket(libmqtt.ConnPacket{
		Username: config.Username,
		Password: config.Password,
		ClientID: config.ClientId,
	}))

	// connect tcp server
	err = client.ConnectServer(config.Host, options...)
	if err != nil{
		log.Printf("connect to server failed: %v", err)
	}

	client.Wait()
}

func connHandler(client libmqtt.Client, server string, code byte, err error) {
	if err != nil {
		log.Printf("connect to server [%v] failed: %v", server, err)
		return
	}

	if code != libmqtt.CodeSuccess {
		log.Printf("connect to server [%v] failed with server code [%v]", server, code)
		return
	}

	// connected
	go func() {
		// subscribe to some topics
		client.Subscribe([]*libmqtt.Topic{
			{Name: "/device/" +config.ClientId + "/#", Qos: libmqtt.Qos0},
		}...)
	}()
}

func netHandler(client libmqtt.Client, server string, err error) {
	if err != nil {
		log.Printf("error happened to connection to server [%v]: %v", server, err)
	}
}

func persistHandler(client libmqtt.Client, packet libmqtt.Packet, err error) {
	if err != nil {
		log.Printf("session persist error: %v", err)
	}
}

func subHandler(client libmqtt.Client, topics []*libmqtt.Topic, err error) {
	if err != nil {
		for _, t := range topics {
			log.Printf("subscribe to topic [%v] failed: %v", t.Name, err)
		}
	} else {
		for _, t := range topics {
			log.Printf("subscribe to topic [%v] success: %v", t.Name, err)
		}
	}
}

func unSubHandler(client libmqtt.Client, topic []string, err error) {
	if err != nil {
		// handle unsubscribe failure
		for _, t := range topic {
			log.Printf("unsubscribe to topic [%v] failed: %v", t, err)
		}
	} else {
		for _, t := range topic {
			log.Printf("unsubscribe to topic [%v] failed: %v", t, err)
		}
	}
}

func pubHandler(client libmqtt.Client, topic string, err error) {
	if err != nil {
		log.Printf("publish packet to topic [%v] failed: %v", topic, err)
	} else {
		log.Printf("publish packet to topic [%v] success: %v", topic, err)
	}
}

func handleData(topic, msg string) {
	log.Printf("recv [%v] message: %v", topic, string(msg))
	rootJson := gjson.Parse(msg)
	commandNode := rootJson.Get("command")
	log.Println(commandNode.Value())

	switch commandNode.String() {
	case "start": openFFmpeg(switchUrl)
	case "stop": closeFFmpeg()
	case "switch": {
		enabled := rootJson.Get("enabled")
		switchFFmpeg(enabled.Bool())
	}
	default:
		log.Printf("command error %s", commandNode.String())
	}
}

func switchFFmpeg(enabled bool) {
	closeFFmpeg()

	if enabled{
		switchUrl = config.SourceUrl
	}else{
		switchUrl = "rtsp://127.0.0.1/live/hw"
	}
	openFFmpeg(switchUrl)
}

func openFFmpeg(url string) {
	if url == ""{
		log.Println("url is null")
		return
	}
	if Exist(C_PID_FILE){
		log.Println("ffmpeg already run.")
		return
	}

	cmd := exec.Command("ffmpeg", "-rtsp_transport", "tcp", "-i", url, "-vcodec", "copy", "-acodec", "aac", "-ar", "44100", "-f", "flv", config.TargetUrl)
	log.Println(" => " + cmd.String())
	err := cmd.Start()
	if err != nil{
		log.Println("cmd start", err)
	}

	pid := cmd.Process.Pid
	log.Println("Pid ", pid)

	err = ioutil.WriteFile(C_PID_FILE, []byte(fmt.Sprintf("%d", pid)), 0666)
	if err != nil{
		log.Println("cmd write pid file fail. ", err)
	}

	err = cmd.Wait()
	if err != nil{
		log.Println("cmd wait", err)
	}
}

func closeFFmpeg(){
	if !Exist(C_PID_FILE){
		log.Println("gonne.lock file not exists.")
		return
	}
	pid, _ := ioutil.ReadFile(C_PID_FILE)

	cmd := exec.Command("kill", "-9", string(pid))
	log.Println(" => " + cmd.String())

	err := cmd.Start()
	if err != nil{
		log.Println("cmd start", err)
	}
	err = cmd.Wait()
	if err != nil{
		log.Println("cmd wait", err)
	}

	err = os.Remove(C_PID_FILE)
	if err != nil{
		log.Println("cmd remove " + C_PID_FILE, err)
	}
}
