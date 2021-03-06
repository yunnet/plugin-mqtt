package plugin_mqtt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	. "github.com/Monibuca/engine/v3"
	. "github.com/Monibuca/utils/v3"
	"github.com/Monibuca/utils/v3/codec"
	. "github.com/logrusorgru/aurora"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/bluele/gcache"
	"github.com/goiiot/libmqtt"
	"github.com/tidwall/gjson"
	"io/ioutil"
	"log"
	"os/exec"
	"time"
)

/**
功能：
 1、订阅一个主题
 2、接收到开始推流指令后，执行ffmpeg
 3、切换数据源指令：(1)摄像头数据流 (2)算法数据流q:
 4、接收到停止推流指令后，停止ffmpeg

指令主题
topic: /device/xxxxx/cmd

【开始推流】
指令：{"command": "start","requestId":"5627a9fb-f987-4d38-a5d7-e52ca124a42e"}
回复：
topic: /device/xxxxx/record/push
content: {"command": "pushStream", "requestId":"", "result": "ok", "type":1}

【停止推流】
指令：{"command": "stop","requestId":"5627a9fb-f987-4d38-a5d7-e52ca124a42e"}

【切换推流】
指令：{"command": "switch", ,"requestId":"5627a9fb-f987-4d38-a5d7-e52ca124a42e", "enabled": false}

【请求录像列表】
指令：{"command": "recordList","requestId":"5627a9fb-f987-4d38-a5d7-e52ca124a42e","begin": "2021-10-11 00:00:00", "end": "2021-10-11 23:59:59"}
回复：
topic: /device/xxxxx/record/list
content:
{"deviceId":"xxxxx","requestId":"5627a9fb-f987-4d38-a5d7-e52ca124a42e","command":"recordList","data":[{"url":"live/hw/2021-10-25/08-31-18.mp4","size":187366179,"timestamp":1635121878,"duration":301},{"url":"live/hw/2021-10-25/08-36-19.mp4","size":225607892,"timestamp":1635122179,"duration":302},{"url":"live/hw/2021-10-25/08-41-21.mp4","size":202025529,"timestamp":1635122481,"duration":302}]}

【请求上传文件】
指令：{"command": "upload","requestId":"5627a9fb-f987-4d38-a5d7-e52ca124a42e", "file": "live/hw/2021-10-09/15-38-05.mp4"}

注：发现emqx会保留最近会话，更改主题时，请清除emqx中的会话记录
*/

var config struct {
	Path     string //firefly配置文件路径
	Enable   bool
	Host     string //MQTT 地址
	Username string //MQTT 用户名
	Password string //MQTT 密码
	ClientId string //MQTT ID
	SavePath string //录像视频路径
}

var (
	client    libmqtt.Client
	options   []libmqtt.Option
	switchUrl string
	err       error
	topic     string
	gc        gcache.Cache
	LOC, _    = time.LoadLocation("Asia/Shanghai")
	status    int    //0 未运行, 1 正在运行
	sourceUrl string //原始视频源
	algUrl    string //算法视频源
	targetUrl string //视频推送地址
	uploadUrl string //上传视频地址
)

type RecFileInfo struct {
	Url       string `json:"url"`
	Size      int64  `json:"size"`
	Timestamp int64  `json:"timestamp"`
	Duration  uint32 `json:"duration"`
}

type FileWr interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
}

func (c *RecFileInfo) String() string {
	res, _ := json.Marshal(c)
	return string(res)
}

const (
	C_PID_FILE  = "ff.lock"
	C_JSON_FILE = "firefly.json"
)

func init() {
	InstallPlugin(&PluginConfig{
		Name:   "MQTT",
		Config: &config,
		Run:    run,
	})
}

func initBoxConfig() {
	filePath := filepath.Join(config.Path, C_JSON_FILE)
	content, err := readFile(filePath)
	if nil != err {
		log.Printf("read firefly.json error " + err.Error())
		return
	}
	targetUrl = gjson.Get(content, "mqtt.target").Str
	Print(Green("::::::mqtt.target: "), BrightBlue(targetUrl))

	sourceUrl = gjson.Get(content, "boxinfo.rtsp").Str
	Print(Green("::::::mqtt.source: "), BrightBlue(sourceUrl))

	algUrl = gjson.Get(content, "mqtt.target").Str
	Print(Green("::::::mqtt.alg: "), BrightBlue(algUrl))

	uploadUrl = gjson.Get(content, "mqtt.upload").Str
	Print(Green("::::::mqtt.upload: "), BrightBlue(uploadUrl))
}

func run() {
	c, cancel := context.WithCancel(Ctx)
	defer cancel()

	initBoxConfig()

	gc = gcache.New(100).LRU().Build()

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("收到信号，父context的协程退出,time=", time.Now().Unix())
				destroy()
				return
			default:
				time.Sleep(1 * time.Second)
			}
		}
	}(c)

	status = 0
	switchUrl = sourceUrl
	topic = "/device/" + config.ClientId

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
		handleData(client, topic, string(msg))
	})

	options = append(options, libmqtt.WithConnPacket(libmqtt.ConnPacket{
		Username: config.Username,
		Password: config.Password,
		ClientID: config.ClientId,
	}))

	// connect tcp server
	if config.Enable {
		err = client.ConnectServer(config.Host, options...)
		if err != nil {
			log.Printf("connect to server failed: %v", err)
		}
	}
	client.Wait()
}

func destroy() {
	CloseFFmpeg()
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
			{Name: topic + "/cmd/#", Qos: libmqtt.Qos0},
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

func handleData(client libmqtt.Client, topic, msg string) {
	cmd := gjson.Get(msg, "command").Str
	log.Printf("recv command [%s]: %s\n", topic, cmd)

	switch cmd {
	case "start":
		{
			requestId := gjson.Get(msg, "requestId").Str
			openFFmpeg(client, requestId, 0)
		}

	case "stop":
		CloseFFmpeg()

	case "switch":
		switchFFmpeg(client, msg)

	case "recordList":
		getRecordFiles(client, msg)

	case "upload":
		uploadFile(msg)

	default:
		log.Printf("command error %s", cmd)
	}
}

//指令：{"requestId":"5627a9fb-f987-4d38-a5d7-e52ca124a42e","command": "recordList", "begin": "2021-10-11 00:00:00", "end": "2021-10-11 23:59:59"}
func getRecordFiles(client libmqtt.Client, data string) {
	requestId := gjson.Get(data, "requestId").Str
	command := gjson.Get(data, "command").Str
	beginStr := gjson.Get(data, "begin").Str
	endStr := gjson.Get(data, "end").Str

	t := topic + "/record/list"

	var begin, end time.Time
	begin, err = strToDatetime(beginStr)
	if err != nil {
		payload := "开始时间错误 " + err.Error()
		publish(client, t, payload)
		return
	}

	end, err = strToDatetime(endStr)
	if err != nil {
		payload := "结束时间错误 " + err.Error()
		publish(client, t, payload)
		return
	}

	files, err := getRecords(&begin, &end)
	if err != nil {
		payload := "获取文件列表错误 " + err.Error()
		publish(client, t, payload)
		return
	}

	var result = make(map[string]interface{}, 4)
	result["requestId"] = requestId
	result["command"] = command
	result["deviceId"] = config.ClientId
	result["data"] = files

	res, err := json.Marshal(result)
	if err != nil {
		payload := "获取文件列表错误 " + err.Error()
		publish(client, t, payload)
		return
	}

	publish(client, t, string(res))
}

func getRecords(begin, end *time.Time) (files []*RecFileInfo, err error) {
	walkFunc := func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		ext := strings.ToLower(filepath.Ext(filePath))
		if ext == ".flv" || ext == ".mp4" {
			t := time.Now()
			if f, err := getRecFileRange(filePath, begin, end); err == nil && f != nil {
				files = append(files, f)
			}
			spend := time.Since(t).Seconds()
			if spend > 10 {
				log.Printf("spend: %fms", spend)
			}
		}
		return nil
	}

	err = filepath.Walk(config.SavePath, walkFunc)
	return
}

//指令：{"command": "upload", "requestId":"5627a9fb-f987-4d38-a5d7-e52ca124a42e", "file": "live/hw/2021-10-09/15-38-05.mp4"}
func uploadFile(msg string) {
	requestId := gjson.Get(msg, "requestId").Str
	file := gjson.Get(msg, "file").Str

	filePath := filepath.Join(config.SavePath, file)

	ext := strings.ToLower(filepath.Ext(filePath))
	if ext != ".mp4" {
		fmt.Println("文件格式不正确: " + filePath)
		return
	}

	r, w := io.Pipe()
	m := multipart.NewWriter(w)
	go func() {
		defer w.Close()
		defer m.Close()

		part, err := m.CreateFormFile("video", filePath)
		if err != nil {
			return
		}
		f, err := os.Open(filePath)
		if err != nil {
			return
		}
		defer f.Close()

		if _, err := io.Copy(part, f); err != nil {
			return
		}

		info := fmt.Sprintf(`{"requestId": "%s", "deviceId": "%s", "file": "%s"}`, requestId, config.ClientId, file)
		log.Println("update file info: " + info)

		_ = m.WriteField("info", info)
	}()

	//POST
	resp, err := http.Post(uploadUrl, m.FormDataContentType(), r)
	if err != nil {
		log.Println(err.Error())
		return
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err.Error())
		return
	}
	fmt.Printf("[%s] %s \n", resp.Status, string(respBody))
	return
}

func publish(client libmqtt.Client, topic, payload string) {
	client.Publish([]*libmqtt.PublishPacket{
		{TopicName: topic, Payload: []byte(payload), Qos: libmqtt.Qos0},
	}...)
}

func switchFFmpeg(client libmqtt.Client, msg string) {
	CloseFFmpeg()

	requestId := gjson.Get(msg, "requestId").Str
	enabled := gjson.Get(msg, "enabled").Bool()
	var kind = 0
	if enabled {
		switchUrl = sourceUrl
		kind = 0
	} else {
		switchUrl = algUrl
		kind = 1
	}

	openFFmpeg(client, requestId, kind)
}

func openFFmpeg(client libmqtt.Client, requestId string, kind int) {
	t := topic + "/record/push"
	var replyMap = make(map[string]interface{}, 3)
	replyMap["requestId"] = requestId
	replyMap["replyMap"] = "ok"
	replyMap["command"] = "pushStream"
	replyMap["type"] = kind

	if status == 1 {
		res, err := json.Marshal(replyMap)
		if err != nil {
			payload := "push stream fail. " + err.Error()
			publish(client, t, payload)
			return
		}
		publish(client, t, string(res))

		log.Println("ffmpeg already running.")
		return
	}

	CloseFFmpeg()

	url := switchUrl
	if url == "" {
		log.Println("url is null")
		return
	}
	if Exist(C_PID_FILE) {
		log.Println("ffmpeg already run.")
		return
	}

	tarUrl := targetUrl + "/" + config.ClientId
	cmd := exec.Command("ffmpeg", "-rtsp_transport", "tcp", "-i", url, "-vcodec", "copy", "-acodec", "aac", "-ar", "44100", "-f", "flv", tarUrl)
	log.Println(" => " + cmd.String())

	err := cmd.Start()
	if err != nil {
		log.Println("cmd start error ", err)
		return
	}

	pid := cmd.Process.Pid
	log.Println("Pid ", pid)

	err = ioutil.WriteFile(C_PID_FILE, []byte(fmt.Sprintf("%d", pid)), 0666)
	if err != nil {
		log.Println("cmd write pid file fail. ", err)
		return
	}

	res, err := json.Marshal(replyMap)
	if err != nil {
		payload := "push stream fail. " + err.Error()
		publish(client, t, payload)
		return
	}
	publish(client, t, string(res))

	status = 1

	err = cmd.Wait()
	if err != nil {
		log.Println("cmd wait", err)
		return
	}
}

func getRecFileRange(dstPath string, begin, end *time.Time) (recFile *RecFileInfo, err error) {
	p := strings.TrimPrefix(dstPath, config.SavePath)
	p = strings.ReplaceAll(p, "\\", "/")

	_, file := path.Split(p)
	if file[0:1] == "." {
		return nil, errors.New("temp file " + file)
	}

	ext := strings.ToLower(path.Ext(file))
	var timestamp time.Time
	if ext == ".flv" {
		timestamp = getFlvTimestamp(p)
	} else if ext == ".mp4" {
		timestamp = getMp4Timestamp(p)
	} else {
		return nil, errors.New("file types do not match")
	}

	if begin.Before(timestamp) && end.After(timestamp) {
		value, err := gc.Get(timestamp)
		if err != nil {
			var f *os.File
			f, err = os.Open(dstPath)
			if err != nil {
				return nil, err
			}
			defer f.Close()

			fileInfo, err := f.Stat()
			if err != nil {
				return nil, err
			}

			if ext == ".flv" {
				recFile = &RecFileInfo{
					Url:       strings.TrimPrefix(p, "/"),
					Size:      fileInfo.Size(),
					Timestamp: timestamp.Unix(),
					Duration:  getDuration(f),
				}
			} else if ext == ".mp4" {
				recFile = &RecFileInfo{
					Url:       strings.TrimPrefix(p, "/"),
					Size:      fileInfo.Size(),
					Timestamp: timestamp.Unix(),
					Duration:  GetMP4Duration(f),
				}
			}
			gc.SetWithExpire(timestamp, recFile, time.Hour*12)
		} else {
			recFile, _ = (value).(*RecFileInfo)
		}
		return recFile, nil
	}
	return nil, errors.New("not found record file")
}

func getDuration(file FileWr) uint32 {
	_, err := file.Seek(-4, io.SeekEnd)
	if err == nil {
		var tagSize uint32
		if tagSize, err = ReadByteToUint32(file, true); err == nil {
			_, err = file.Seek(-int64(tagSize)-4, io.SeekEnd)
			if err == nil {
				_, timestamp, _, err := codec.ReadFLVTag(file)
				if err == nil {
					return timestamp
				}
			}
		}
	}
	return 0
}

func strToDatetime(t string) (time.Time, error) {
	return time.ParseInLocation("2006-01-02 15:04:05", t, LOC)
}

//live/hk/2021/09/24/143046.flv
func getFlvTimestamp(path string) time.Time {
	return getTimestamp(path, 21, 4, "2006/01/02/150405")
}

//live/hw/2021-09-27/18-07-25.mp4
func getMp4Timestamp(path string) time.Time {
	return getTimestamp(path, 23, 4, "2006-01-02/15-04-05")
}

func getTimestamp(path string, start, end int, layout string) time.Time {
	s := path[len(path)-start : len(path)-end]
	l, err := time.LoadLocation("Local")
	if err != nil {
		return time.Unix(0, 0)
	}
	tmp, err := time.ParseInLocation(layout, s, l)
	if err != nil {
		return time.Unix(0, 0)
	}
	return tmp
}

func readFile(filePath string) (content string, err error) {
	res, err := ioutil.ReadFile(filePath)
	if nil != err {
		return "", err
	}
	return string(res), nil
}
