package plugin_mqtt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	. "github.com/Monibuca/engine/v3"
	. "github.com/Monibuca/utils/v3"
	"github.com/Monibuca/utils/v3/codec"
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
 3、切换数据源指令：(1)摄像头数据流 (2)算法数据流
 4、接收到停止推流指令后，停止ffmpeg

【开始推流】
指令：{"command": "start"}

【停止推流】
指令：{"command": "stop"}

【切换推流】
指令：{"command": "switch", "enabled": false}

【请求录像列表】
指令：{"command": "record", "begin": "2021-10-11 00:00:00", "end": "2021-10-11 23:59:59"}

【请求上传文件】
指令：{"command": "upload", "file": "live/hw/2021-10-09/15-38-05.mp4"}

*/

var config struct {
	Host      string
	Username  string
	Password  string
	ClientId  string
	SourceUrl string
	AlgUrl    string // 算法源
	TargetUrl string
	UploadUrl string
	SavePath  string
}

var (
	client    libmqtt.Client
	options   []libmqtt.Option
	switchUrl string
	err       error
	topic     string
	gc        gcache.Cache
	LOC, _    = time.LoadLocation("Asia/Shanghai")
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

const C_PID_FILE = "pull.lock"

func init() {
	InstallPlugin(&PluginConfig{
		Name:   "MQTT",
		Config: &config,
		Run:    run,
	})
}

func run() {
	c, cancel := context.WithCancel(Ctx)
	defer cancel()

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

	switchUrl = config.SourceUrl
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
	err = client.ConnectServer(config.Host, options...)
	if err != nil {
		log.Printf("connect to server failed: %v", err)
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
			{Name: topic + "/#", Qos: libmqtt.Qos0},
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
	log.Printf("recv [%v] message: %v", topic, msg)

	commandNode := gjson.Get(msg, "command")

	log.Println(commandNode.Value())

	switch commandNode.String() {
	case "start":
		openFFmpeg(switchUrl)
	case "stop":
		CloseFFmpeg()
	case "switch":
		{
			enabled := gjson.Get(msg, "enabled").Bool()
			switchFFmpeg(enabled)
		}
	case "record":
		getRecordFiles(client, msg)

	case "upload":
		{
			file := gjson.Get(msg, "file").Str
			uploadFile(file)
		}
	default:
		log.Printf("command error %s", commandNode.String())
	}
}

//指令：{"command": "record", "begin": "2021-10-11 00:00:00", "end": "2021-10-11 23:59:59"}
func getRecordFiles(client libmqtt.Client, data string) {
	beginNode := gjson.Get(data, "begin")
	beginStr := beginNode.Str

	endNode := gjson.Get(data, "end")
	endStr := endNode.Str

	var begin, end time.Time
	begin, err = strToDatetime(beginStr)
	if err != nil {
		payload := "开始日期错误 " + err.Error()
		publish(client, payload)
		return
	}

	end, err = strToDatetime(endStr)
	if err != nil {
		payload := "开始日期错误 " + err.Error()
		publish(client, payload)
		return
	}

	files, err := getRecords(&begin, &end)
	if err != nil {
		payload := "获取文件列表错误 " + err.Error()
		publish(client, payload)
		return
	}

	res, err := json.Marshal(files)
	if err != nil {
		payload := "获取文件列表错误 " + err.Error()
		publish(client, payload)
		return
	}
	publish(client, string(res))
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
				log.Printf("append file" + f.Url)

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

//指令：{"command": "upload", "file": "live/hw/2021-10-09/15-38-05.mp4"}
func uploadFile(file string) {
	filePath := filepath.Join(config.SavePath, file)

	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)

	//关键的一步操作
	fileWriter, err := bodyWriter.CreateFormFile("uploadfile", filePath)
	if err != nil {
		fmt.Println("error writing to buffer")
		return
	}

	//打开文件句柄操作
	fh, err := os.Open(filePath)
	if err != nil {
		fmt.Println("error opening file")
		return
	}
	defer fh.Close()

	//iocopy
	_, err = io.Copy(fileWriter, fh)
	if err != nil {
		return
	}

	contentType := bodyWriter.FormDataContentType()
	bodyWriter.Close()

	//POST
	resp, err := http.Post(config.UploadUrl, contentType, bodyBuf)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	fmt.Println(resp.Status)
	fmt.Println(string(respBody))
	return
}

func publish(client libmqtt.Client, payload string) {
	client.Publish([]*libmqtt.PublishPacket{
		{TopicName: topic, Payload: []byte(payload), Qos: libmqtt.Qos0},
	}...)
}

func switchFFmpeg(enabled bool) {
	CloseFFmpeg()

	if enabled {
		switchUrl = config.SourceUrl
	} else {
		switchUrl = config.AlgUrl
	}
	openFFmpeg(switchUrl)
}

func openFFmpeg(url string) {
	CloseFFmpeg()

	if url == "" {
		log.Println("url is null")
		return
	}
	if Exist(C_PID_FILE) {
		log.Println("ffmpeg already run.")
		return
	}

	cmd := exec.Command("ffmpeg", "-rtsp_transport", "tcp", "-i", url, "-vcodec", "copy", "-acodec", "aac", "-ar", "44100", "-f", "flv", config.TargetUrl)
	log.Println(" => " + cmd.String())
	err := cmd.Start()
	if err != nil {
		log.Println("cmd start", err)
	}

	pid := cmd.Process.Pid
	log.Println("Pid ", pid)

	err = ioutil.WriteFile(C_PID_FILE, []byte(fmt.Sprintf("%d", pid)), 0666)
	if err != nil {
		log.Println("cmd write pid file fail. ", err)
	}

	err = cmd.Wait()
	if err != nil {
		log.Println("cmd wait", err)
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
