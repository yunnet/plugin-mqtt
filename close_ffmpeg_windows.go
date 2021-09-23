//go:build windows
// +build windows

package plugin_mqtt

import (
	"io/ioutil"
	"log"
	"os"
	"os/exec"
)

// Exist 检查文件或目录是否存在
// 如果由 filename 指定的文件或目录存在则返回 true，否则返回 false
func Exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

func CloseFFmpeg() {
	log.Println(":::::::::::::close FFmpeg.")
	if !Exist(C_PID_FILE) {
		log.Println("gonne.lock file not exists.")
		return
	}
	s, _ := ioutil.ReadFile(C_PID_FILE)
	pid := string(s)

	// gracefully kill pid, this closes the command window
	if err := exec.Command("taskkill.exe", "/f", "/t", "/pid", pid).Run(); err != nil {
		log.Printf("kill ffmepg error: %v", err)
	}

	err = os.Remove(C_PID_FILE)
	if err != nil {
		log.Println("cmd remove "+C_PID_FILE, err)
	}
}
