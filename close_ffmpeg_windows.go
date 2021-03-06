//go:build windows
// +build windows

package plugin_mqtt

import (
	. "github.com/Monibuca/utils/v3"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
)

func CloseFFmpeg() {
	if !Exist(C_PID_FILE) {
		log.Println(C_PID_FILE + " file not exists.")
		return
	}
	log.Println(":::::::::::::close FFmpeg.")
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
	status = 0
}
