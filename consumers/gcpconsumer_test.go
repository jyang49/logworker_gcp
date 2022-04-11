package consumers

import (
	"os"
	"testing"
)

var subscription string = "AllEvents-Sriram-Test"
var topic string = "AllEvents"
var project string = "box-all-events-pub-sub"
var gcp_log_path string = "/tmp/"
var workerlog string = "/tmp/workerlogtest.log"
var file *os.File

func TestNewGCPclient(t *testing.T) {

	if _, err := os.Stat(workerlog); os.IsNotExist(err) {
		file, err := os.Create(workerlog)
		defer file.Close()
		if err != nil {
			t.Error("Not able to create file")
		}
	}

	client, err := NewGCPclient(subscription, project, topic, gcp_log_path, file)

	if err != nil {
		t.Error("Error creating client")

	} else {
		t.Log("Client created successfully")
	}
}
