/*
Author: Sriram Kaushik
worker process to mirror messages from GOOGLE pub/sub to a log file. Splunk forwarder will pick those logs and send to indexers.
*/

package consumers

import (
	"bufio"
	"cloud.google.com/go/pubsub"
	"encoding/json"
	"errors"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
	"gopkg.in/natefinch/lumberjack.v2"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

//This is the GCP struct. In future you can create similar for AWS or AZURE and implement methods for them.

type GCPInfo struct {
	Project      string       `json:"project"`
	Topic        string       `json:"topic,omitempty"`
	Subscription string       `json:"subscription"`
	Keyfile      string       `json:"keyfile"`
	Worker       WorkerInfo   `json:"workerinfo"`
	mu           sync.RWMutex //protect concurrent writes from different goroutines to avoid race conditions
	batch        []*pubsub.Message
	writer       *bufio.Writer
}

type WorkerInfo struct {
	Message_log_path    string        `json:"messagelogpath"`
	Worker_log_path     string        `json:"workerlogpath"`
	Batchsize           float32       `json:"batchsize"`
	Maxwaittime         time.Duration `json:"maxwaitmin"`
	Worker_logger_info  *log.Logger
	Worker_logger_error *log.Logger
}

func NewGCPclient(configfile string) (*GCPInfo, error) {
	//Check the cloud provider and create a struct accordingly
	gcpinfo := &GCPInfo{}

	//Read the config file and populate the json parameters.
	content, err := ioutil.ReadFile(configfile)
	if err != nil {
		return nil, errors.New("ERROR: Unable to read the json file")
	}

	//unmarshal json to struct object
	if err = json.Unmarshal(content, gcpinfo); err != nil {
		return nil, errors.New("ERROR: Unable to unmarshal config file contents. Check if valid json or if some parameter missing")
	}

	//lumberjack will compress and rotate the worker file. If Json is missing worker log path, then create a tmp path

	if gcpinfo.Worker.Worker_log_path == "" {
		gcpinfo.Worker.Worker_log_path = "/tmp/"
	}

	l := &lumberjack.Logger{
		Filename:   gcpinfo.Worker.Worker_log_path + "/" + configfile + ".log",
		MaxSize:    500,
		MaxBackups: 3,
		MaxAge:     18,
		Compress:   true,
	}

	//use this only one goroutine as making copies of logger will duplicate the interface and cause concurrency issues if multiple goroutines are used.
	gcpinfo.Worker.Worker_logger_error = log.New(l, "ERROR: ", log.Ldate|log.Ltime)
	gcpinfo.Worker.Worker_logger_info = log.New(l, "INFO: ", log.Ldate|log.Ltime)

	//create a message log file
	if err = CreateMessageLogFiles(gcpinfo.Worker.Message_log_path, gcpinfo.Subscription); err != nil {
		return nil, errors.New("ERROR: Unable to create message log files. Check permissions")
	}

	//define the batch

	gcpinfo.batch = make([]*pubsub.Message, 0, int(gcpinfo.Worker.Batchsize))

	//define default batch size

	if gcpinfo.Worker.Batchsize == 0 {
		gcpinfo.Worker.Batchsize = 3
	}

	if gcpinfo.Worker.Maxwaittime == 0 {
		//set default to 10 min
		gcpinfo.Worker.Maxwaittime = time.Duration(10)
	}

	return gcpinfo, nil
}

//consume messages from the pubsub queue.

func (gcpinfo *GCPInfo) Flush() {

	for _, msg := range gcpinfo.batch {
		_, err := gcpinfo.writer.WriteString(string(msg.Data) + "\n")
		if err != nil {
			msg.Nack()
		} else {
			msg.Ack()
		}
	}

	gcpinfo.writer.Flush()
	//empty the batch
	gcpinfo.batch = make([]*pubsub.Message, 0, int(gcpinfo.Worker.Batchsize))

}

func (gcpinfo *GCPInfo) Consume() error {

	gcpinfo.Worker.Worker_logger_info.Println("Starting Receiver")
	ctx := context.Background()

	//Create a new consumer client. Pass the credentials via a file.

	client, err := pubsub.NewClient(ctx, gcpinfo.Project, option.WithCredentialsFile(gcpinfo.Keyfile))

	if err != nil {
		return errors.New("ERROR: Unable to create a pub/sub client. Is the credentials file exported?")
	}

	Subscription := client.Subscription(gcpinfo.Subscription)

	//Open the target file.

	file, err := os.OpenFile(gcpinfo.Worker.Message_log_path+"/"+gcpinfo.Subscription+".log", os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return errors.New("Unable to Open events output log file")
	}
	defer file.Close()

	gcpinfo.writer = bufio.NewWriter(file)

	//A context to stop receive after a certain time. We will restart the worker eventaully. This is done to keep it consistent with Azure Worker. May not be needed for gcp
	cctx, cancel := context.WithTimeout(context.Background(), gcpinfo.Worker.Maxwaittime*time.Minute)
	defer cancel()
	err = Subscription.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		gcpinfo.mu.Lock()
		gcpinfo.batch = append(gcpinfo.batch, msg)

		if len(gcpinfo.batch) > int(gcpinfo.Worker.Batchsize) {
			gcpinfo.Flush()
		}
		gcpinfo.mu.Unlock()
	})
	if err != nil {
		return errors.New("ERROR:error to receive messages, is the pub/sub up and does the user logmonitor has view permissions?")
	}
	gcpinfo.Worker.Worker_logger_info.Println("Closing GCP Receiver")
	return nil
}

//create a message log file
func CreateMessageLogFiles(logpath string, filename string) error {
	//Create the target log file.
	err := os.MkdirAll(logpath, 0744)
	if err != nil {
		return errors.New("Error: Unable to create target log files path")
	}

	//create a file with topic name as the filename
	if _, err := os.Stat(logpath + "/" + filename + ".log"); os.IsNotExist(err) {
		file, err := os.Create(logpath + "/" + filename + ".log")
		defer file.Close()
		if err != nil {
			return errors.New("Error: Unable to create target log file")
		}
	}
	return nil
}
