package redispipe

import (
	"fmt"
	"io"
	"log"
	"os/exec"
)

func init() {
	path, err := exec.LookPath("redis-cli")
	if err != nil {
		log.Fatalln("didn't find 'redis-cli' executable")
	} else {
		log.Println("Using 'redis-cli' executable: ", path)
	}
}

const crlf = "\r\n"

type Pipe struct {
	input io.WriteCloser
	cmd   *exec.Cmd
}

type Configuration struct {
	Host     string
	Port     string
	Password string
	Database string
}

func (r *Pipe) Send(parts ...string) {
	fmt.Fprint(r.input, "*", len(parts), crlf)
	for _, part := range parts {
		fmt.Fprint(r.input, "$", len(part), crlf, part, crlf)
	}
}

func Open(c Configuration) *Pipe {
	cmd := exec.Command("redis-cli", "--pipe", "-h", c.Host, "-p", c.Port, "-a", c.Password, "-d", c.Database)

	input, err := cmd.StdinPipe()

	if nil != err {
		log.Fatalf("Error obtaining `redis-cli` stdin: %s\n", err)
	}

	err = cmd.Start()
	if err != nil {
		log.Fatalf("Error starting `redis-cli`: %s\n", err)
	}

	go cmd.Wait()
	if err != nil {
		log.Fatalf("Error waiting for `redis-cli`: %s\n", err)
	}

	return &Pipe{
		cmd:   cmd,
		input: input,
	}
}

func (r *Pipe) Close() {
	r.input.Close()
	r.input = nil
}
