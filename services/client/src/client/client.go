package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const ConnectionAttemptsMax = 3
const ConnectionAttempsDelayMs = 200

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range ConnectionAttemptsMax {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(ConnectionAttempsDelayMs * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) closeConnection(err *error) {
	closeErr := client.conn.Close()
	if closeErr != nil {
		logger.Error("close-connection", logger.Fail, "error", closeErr)
		*err = errors.Join(*err, closeErr)
		return
	}
	return
}

func (client *Client) closeFile(file *os.File, err *error) {
	closeErr := file.Close()
	if closeErr != nil {
		logger.Error("close-input-file", logger.Fail, "path", file.Name(), "error", closeErr)
		*err = errors.Join(*err, closeErr)
		return
	}
	return
}

func (client *Client) flushFile(writer *bufio.Writer, err *error) {
	flushErr := writer.Flush()
	if flushErr != nil {
		logger.Warn("flush-file", logger.Fail, "error", flushErr)
		*err = errors.Join(*err, flushErr)
	}
}

func (client *Client) sendAndRecvResponse(message []byte, messageArgs []any) ([]byte, error) {
	if err := safe_socket.SendAll(client.conn, message); err != nil {
		logger.Error("send-message", logger.Fail, messageArgs...)
		return nil, err
	}
	responseBuffer, err := safe_socket.RecvAll(client.conn, len(message))
	if err != nil {
		logger.Error("recv-response", logger.Fail, messageArgs...)
		return nil, err
	}
	return responseBuffer, nil
}

func (client *Client) Run() (err error) {
	defer client.closeConnection(&err)

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "path", client.config.InputFile, "error", err)
		return err
	}
	defer client.closeFile(inputFile, &err)

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		logger.Error("create-output-file", logger.Fail, "path", client.config.OutputFile, "err", err)
		return err
	}
	defer client.closeFile(outputFile, &err)

	scanner := bufio.NewScanner(inputFile)
	writer := bufio.NewWriter(outputFile)
	defer client.flushFile(writer, &err)

	for messageId := 0; scanner.Scan(); messageId++ {
		logger.Info("send-input-file", logger.InProgress)
		clientMessage := scanner.Text()
		messageArgs := []any{"message", clientMessage, "message-id", messageId}

		responseBuffer, err := client.sendAndRecvResponse([]byte(clientMessage), messageArgs)
		if err != nil {
			return err
		}
		if string(responseBuffer) != clientMessage {
			logger.Error("check-response", logger.Fail, messageArgs...)
			return fmt.Errorf("invalid response: expected %s, got %s", clientMessage, responseBuffer)
		}
		if _, err := writer.Write(append(responseBuffer, '\n')); err != nil {
			logger.Error("write-response", logger.Fail, messageArgs...)
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Error("read-file", logger.Fail, "path", client.config.InputFile, "error", err)
		return err
	}
	logger.Info("send-input-file", logger.Success)
	return nil
}
