package safe_socket

import "io"

func SendAll(socket io.Writer, bytes []byte) error {
	for totalSent := 0; totalSent < len(bytes); {
		n, err := socket.Write(bytes[totalSent:])
		if err != nil {
			return err
		}
		totalSent += n
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	for recv := 0; recv < size; {
		n, err := socket.Read(buff[recv:])
		recv += n
		if err != nil {
			return buff[:recv], err
		}
	}
	return buff, nil
}
