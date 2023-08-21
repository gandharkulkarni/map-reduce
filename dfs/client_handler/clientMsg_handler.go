package client_handler

import (
	"encoding/binary"
	"net"

	"google.golang.org/protobuf/proto"
)

type FileHandler struct {
	conn net.Conn
}

func NewFileHandler(conn net.Conn) *FileHandler {
	m := &FileHandler{
		conn: conn,
	}
	return m
}

func (m *FileHandler) readN(buf []byte) error {
	bytesRead := uint64(0)
	for bytesRead < uint64(len(buf)) {
		n, err := m.conn.Read(buf[bytesRead:])
		if err != nil {
			return err
		}
		bytesRead += uint64(n)
	}
	return nil
}

func (m *FileHandler) writeN(buf []byte) error {
	bytesWritten := uint64(0)
	for bytesWritten < uint64(len(buf)) {
		n, err := m.conn.Write(buf[bytesWritten:])
		if err != nil {
			return err
		}
		bytesWritten += uint64(n)
	}
	return nil
}

func (m *FileHandler) Send(wrapper *ClientMsg) error {
	serialized, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}

	prefix := make([]byte, 64000)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	m.writeN(prefix)
	m.writeN(serialized)

	return nil
}

func (m *FileHandler) Receive() (*ClientMsg, error) {
	prefix := make([]byte, 64000)
	m.readN(prefix)

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	m.readN(payload)

	wrapper := &ClientMsg{}
	err := proto.Unmarshal(payload, wrapper)
	return wrapper, err
}

func (m *FileHandler) Close() {
	m.conn.Close()
}
