//dfs msg handler code
package storageNode_handler

import (
	"encoding/binary"
	"net"
	"google.golang.org/protobuf/proto"
)

type StorageNode struct {
	conn net.Conn
}

func NewStorageNode(conn net.Conn) *StorageNode {
	m := &StorageNode{
		conn: conn,
	}
	return m
}

func (m *StorageNode) readN(buf []byte) error {
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

func (m *StorageNode) writeN(buf []byte) error {
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

func (m *StorageNode) Send(wrapper *Beat) error {  
	serialized, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}

	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	m.writeN(prefix)
	m.writeN(serialized)

	return nil
}

func (m *StorageNode) Receive() (*Beat, error) {
	prefix := make([]byte, 8)
	m.readN(prefix)

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	m.readN(payload)

	wrapper := &Beat{}
	err := proto.Unmarshal(payload, wrapper)
	return wrapper, err
}

// StorageNode

func (m *StorageNode) SendResponse(wrapper *Response) error {  
	serialized, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}

	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	m.writeN(prefix)
	m.writeN(serialized)

	return nil
}

func (m *StorageNode) ReceiveResponse() (*Response, error) {
	prefix := make([]byte, 8)
	m.readN(prefix)

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	m.readN(payload)

	wrapper := &Response{}
	err := proto.Unmarshal(payload, wrapper)
	return wrapper, err
}

//

func (m *StorageNode) Close() {
	m.conn.Close()
}

