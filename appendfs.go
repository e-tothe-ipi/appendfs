package appendfs

import (
	"time"
	"os"
	"sync"
	"errors"
	"encoding/binary"
	"io"
	"fmt"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/golang/protobuf/proto"
	"github.com/e-tothe-ipi/appendfs/messages"
)


func NewAppendFS(dataFilePath string, metadataFilePath string) (*AppendFS, error) {
	fs := &AppendFS{}
	fs.blockSize = 4096
	fs.dataFilePath = dataFilePath
	dataFile, err := os.OpenFile(dataFilePath, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	//err = dataFile.Truncate(0)
	//if err != nil {
	//	return nil, err
	//}
	dataFileStat, err := dataFile.Stat()
	if err != nil {
		return nil, err
	}
	fs.dataFile = dataFile
	fs.dataFileOffset = int(dataFileStat.Size())
	fs.metadataFilePath = metadataFilePath
	metadataFile, err := os.OpenFile(metadataFilePath, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	//err = metadataFile.Truncate(0)
	//if err != nil {
	//	return nil, err
	//}
	fs.metadataFile = metadataFile
	fs.root = fs.createNode(nil)
	fs.root.attr.Mode = fuse.S_IFDIR | 0755
	fs.root.attr.Nlink = 2
	return fs, nil
}

type AppendFS struct {
	root *AppendFSNode
	blockSize uint32
	dataMutex sync.RWMutex
	dataFile io.ReadWriter
	dataFileOffset int
	dataFilePath string
	fileIdMutex sync.RWMutex
	lastFileId uint64
	metadataMutex sync.RWMutex
	metadataFile io.ReadWriteSeeker
	metadataFilePath string
}

func (fs *AppendFS) Root() *AppendFSNode {
	return fs.root
}

func (fs *AppendFS) createNode(parent *AppendFSNode) *AppendFSNode {
	node := &AppendFSNode{fs: fs,}
	now := time.Now()
	node.attr.SetTimes(&now, &now, &now)
	node.attr.Nlink = 1
	node.attr.Blksize = fs.blockSize
	node.xattr = make(map[string][]byte)
	if parent != nil {
		node.parentFileId = parent.fileId
	}
	fs.fileIdMutex.Lock()
	node.fileId = fs.lastFileId + 1
	fs.lastFileId = node.fileId
	fs.fileIdMutex.Unlock()
	return node
}

func (fs *AppendFS) AppendData(data []byte) (int, error) {
	fs.dataMutex.Lock()
	pos := fs.dataFileOffset
	n, err := fs.dataFile.Write(data)
	fs.dataFileOffset += n
	fs.dataMutex.Unlock()
	return pos, err
}

func (fs *AppendFS) AppendMetadata(metadata *messages.FileMetadata) error {
	data, err := proto.Marshal(metadata)
	if err != nil {
		return err
	}
	if len(data) > 65535 {
		return errors.New("Metadata too large")
	}
	n := uint16(len(data))
	fs.metadataMutex.Lock()
	err = binary.Write(fs.metadataFile, binary.BigEndian, n)
	if err == nil {
		_, err = fs.metadataFile.Write(data)
	}
	fs.metadataMutex.Unlock()
	return err
}

func (fs *AppendFS) LoadMetadata() error {
	err := (error)(nil)
	fs.metadataMutex.Lock()
	nodes := make(map[uint64]*messages.FileMetadata)
	children := make(map[uint64][]uint64)
	_, err = fs.metadataFile.Seek(0, 0)
	if err != nil {
		goto Finally
	}
	for {
		var msgLen uint16
		err = binary.Read(fs.metadataFile, binary.BigEndian, &msgLen)
		if err != nil {
			// EOF is ok here
			if err == io.EOF {
				fmt.Println("Reached expected EOF")
				err = nil
				break
			}
			goto Finally
		}
		fmt.Printf("MsgLen: %d\n", msgLen)
		msgBuf := make([]byte, msgLen)
		_, err = fs.metadataFile.Read(msgBuf)
		if err != nil {
			goto Finally
		}
		metadata := &messages.FileMetadata{}
		err = proto.Unmarshal(msgBuf, metadata)
		if err != nil {
			goto Finally
		}
		fmt.Printf("Read nodeId:%d parentNodeId:%d\n", metadata.GetFileId(), metadata.GetParentFileId())
		if currentNode, ok := nodes[metadata.GetFileId()]; ok {
			proto.Merge(currentNode, metadata)
		} else {
			nodes[metadata.GetFileId()] = metadata
		}
	}
	for id, node := range nodes {
		if node.ParentFileId != nil {
			if currentChildren, ok := children[node.GetParentFileId()]; ok {
				children[node.GetParentFileId()] = append(currentChildren, id)
			} else {
				children[node.GetParentFileId()] = append(make([]uint64, 0), id)
			}
		} else {
			err = errors.New("Corrupt metadata: missing ParentFileId for file " + string(id))
			goto Finally
		}
	}

	fs.addChildrenHelper(nodes, children, fs.root)

	Finally:
	fs.metadataMutex.Unlock()
	return err
}

func (fs *AppendFS) addChildrenHelper(nodes map[uint64]*messages.FileMetadata, children map[uint64][]uint64, currentNode *AppendFSNode) {
	fileId := currentNode.fileId
	fmt.Printf("Adding Children for node %d\n", fileId)
	if nodeChildren, ok := children[fileId]; ok {
		newChildren := make([]*AppendFSNode, 0)
		for _, childId := range nodeChildren {
			childMetadata := nodes[childId]
			child := FromFileMetadata(fs, childMetadata)
			currentNode.Inode().NewChild(child.name, child.attr.IsDir(), child)
			newChildren = append(newChildren, child)
		}
		for _, child := range newChildren {
			fs.addChildrenHelper(nodes, children, child)
		}
	}
}

func (fs *AppendFS) Close() error {
	var err error
	fs.dataMutex.Lock()
	if closer, ok := fs.dataFile.(io.Closer); ok {
		err = closer.Close()
	}
	fs.dataMutex.Unlock()
	if err != nil {
		return err
	}
	fs.metadataMutex.Lock()
	if closer, ok := fs.metadataFile.(io.Closer); ok {
		err = closer.Close()
	}
	fs.metadataMutex.Unlock()
	if err != nil {
		return err
	}
	return nil
}
