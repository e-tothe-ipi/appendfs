package appendfs

import (
	"time"
	"os"
	"sync"

	"github.com/hanwen/go-fuse/fuse"
)


func NewAppendFS(dataFilePath string) (*AppendFS, error) {
	fs := &AppendFS{}
	fs.root = fs.createNode()
	fs.root.attr.Mode = fuse.S_IFDIR | 0755
	fs.root.attr.Nlink = 2
	fs.blockSize = 4096
	fs.dataFilePath = dataFilePath
	file, err := os.OpenFile(dataFilePath, os.O_RDWR | os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	err = file.Truncate(0)
	if err != nil {
		return nil, err
	}
	fs.dataFile = file
	fs.dataFileOffset = 0
	return fs, nil
}

type AppendFS struct {
	root *AppendFSNode
	blockSize uint32
	dataMutex sync.RWMutex
	dataFile *os.File
	dataFileOffset int
	dataFilePath string
}

func (fs *AppendFS) Root() *AppendFSNode {
	return fs.root
}

func (fs *AppendFS) createNode() *AppendFSNode {
	node := &AppendFSNode{fs: fs,}
	now := time.Now()
	node.attr.SetTimes(&now, &now, &now)
	node.attr.Nlink = 1
	node.attr.Blksize = fs.blockSize
	node.xattr = make(map[string][]byte)
	return node
}

func (fs *AppendFS) Close() error {
	fs.dataMutex.Lock()
	err := fs.dataFile.Close()
	fs.dataMutex.Unlock()
	return err
}
