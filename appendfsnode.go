package appendfs

import (
	"fmt"
	"sync"
	"time"
	"syscall"
	"os"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/e-tothe-ipi/appendfs/messages"
	"github.com/e-tothe-ipi/appendfs/rangelist"
)

type AppendFSNode struct {
	fs *AppendFS
	fileId uint64
	parentFileId uint64
	inode *nodefs.Inode
	metadataMutex sync.RWMutex
	name string
	attr fuse.Attr
	xattr map[string][]byte
	contentRanges rangelist.RangeList
	symlink	[]byte
}

func (node *AppendFSNode) incrementLinks() {
	node.metadataMutex.Lock()
	node.attr.Nlink += 1
	node.metadataMutex.Unlock()
}

func (node *AppendFSNode) decrementLinks() {
	node.metadataMutex.Lock()
	node.attr.Nlink -= 1
	node.metadataMutex.Unlock()
}

func (node *AppendFSNode) Inode() *nodefs.Inode {
	return node.inode
}

func (node *AppendFSNode) SetInode(inode *nodefs.Inode) {
	node.inode = inode
}

func (node *AppendFSNode) OnMount(conn *nodefs.FileSystemConnector) {
	fmt.Printf("Mounted\n")
	if node == node.fs.root {
		err := node.fs.LoadMetadata()
		if err != nil {
			panic(err)
		}
	}
}

func (node *AppendFSNode) OnUnmount() {
	fmt.Printf("Unmounted\n")
}

func (parent *AppendFSNode) Lookup(out *fuse.Attr, name string, context *fuse.Context) (*nodefs.Inode, fuse.Status) {
	child := parent.inode.GetChild(name)
	if child != nil {
		if appendfsChild, success := child.Node().(*AppendFSNode); success {
			appendfsChild.metadataMutex.RLock()
			*out = appendfsChild.attr
			appendfsChild.metadataMutex.RUnlock()
		}
		return child, fuse.OK
	}
	return nil, fuse.ENOENT
}

func (node *AppendFSNode) Deletable() bool {
	node.metadataMutex.RLock()
	deletable := node.attr.Nlink == 0
	node.metadataMutex.RUnlock()
	return deletable
}

const (
	NODE_SIZE_GC_THRESHOLD = 1 * 1024 * 1024 // 1MB
)

func (node *AppendFSNode) OnForget() {
}


func (node *AppendFSNode) Access(mode uint32, context *fuse.Context) (code fuse.Status) {

	node.metadataMutex.RLock()
	code = fuse.OK
	//if mode == fuse.F_OK {
	//	if !getBit(&node.attr.Mode, fuse.S_IFREG){
	//		code = fuse.EACCES
	//	}
	//}
	if mode & fuse.R_OK > 0 {
		if !( (node.attr.Uid == context.Uid && getBit(&node.attr.Mode, syscall.S_IRUSR)) ||
		      (node.attr.Gid == context.Gid && getBit(&node.attr.Mode, syscall.S_IRGRP)) ||
			  (getBit(&node.attr.Mode, syscall.S_IROTH)) ) {
			code = fuse.EACCES
		}
	}
	if mode & fuse.W_OK > 0 {
		if !( (node.attr.Uid == context.Uid && getBit(&node.attr.Mode, syscall.S_IWUSR)) ||
		      (node.attr.Gid == context.Gid && getBit(&node.attr.Mode, syscall.S_IWGRP)) ||
			  (getBit(&node.attr.Mode, syscall.S_IWOTH)) ) {
			code = fuse.EACCES
		}
	}
	if mode & fuse.X_OK > 0 {
		if !( (node.attr.Uid == context.Uid && getBit(&node.attr.Mode, syscall.S_IXUSR)) ||
		      (node.attr.Gid == context.Gid && getBit(&node.attr.Mode, syscall.S_IXGRP)) ||
			  (getBit(&node.attr.Mode, syscall.S_IXOTH)) ) {
			code = fuse.EACCES
		}
	}
	node.metadataMutex.RUnlock()
	return
}

func (node *AppendFSNode) Readlink(c *fuse.Context) ([]byte, fuse.Status) {
	node.metadataMutex.RLock()
	if !node.attr.IsSymlink() {
		return nil, fuse.EINVAL
	}
	node.metadataMutex.RUnlock()
	return node.symlink, fuse.OK
}

func (node *AppendFSNode) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) (newNode *nodefs.Inode, code fuse.Status) {
	return nil, fuse.ENOSYS
}

func (parent *AppendFSNode) Mkdir(name string, mode uint32, context *fuse.Context) (newNode *nodefs.Inode, code fuse.Status) {
	if parent.Inode().GetChild(name) != nil {
		return nil, fuse.Status(syscall.EEXIST)
	}
	node := parent.fs.createNode(parent)
	node.attr.Mode = mode | fuse.S_IFDIR
	node.attr.Nlink = 2
	node.name = name
	inode := parent.inode.NewChild(name, true, node)
	parent.incrementLinks()
	return inode, fuse.OK
}

func (node *AppendFSNode) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	child := node.Inode().GetChild(name)
	if(child == nil) {
		return fuse.ENOENT
	}
	if appendfsChild, ok := child.Node().(*AppendFSNode); ok {
		appendfsChild.decrementLinks()
		appendfsChild.metadataMutex.RLock()
		if appendfsChild.attr.Nlink == 0 {
			node.decrementLinks()
		}
		appendfsChild.metadataMutex.RUnlock()
	}
	node.Inode().RmChild(name)
	return fuse.OK
}

func (node *AppendFSNode) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	return node.Unlink(name, context)
}

func (parent *AppendFSNode) Symlink(name string, content string, context *fuse.Context) (*nodefs.Inode, fuse.Status) {
	if parent.Inode().GetChild(name) != nil {
		return nil, fuse.Status(syscall.EEXIST)
	}
	node := parent.fs.createNode(parent)
	node.attr.Mode = 0777 | fuse.S_IFLNK
	contentBytes := []byte(content)
	node.setSize(uint64(len(contentBytes)))
	node.symlink = contentBytes
	node.name = name
	parent.Inode().NewChild(name, false, node)
	parent.incrementLinks()
	return node.Inode(), fuse.OK
}

func (parent *AppendFSNode) Rename(oldName string, newParent nodefs.Node, newName string, context *fuse.Context) (code fuse.Status) {
	child := parent.Inode().GetChild(oldName)
	if(child == nil) {
		return fuse.ENOENT
	}
	parent.Inode().RmChild(oldName)
	parent.decrementLinks()
	newParent.Inode().RmChild(newName)
	newParent.Inode().AddChild(newName, child)
	if appendfsNewParent, ok := newParent.(*AppendFSNode); ok {
		appendfsNewParent.incrementLinks()
	}
	return fuse.OK
}

func (node *AppendFSNode) Link(name string, existing nodefs.Node, context *fuse.Context) (newNode *nodefs.Inode, code fuse.Status) {
	if node.Inode().GetChild(name) != nil {
		return nil, fuse.Status(syscall.EEXIST)
	}
	node.Inode().AddChild(name, existing.Inode())
	if appendfsChild, ok := existing.(*AppendFSNode); ok {
		appendfsChild.incrementLinks()
	}
	return existing.Inode(), fuse.OK
}

func (node *AppendFSNode) AsFileMetadata() *messages.FileMetadata {
	metadata := &messages.FileMetadata{FileId:&node.fileId, Mode:&node.attr.Mode,
					Uid:&node.attr.Uid, Gid:&node.attr.Gid, ParentFileId:&node.parentFileId,
					Atime:&node.attr.Atime, Mtime:&node.attr.Mtime, Ctime:&node.attr.Ctime,
					Name:&node.name, Nlink:&node.attr.Nlink}
	return metadata
}

func FromFileMetadata(fs *AppendFS, md *messages.FileMetadata) (*AppendFSNode) {
	node := &AppendFSNode{fileId:md.GetFileId(), parentFileId:md.GetParentFileId(),
							name:md.GetName()}
	node.attr.Uid = md.GetUid()
	node.attr.Gid = md.GetGid()
	node.attr.Mode = md.GetMode()
	node.attr.Atime = md.GetAtime()
	node.attr.Mtime = md.GetMtime()
	node.attr.Ctime = md.GetCtime()
	node.attr.Nlink = md.GetNlink()
	node.attr.Size = md.GetSize()
	node.symlink = md.GetSymlink()
	node.fs = fs
	node.attr.Blksize = fs.blockSize
	for _, entry := range md.GetContents().GetEntry() {
		fData := fileSegmentEntry{fileOffset:int(entry.GetBase())}
		node.contentRanges.Overwrite(&rangelist.RangeListEntry{Min:int(entry.GetStart()),
																Max:int(entry.GetEnd()),
																Data:fData})

	}
	return node
}

func (parent *AppendFSNode) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, child *nodefs.Inode, code fuse.Status) {
	if parent.Inode().GetChild(name) != nil {
		return nil, nil, fuse.Status(syscall.EEXIST)
	}
	node := parent.fs.createNode(parent)
	node.attr.Mode = mode | fuse.S_IFREG
	node.attr.Uid = context.Uid
	node.attr.Gid = context.Gid
	node.name = name
	parent.Inode().NewChild(name, false, node)
	parent.incrementLinks()

	err := node.fs.AppendMetadata(node.AsFileMetadata())
	if err != nil {
		return nil, nil, fuse.EIO
	}

	f, openStatus := node.Open(flags, context)
	if openStatus != fuse.OK {
		return nil, nil, openStatus
	}
	return f, node.Inode(), fuse.OK
}

func (node *AppendFSNode) Open(flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	f := node.createFile()
	f.flags = flags
	return f, fuse.OK
}

func (node *AppendFSNode) OpenDir(context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	children := node.inode.FsChildren()
	ls := make([]fuse.DirEntry, 0, len(children))
	for name, inode := range children {
		if childNode, success := inode.Node().(*AppendFSNode); success {
			childNode.metadataMutex.RLock()
			ls = append(ls, fuse.DirEntry{Name: name, Mode: childNode.attr.Mode})
			childNode.metadataMutex.RUnlock()
		}
	}
	return ls, fuse.OK
}

type fileSegmentEntry struct {
	fileOffset int
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (node *AppendFSNode) Read(file nodefs.File, dest []byte, off int64, context *fuse.Context) (fuse.ReadResult, fuse.Status) {
	ret := fuse.OK
	dataFile, err := os.Open(node.fs.dataFilePath)
	if err != nil {
		ret = fuse.EIO
	}
	node.metadataMutex.RLock()
	start, end := int(off), int(off) + len(dest) - 1
	entries := node.contentRanges.InRange(start, end)
	for _, entry := range entries {
		readStart, readEnd := max(entry.Min, start), min(entry.Max, end) + 1
		blockStart, blockEnd := readStart - int(off), readEnd - int(off)
		blockDest := dest[blockStart:blockEnd]
		if fse, ok := entry.Data.(fileSegmentEntry); ok {
			readPos :=  int64(fse.fileOffset + readStart)
			//fmt.Printf("fileOffset: %d, blockStart: %d, blockEnd: %d, readPos: %d, min: %d, max: %d\n", 
			//fse.fileOffset, blockStart, blockEnd, readPos, entry.Min, entry.Max)
			_, err = dataFile.ReadAt(blockDest,readPos)
			if err != nil {
				fmt.Printf("Read error\n")
				ret = fuse.EIO
			}

		}
	}
	node.metadataMutex.RUnlock()
	err = dataFile.Close()
	if err != nil {
		ret = fuse.EIO
	}
	if ret != fuse.OK {
		return nil, ret
	}
	return fuse.ReadResultData(dest), ret
}


func (node *AppendFSNode) setSize(size uint64) {
	node.attr.Size = size
	node.attr.Blocks = uint64(node.attr.Size / 512)
	if node.attr.Size % 512 > 0 {
		node.attr.Blocks += 1
	}
}

func (node *AppendFSNode) Write(file nodefs.File, data []byte, off int64, context *fuse.Context) (written uint32, code fuse.Status) {
	if f, ok := file.(*AppendFSFile); ok {
		f.SetDirty(true)
	}
	pos, err := node.fs.AppendData(data)
	if err != nil {
		return 0, fuse.EIO
	}
	n := len(data)
	node.metadataMutex.Lock()
	node.contentRanges.Overwrite(&rangelist.RangeListEntry{Min:int(off),
			Max:int(off) + n - 1,
			Data:fileSegmentEntry{fileOffset: pos - int(off)}})
	node.setSize(uint64(max(int(node.attr.Size), len(data) + int(off))))
	node.metadataMutex.Unlock()
	return uint32(n), fuse.OK
}


func (node *AppendFSNode) GetXAttr(attribute string, context *fuse.Context) (data []byte, code fuse.Status) {
	node.metadataMutex.RLock()
	xattr := node.xattr[attribute]
	node.metadataMutex.RUnlock()
	if xattr == nil {
		return nil, fuse.ENODATA
	}
	return xattr, fuse.OK
}

func (node *AppendFSNode) RemoveXAttr(attr string, context *fuse.Context) fuse.Status {
	node.metadataMutex.Lock()
	xattr := node.xattr[attr]
	delete(node.xattr, attr)
	node.metadataMutex.Unlock()
	if xattr == nil {
		return fuse.ENODATA
	}
	return fuse.OK
}

func (node *AppendFSNode) SetXAttr(attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	node.metadataMutex.Lock()
	node.xattr[attr] = data
	node.metadataMutex.Unlock()
	return fuse.OK
}

func (node *AppendFSNode) ListXAttr(context *fuse.Context) (attrs []string, code fuse.Status) {
	out := make([]string, 0)
	node.metadataMutex.RLock()
	for key := range node.xattr {
		out = append(out, key)
	}
	node.metadataMutex.RUnlock()
	return out, fuse.OK
}

func (node *AppendFSNode) GetAttr(out *fuse.Attr, file nodefs.File, context *fuse.Context) (code fuse.Status) {
	node.metadataMutex.RLock()
	*out = node.attr
	node.metadataMutex.RUnlock()
	return fuse.OK
}

func setBit(attr *uint32, mask uint32, field uint32) {
	*attr &= ^mask
	*attr |= (mask & field)
}

func getBit(attr *uint32, mask uint32) bool {
	return *attr & mask > 0
}

func (node *AppendFSNode) Chmod(file nodefs.File, perms uint32, context *fuse.Context) (code fuse.Status) {
	node.metadataMutex.Lock()
	setBit(&node.attr.Mode, syscall.S_IRUSR, perms)
	setBit(&node.attr.Mode, syscall.S_IWUSR, perms)
	setBit(&node.attr.Mode, syscall.S_IXUSR, perms)
	setBit(&node.attr.Mode, syscall.S_IRGRP, perms)
	setBit(&node.attr.Mode, syscall.S_IWGRP, perms)
	setBit(&node.attr.Mode, syscall.S_IXGRP, perms)
	setBit(&node.attr.Mode, syscall.S_IROTH, perms)
	setBit(&node.attr.Mode, syscall.S_IWOTH, perms)
	setBit(&node.attr.Mode, syscall.S_IXOTH, perms)
	node.metadataMutex.Unlock()
	return fuse.OK
}

func (node *AppendFSNode) Chown(file nodefs.File, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	node.metadataMutex.Lock()
	node.attr.Uid = uid
	node.attr.Gid = gid
	node.metadataMutex.Unlock()
	return fuse.OK
}

func (node *AppendFSNode) Truncate(file nodefs.File, size uint64, context *fuse.Context) (code fuse.Status) {
	if size == 0 {
		node.metadataMutex.Lock()
		node.contentRanges = rangelist.RangeList{}
		node.setSize(0)
		node.metadataMutex.Unlock()
		return fuse.OK
	}
	return fuse.ENOSYS
}

func (node *AppendFSNode) Utimens(file nodefs.File, atime *time.Time, mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	node.metadataMutex.Lock()
	changeTime := node.attr.ChangeTime()
	node.attr.SetTimes(atime, mtime, &changeTime)
	node.metadataMutex.Unlock()
	return fuse.OK
}

func (node *AppendFSNode) Fallocate(file nodefs.File, off uint64, size uint64, mode uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.ENOSYS
}

func (node *AppendFSNode) StatFs() *fuse.StatfsOut {
	return &fuse.StatfsOut{}
}
