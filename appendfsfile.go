package appendfs

import (
	"fmt"
	"time"
	"sync"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/e-tothe-ipi/appendfs/messages"
	"github.com/golang/protobuf/proto"
)

var _ nodefs.File = (*AppendFSFile)(nil)

type AppendFSFile struct {
	node *AppendFSNode
	flags uint32
	metadataMutex sync.RWMutex
	dirty bool
}

func (f *AppendFSFile) SetDirty(dirty bool) {
	f.metadataMutex.Lock()
	f.dirty = dirty
	f.metadataMutex.Unlock()
}

func (f *AppendFSFile) Dirty() bool {
	f.metadataMutex.RLock()
	dirty := f.dirty
	f.metadataMutex.RUnlock()
	return dirty
}

func (node *AppendFSNode) createFile() *AppendFSFile {
	return &AppendFSFile{node: node}
}

// Called upon registering the filehandle in the inode.
func (f *AppendFSFile) SetInode(inode *nodefs.Inode) {
	if f.node.inode != inode {
		panic("AppendFSFile: wrong inode detected")
	}
}

// The String method is for debug printing.
func (f *AppendFSFile) String() string {
	return fmt.Sprintf("AppendFSFile")
}

// Wrappers around other File implementations, should return
// the inner file here.
func (f *AppendFSFile) InnerFile() nodefs.File {
	return nil
}

func (f *AppendFSFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status){
	return f.node.Read(f, dest, off, nil)
}

func (f *AppendFSFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	return f.node.Write(f, data, off, nil)
}

// Flush is called for close() call on a file descriptor. In
// case of duplicated descriptor, it may be called more than
// once for a file.
func (f *AppendFSFile) Flush() fuse.Status {
	return f.Fsync(0)
}

// This is called to before the file handle is forgotten. This
// method has no return value, so nothing can synchronizes on
// the call. Any cleanup that requires specific synchronization or
// could fail with I/O errors should happen in Flush instead.
func (f *AppendFSFile) Release() {

}

func (f *AppendFSFile) Fsync(flags int) (code fuse.Status) {
	if(f.Dirty()) {
		metadata := &messages.FileMetadata{FileId:&f.node.fileId,
											Contents:&messages.FileMap{},
											Size:&f.node.attr.Size}
		rlEntries := f.node.contentRanges.InRange(0, int(f.node.attr.Size))
		metadata.Contents.Entry = make([]*messages.FileMapEntry,0,len(rlEntries))
		for _, entry := range rlEntries {
			if fData, ok := entry.Data.(fileSegmentEntry); ok {
				newEntry := &messages.FileMapEntry{Start:proto.Uint64(uint64(entry.Min)),
													End:proto.Uint64(uint64(entry.Max)),
													Base:proto.Uint64(uint64(fData.fileOffset))}
				metadata.Contents.Entry = append(metadata.Contents.Entry, newEntry)
			}
		}
		err := f.node.fs.AppendMetadata(metadata)
		if (err != nil) {
			fmt.Println(err)
			return fuse.EIO
		}
	}
	return fuse.OK
}

// The methods below may be called on closed files, due to
// concurrency.  In that case, you should return EBADF.
func (f *AppendFSFile) Truncate(size uint64) fuse.Status {
	return fuse.ENOSYS
}

func (f *AppendFSFile) GetAttr(out *fuse.Attr) fuse.Status {
	return f.node.GetAttr(out, f, nil)
}

func (f *AppendFSFile) Chown(uid uint32, gid uint32) fuse.Status {
	return f.node.Chown(f, uid, gid, nil)
}

func (f *AppendFSFile) Chmod(perms uint32) fuse.Status {
	return f.node.Chmod(f, perms, nil)
}

func (f *AppendFSFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	return f.node.Utimens(f, atime, mtime, nil)
}

func (f *AppendFSFile) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	return f.node.Fallocate(f, off, size, mode, nil)
}








