package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/e-tothe-ipi/appendfs"
)

// this function was borrowed from https://raw.githubusercontent.com/hanwen/go-fuse/master/example/memfs/main.go
func main() {
	// Scans the arg list and sets up flags
	debug := flag.Bool("debug", false, "print debugging messages.")
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Println("usage: inmemfs MOUNTPOINT")
		os.Exit(2)
	}

	mountPoint := flag.Arg(0)
	fs, err := appendfs.NewAppendFS(flag.Arg(1))
	if err != nil {
		fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}
	options := nodefs.NewOptions()
	options.Owner = nil
	conn := nodefs.NewFileSystemConnector(fs.Root(), options)
	server, err := fuse.NewServer(conn.RawFS(), mountPoint, nil)
	if err != nil {
		fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}
	server.SetDebug(*debug)
	fmt.Println("Mounted!")
	server.Serve()
	fmt.Println("Closing filesystem")
	err = fs.Close()
	if err != nil {
		fmt.Printf("Unmount fail: %v\n", err)
		os.Exit(1)
	}
}









