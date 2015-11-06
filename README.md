# appendfs

A filesystem built on go-fuse in Go.

Backed by an append only file.

Note: currently, there is no way to save the filesystem metadata, so as soon as the process exits, all of the data is lost.

To run:
	
	appendfs [-debug] <mountpoint> <backerfile> &

To stop:

	umount <mountpoint>



