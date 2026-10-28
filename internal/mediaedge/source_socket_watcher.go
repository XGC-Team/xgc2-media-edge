package mediaedge

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

// All camera sources share one inotify fd and reader. Ancestor watches also
// cover runtime directories which are created after media-edge starts.
type sourceSocketWatcher struct {
	fd    int
	file  *os.File
	mu    sync.Mutex
	dirs  map[int]string
	peers map[*cameraPeer]struct{}
	done  chan struct{}
}

func (server *MediaMTXServer) watchSourceSockets() (*sourceSocketWatcher, error) {
	server.socketWatchOnce.Do(func() {
		fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
		if err != nil {
			server.socketWatchError = err
			return
		}
		watcher := &sourceSocketWatcher{fd: fd, file: os.NewFile(uintptr(fd), "camera source sockets"),
			dirs: make(map[int]string), peers: make(map[*cameraPeer]struct{}), done: make(chan struct{})}
		server.socketWatcher = watcher
		if !server.startBackground(func() {
			stop := context.AfterFunc(server.lifecycleContext, func() { _ = watcher.file.Close() })
			defer stop()
			defer watcher.file.Close()
			watcher.run()
		}) {
			_ = watcher.file.Close()
			close(watcher.done)
		}
	})
	return server.socketWatcher, server.socketWatchError
}

func (watcher *sourceSocketWatcher) watchParent(socket string) error {
	for directory := filepath.Dir(socket); ; directory = filepath.Dir(directory) {
		for _, watched := range watcher.dirs {
			if watched == directory {
				return nil
			}
		}
		wd, err := unix.InotifyAddWatch(watcher.fd, directory,
			unix.IN_CREATE|unix.IN_DELETE|unix.IN_MOVED_FROM|unix.IN_MOVED_TO|unix.IN_DELETE_SELF|unix.IN_MOVE_SELF)
		if err == nil {
			watcher.dirs[wd] = directory
			return nil
		}
		if directory == "/" {
			return err
		}
	}
}

func (watcher *sourceSocketWatcher) add(peer *cameraPeer) error {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if err := watcher.watchParent(peer.control.socket); err != nil {
		return err
	}
	watcher.peers[peer] = struct{}{}
	return nil
}

func (watcher *sourceSocketWatcher) remove(peer *cameraPeer) {
	watcher.mu.Lock()
	delete(watcher.peers, peer)
	watcher.mu.Unlock()
}

func (watcher *sourceSocketWatcher) run() {
	defer close(watcher.done)
	buffer := make([]byte, 16<<10)
	for {
		count, err := watcher.file.Read(buffer)
		if err != nil {
			return
		}
		for offset := 0; offset+unix.SizeofInotifyEvent <= count; {
			event := buffer[offset:]
			wd := int(int32(binary.NativeEndian.Uint32(event)))
			mask := binary.NativeEndian.Uint32(event[4:])
			length := int(binary.NativeEndian.Uint32(event[12:]))
			end := offset + unix.SizeofInotifyEvent + length
			if end > count {
				break
			}
			name := strings.TrimRight(string(buffer[offset+unix.SizeofInotifyEvent:end]), "\x00")
			watcher.mu.Lock()
			path := filepath.Join(watcher.dirs[wd], name)
			if mask&unix.IN_IGNORED != 0 {
				delete(watcher.dirs, wd)
			}
			var affected []*cameraPeer
			for peer := range watcher.peers {
				if mask&unix.IN_Q_OVERFLOW != 0 || peer.control.socket == path || strings.HasPrefix(peer.control.socket, path+"/") {
					_ = watcher.watchParent(peer.control.socket)
					affected = append(affected, peer)
				}
			}
			watcher.mu.Unlock()
			for _, peer := range affected {
				peer.invalidate(true)
			}
			offset = end
		}
	}
}
