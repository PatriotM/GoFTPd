package core

import (
	"path"
	"sync"
	"time"
)

type releasePostHookQueue struct {
	tasks chan func()
}

var (
	releasePostHookQueuesMu sync.Mutex
	releasePostHookQueues   = map[string]*releasePostHookQueue{}
)

func enqueueReleasePostHook(dirPath string, task func()) {
	if task == nil {
		return
	}
	key := path.Clean("/" + dirPath)
	if key == "." || key == "" {
		key = "/"
	}

	releasePostHookQueuesMu.Lock()
	q := releasePostHookQueues[key]
	if q == nil {
		q = &releasePostHookQueue{tasks: make(chan func(), 128)}
		releasePostHookQueues[key] = q
		go runReleasePostHookQueue(key, q)
	}
	releasePostHookQueuesMu.Unlock()

	q.tasks <- task
}

func runReleasePostHookQueue(key string, q *releasePostHookQueue) {
	idle := time.NewTimer(30 * time.Second)
	defer idle.Stop()

	for {
		select {
		case task := <-q.tasks:
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			if task != nil {
				task()
			}
			idle.Reset(30 * time.Second)
		case <-idle.C:
			releasePostHookQueuesMu.Lock()
			if releasePostHookQueues[key] == q && len(q.tasks) == 0 {
				delete(releasePostHookQueues, key)
				releasePostHookQueuesMu.Unlock()
				return
			}
			releasePostHookQueuesMu.Unlock()
			idle.Reset(30 * time.Second)
		}
	}
}
