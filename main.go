package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Queue struct {
	stack    []string
	msgChan  chan string
	waitChan chan interface{}
	lock     sync.RWMutex
}

func NewQueue() *Queue {
	return &Queue{
		msgChan:  make(chan string),
		waitChan: make(chan interface{}),
		stack:    make([]string, 0),
		lock:     sync.RWMutex{},
	}
}

func (q *Queue) Count() int {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return len(q.stack)
}

func (q *Queue) Run() {
	go func() {
		for {
			if q.Count() == 0 {
				time.Sleep(1 * time.Millisecond)
				continue
			}
			select {
			case q.waitChan <- struct{}{}:
				nextElem := q.Pop()
				q.msgChan <- nextElem
			}
		}
	}()
}

func (q *Queue) GetMessage(timeout uint64) string {
	ctx, cancel := context.WithCancel(context.Background())
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	}
	defer cancel()
	for {
		if q.Count() == 0 && timeout == 0 {
			return ""
		}
		select {
		case <-ctx.Done():
			return ""
		case <-q.waitChan:
			select {
			case <-ctx.Done():
				return ""
			case msg := <-q.msgChan:
				return msg
			}
		}
	}
}

func (q *Queue) Push(s string) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.stack = append(q.stack, s)
}

func (q *Queue) Pop() string {
	q.lock.Lock()
	defer q.lock.Unlock()
	if len(q.stack) == 0 {
		return ""
	}
	res := q.stack[0]
	q.stack = q.stack[1:]
	return res
}

type QueueManager struct {
	queues map[string]*Queue
	lock   sync.RWMutex
}

func NewQueueManager() *QueueManager {
	return &QueueManager{
		queues: make(map[string]*Queue),
		lock:   sync.RWMutex{},
	}
}

func (m *QueueManager) GetQueue(name string) *Queue {
	m.lock.RLock()
	defer m.lock.RUnlock()
	queue, exists := m.queues[name]
	if !exists {
		return nil
	}
	return queue
}

func (m *QueueManager) CreateQueue(name string) *Queue {
	m.lock.Lock()
	defer m.lock.Unlock()
	queue, exists := m.queues[name]
	if !exists {
		queue = NewQueue()
		queue.Run()
		m.queues[name] = queue
	}
	return queue
}

var (
	qManager *QueueManager = nil
	once     sync.Once
)

func GetQueueManager() *QueueManager {
	once.Do(func() {
		qManager = NewQueueManager()
	})
	return qManager
}

func main() {
	manager := GetQueueManager()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		qName := r.URL.Path[len("/"):]
		queue := manager.GetQueue(qName)
		if r.Method == "GET" {
			if queue == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			timeoutStr := r.URL.Query().Get("timeout")
			timeout, err := strconv.ParseUint(timeoutStr, 10, 0)
			if err != nil {
				timeout = 0
			}
			msg := queue.GetMessage(timeout)
			if msg == "" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, err = w.Write([]byte(msg))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		} else if r.Method == "PUT" {
			msg := r.URL.Query().Get("v")
			if msg == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if queue == nil {
				queue = manager.CreateQueue(qName)
			}
			queue.Push(msg)
		}
	})

	args := os.Args
	port := "8080"
	if len(args) > 1 {
		port = args[1]
	}
	err := http.ListenAndServe(fmt.Sprintf(":%s", port), nil)
	panic(err)
}
