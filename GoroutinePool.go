package simple_goroutine_pool

import (
	"fmt"
	"sync"
	"time"
)

type GoroutinePool struct {
	mu           sync.Mutex    //mu 线程锁,用于处理并发
	coreSize     int32         //coreSize 核心工作协程数量
	maxSize      int32         //maxSize 最大工作协程数量
	queue        chan Task     //queue 任务队列
	duration     time.Duration //duration 最大协程等待时间
	status       int32         //status 线程池状态,0:初始化,1:正在运行,2:关闭,4:停止
	size         int32         //size 当前线程池中的工作线程数
	maxQueueSize int32         //maxQueueSize 工作队列中的最大数量
	reject       func()        //reject 丢弃任务后回调该方法
}

//NewGoroutinePool 构建一个新的线程池对象
func NewGoroutinePool(core int32, max int32, queueSize int32,
	duration time.Duration, rejectHandler func()) *GoroutinePool {
	return &GoroutinePool{
		mu:           sync.Mutex{},
		coreSize:     core,
		maxSize:      max,
		maxQueueSize: queueSize,
		queue:        make(chan Task, queueSize),
		duration:     duration,
		reject:       rejectHandler,
	}
}

//atomicOper 原子化线程池操作
func (pool *GoroutinePool) atomicOper(f func() bool) bool{
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return f()
}

//Start 开始一个新的线程池,新的线程池必须在Put()调用之前调用一次该函数
func (pool *GoroutinePool) Start() {
	if pool.status != 0 {
		panic("GoroutinePool has started!")
	}
	pool.status = 1
}

//Put 向当前线程池任务队列中加入一个新的任务,返回一个是否接收该任务
func (pool *GoroutinePool) Put(t Task) bool {
	if pool.status != 1 {
		err := fmt.Sprintf("GroutinePool status: %d\n", pool.status)
		panic(err)
	}
	return pool.atomicOper(func() bool {
		//如果当前协程数小于核心协程数则创建新协程处理任务
		if pool.size < pool.coreSize {
			pool.size++
			go pool.run(t)
			return true
		} else if len(pool.queue) < int(pool.maxQueueSize) {
			//当前协程数大于等于核心协程数并且队列没有满，则将任务放入队列
			pool.queue <- t
			return true
		} else if pool.size < pool.maxSize {
			//如果当前协程数量小于最大支持协程数,则扩大协程池
			pool.size++
			go pool.run(t)
			return true
		} else {
			//协程池已满,调用reject方法
			pool.reject()
			return false
		}
	})
}

func (pool *GoroutinePool) run(t Task) {
	if t != nil {
		t.Run()
	}
loop:
	for pool.status == 1 || pool.status == 2 {
		select {
		case t := <-pool.queue:
			t.Run()
		case <-time.After(pool.duration):
			//如果超时还没有拿到任务，说明该协程已经空闲了一段时间，该协程的生命周期可以结束
			if pool.atomicOper(func() bool {
				if pool.size > pool.coreSize {
					pool.size--
					return true
				} else if pool.status == 2 {
					//如果协程池状态为shutdown,需要查看队列中是否还有任务，如果有任务则保留协程，否则结束协程
					if len(pool.queue) > 0 {
						return false
					} else {
						pool.size--
						return true
					}
				} else {
					return false
				}
			}) {
				break loop
			}
		}
	}
}

func (pool *GoroutinePool) Shutdown() {
	pool.status = 2
}

func (pool *GoroutinePool) ShutdownNow() {
	pool.status = 3
}

func (pool *GoroutinePool) GetStatus() int32 {
	return pool.status
}

func (pool *GoroutinePool) GetQueueSize() int32 {
	return int32(len(pool.queue))
}

func (pool *GoroutinePool) GetPoolSize() int32 {
	return pool.size
}
