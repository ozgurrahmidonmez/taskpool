package pool

import (
	"fmt"
	"github.com/ozgurrahmidonmez/taskpool/model"
	"github.com/ozgurrahmidonmez/taskpool/queue"
	"sync"
	"time"
)

type Work func(in model.Data)

type Pool interface {
	Stop()
	Submit(model.Data)
}

type pool struct {
	maxWorkers int
	in         chan model.Data
	taskQueue  chan model.Data
	taskDef    Work
	done                 chan struct{}
	waitingQueue         queue.FifoQueue
	waitingQueueCapacity int
}

func (p *pool) processWaitingQueue(){
	go func(){
	Loop:
		for p.waitingQueue.Size() > 0{
			item,err := p.waitingQueue.Pull()
			if err != nil {
				fmt.Println(err)
				continue Loop
			}
			select {
				// dequeue
				case p.in <- item:
					continue Loop
				case <-time.After(time.Millisecond * 100):
					if err := p.waitingQueue.PushFront(item); err != nil {
						fmt.Println(err)
					}
					continue Loop
			}
		}
	}()
}

func (p *pool) Dispatch(){
	p.processWaitingQueue()
	var wg sync.WaitGroup
	var workerCount int
	go func() {
		defer close(p.in)
		defer close(p.taskQueue)
		Loop:
		for {
			select {
				case d := <- p.in:
					select {
						case p.taskQueue <- d:
						default:
							if workerCount < p.maxWorkers {
								wg.Add(1)
								go p.startWorker(d, &wg)
								workerCount++
							}else{
								// put in a waiting queue
								if err := p.waitingQueue.Push(d); err != nil {
									fmt.Println(err)
								}
							}
					}
				case <-time.After(time.Millisecond * 1000):
					if workerCount > 0 && p.KillAnyOneWorker(){
						workerCount--
					}
				case <-p.done:
					break Loop
			}
		}
		for workerCount > 0 {
			p.taskQueue <- nil
			workerCount--
		}
		// waiting all workers to finish
		wg.Wait()
	}()
}

func (p *pool) startWorker(d model.Data,wg *sync.WaitGroup){
	// execute task first then start consuming
	p.taskDef(d)
	for t := range p.taskQueue {
		if t == nil{
			wg.Done()
			return
		}
		p.taskDef(t)
	}
}

func (p *pool) KillAnyOneWorker() bool {
	select {
	case p.taskQueue <- nil:
		return true
	default:
		return false
	}
}

func (p *pool) Stop(){
	p.done <- struct{}{}
}

func (p *pool) Submit(d model.Data){
	p.in <- d
}


func New(maxWorkers int,work Work,waitingQueueCapacity int) Pool {
	var fifoQueue  = queue.NewQueue(100.000)
	p := &pool{maxWorkers,make(chan model.Data),make(chan model.Data),work,make(chan struct{}),
		fifoQueue,waitingQueueCapacity}
	p.Dispatch()
	return p
}










