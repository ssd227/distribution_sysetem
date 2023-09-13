package mapreduce

import "fmt"

type taskPool struct {
	taskChan chan int
}

func (tp *taskPool) initTasks(ntasks int) {
	for i := 0; i < ntasks; i++ {
		tp.taskChan <- i
	}
}

type scheduler struct {
	closing chan chan error
}

func (s *scheduler) close() error {
	errc := make(chan error)
	s.closing <- errc
	return <-errc
}

func (s *scheduler) loop(mr *Master, phase jobPhase, ntasks int, nios int, waitGroup chan bool) {
	var err error

	tpool := taskPool{make(chan int)} // task pool
	go tpool.initTasks(ntasks)        // init Tasks

	// rpc 远程调用，使用goroutinue
	for {
		select {
		// stop 任务分配
		case errc := <-s.closing:
			errc <- err
			return

		case task_id := <-tpool.taskChan: // 给worker分配任务
			go func(taskId int) {
				doTaskArgs := DoTaskArgs{
					JobName:       mr.jobName,
					File:          mr.files[taskId],
					Phase:         phase,
					TaskNumber:    taskId,
					NumOtherPhase: nios, // intermediate file num
				}

				// 从woker池子中取一个可用worker
				workerAddr := <-mr.registerChannel

				// work rpc 服务调用
				ok := call(workerAddr, "Worker.DoTask", &doTaskArgs, new(struct{}))
				if !ok {
					fmt.Printf("Worker.DoTask %s rpc call error\n", workerAddr)
					tpool.taskChan <- taskId
					return // fail
				}

				// woker return scussess
				waitGroup <- true                // work done
				mr.registerChannel <- workerAddr // worker 空闲重新加入待工作队列
				return                           // success
			}(task_id)
		}

	}
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	scheduler := &scheduler{
		closing: make(chan chan error), //for Close
	}

	waitGroup := make(chan bool)
	go scheduler.loop(mr, phase, ntasks, nios, waitGroup) // 分发任务的大循环

	// wait for all tasks done
	for i := 0; i < ntasks; i++ {
		<-waitGroup
	}

	scheduler.close()

	debug("Schedule: %v phase done\n", phase)
}
