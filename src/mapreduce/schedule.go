package mapreduce

import "fmt"
import "sync"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

    waitGroup := sync.WaitGroup{}
    waitGroup.Add(ntasks)
    taskArgChan := make(chan *DoTaskArgs, 10)
    idleWorkers := make([]string, 0)
    penddingTasks := make([]*DoTaskArgs, 0)

    // 选一个可用的worker派发task
    execTask := func(workerAddr string, taskArg *DoTaskArgs) {
        if (call(workerAddr, "Worker.DoTask", taskArg, nil)) {
            // 成功后, worker重新放回idle数组,
            // 用于运行其他task
            registerChan <- workerAddr
            waitGroup.Done()
        } else {
            // 失败后, 选择其他worker重跑task
            taskArgChan <- taskArg
        }
    }
    tryExecTask := func() {
        for len(penddingTasks) > 0 {
            if len(idleWorkers) == 0 {
                return
            }
            workerAddr := idleWorkers[0]
            taskArg := penddingTasks[0]
            go execTask(workerAddr, taskArg)
            idleWorkers = idleWorkers[1:]
            penddingTasks = penddingTasks[1:]
        }
    }

    // 运行一个goroutine消费task
    go func() {
        for {
            select {
            case workerAddr := <-registerChan:
                // 有worker注册
                idleWorkers = append(idleWorkers, workerAddr)
                tryExecTask()
            case taskArg := <-taskArgChan:
                // 有task要分发
                penddingTasks = append(penddingTasks, taskArg)
                tryExecTask()
            }
        }
    }()

    // 向goroutine发送task
    for i:=0; i<ntasks; i++ {
        taskArg := &DoTaskArgs{
            JobName: jobName,
            Phase: phase,
            TaskNumber: i,
            NumOtherPhase: n_other,
        }
        if phase == mapPhase {
            taskArg.File = mapFiles[i]
        }
        taskArgChan <- taskArg
    }

    // 等待执行完成
    waitGroup.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
