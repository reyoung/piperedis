package piperedis

import (
	"context"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

type job struct {
	ctx    context.Context
	args   []interface{}
	result chan<- *redis.Cmd
}

func (j *job) isDone() bool {
	select {
	case <-j.ctx.Done():
		return true
	default:
		return false
	}
}

type bgWorker struct {
	jobChan            chan *job
	complete           sync.WaitGroup
	minCollectInterval time.Duration
	client             redis.UniversalClient
	maxNReqPerBatch    int
}

func (w *bgWorker) init(bufSize int) {
	w.jobChan = make(chan *job, bufSize)
	w.complete.Add(1)
	go func() {
		defer w.complete.Done()

		for {
			j, ok := <-w.jobChan
			if !ok {
				return
			}
			if j.isDone() {
				continue
			}

			jobs := []*job{j}
			interval := time.After(w.minCollectInterval)
			for exited := false; !exited; {
				select {
				case j, ok := <-w.jobChan:
					if !ok {
						exited = true
						continue
					}
					if j.isDone() {
						continue
					}
					jobs = append(jobs, j)
					if len(jobs) >= w.maxNReqPerBatch {
						exited = true
					}
				case <-interval:
					exited = true
				}
			}

			w.doJobs(jobs)
		}
	}()
}

func (w *bgWorker) doJobs(jobs []*job) {
	pipe := w.client.Pipeline()
	cmds := make([]*redis.Cmd, len(jobs))
	for i := range jobs {
		cmds[i] = pipe.Do(jobs[i].ctx, jobs[i].args...)
	}
	_, err := pipe.Exec(context.Background())
	if err != nil {
		for _, cmd := range cmds {
			cmd.SetErr(err)
		}
	}

	for i := range jobs {
		jobs[i].result <- cmds[i]
	}
}

func (w *bgWorker) do(ctx context.Context, args ...interface{}) *redis.Cmd {
	result := make(chan *redis.Cmd, 1)
	j := &job{
		ctx:    ctx,
		args:   args,
		result: result,
	}
	select {
	case w.jobChan <- j:
	case <-ctx.Done():
		cmd := redis.NewCmd(ctx, args)
		cmd.SetErr(ctx.Err())
		return cmd
	}
	select {
	case r := <-result:
		return r
	case <-ctx.Done():
		cmd := redis.NewCmd(ctx, args)
		cmd.SetErr(ctx.Err())
		return cmd
	}
}

func (w *bgWorker) close() error {
	close(w.jobChan)
	w.complete.Wait()
	return nil
}

func newBGWorker(client redis.UniversalClient, bufSize int, minCollectInterval time.Duration,
	maxNReqPerBatch int) (*bgWorker, error) {
	w := &bgWorker{
		minCollectInterval: minCollectInterval,
		client:             client,
		maxNReqPerBatch:    maxNReqPerBatch,
	}
	w.init(bufSize)
	return w, nil
}
