package piperedis

import (
	"context"
	"emperror.dev/errors"
	"github.com/go-redis/redis/v8"
	"go.uber.org/atomic"
	"time"
)

type Client struct {
	workers []*bgWorker
	nextPos *atomic.Uint64
}

func (c *Client) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	pos := c.nextPos.Inc()
	w := c.workers[pos%uint64(len(c.workers))]
	return w.do(ctx, args...)
}

func (c *Client) Close() (err error) {
	for _, w := range c.workers {
		err = errors.Append(err, w.close())
	}
	return
}

const (
	kDefaultChannelBufferSize  = 256
	kDefaultMinCollectInterval = time.Millisecond
	kDefaultNWorker            = 1
)

type Option struct {
	NumBackgroundWorker int
	ChannelBufferSize   int
	MinCollectInterval  time.Duration
}

func New(client redis.UniversalClient, option Option) (*Client, error) {
	if option.NumBackgroundWorker <= 0 {
		option.NumBackgroundWorker = kDefaultNWorker
	}
	if option.ChannelBufferSize <= 0 {
		option.ChannelBufferSize = kDefaultChannelBufferSize
	}
	if option.MinCollectInterval <= 0 {
		option.MinCollectInterval = kDefaultMinCollectInterval
	}

	cli := &Client{
		workers: nil,
		nextPos: atomic.NewUint64(0),
	}

	for i := 0; i < option.NumBackgroundWorker; i++ {
		bgWorker, err := newBGWorker(client, option.ChannelBufferSize, option.MinCollectInterval)
		if err != nil {
			for _, w := range cli.workers {
				err = errors.Append(err, w.close())
			}
			return nil, err
		}
		cli.workers = append(cli.workers, bgWorker)
	}
	return cli, nil
}
