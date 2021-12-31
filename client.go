package piperedis

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

type Client struct {
	cli redis.UniversalClient
}

func (c *Client) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	panic("not implement")
}

func (c *Client) Close() error {
	panic("not implement")
}

const (
	kDefaultChannelBufferSize = 256
	kDefaultBatchInterval     = time.Millisecond
)

type Option struct {
	NumBackgroundWorker int
	ChannelBufferSize   int
	MinCollectInterval  time.Duration
}

func New(client redis.UniversalClient, option Option) (*Client, error) {
	panic("not implement")
}
