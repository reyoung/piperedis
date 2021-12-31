package piperedis

import (
	"context"
	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	r, err := miniredis.Run()
	require.NoError(t, err)
	defer func() {
		r.Close()
	}()
	c := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{r.Addr()},
	})
	defer func() {
		require.NoError(t, c.Close())
	}()

	require.NoError(t, c.Ping(context.Background()).Err())
	worker, err := New(c, Option{
		NumBackgroundWorker: 2,
		ChannelBufferSize:   16,
		MinCollectInterval:  time.Millisecond,
	})
	defer func() {
		require.NoError(t, worker.Close())
	}()
	require.NoError(t, worker.Do(context.Background(), "set", "a", "b").Err())

	const kConcurrency = 1024
	var complete sync.WaitGroup
	complete.Add(kConcurrency)

	for i := 0; i < kConcurrency; i++ {
		go func(drop bool) {
			defer complete.Done()
			ctx, cancel := context.WithCancel(context.Background())
			if drop {
				cancel()
			}
			cmd := worker.Do(ctx, "get", "a")
			if !drop {
				cancel()
			}
			if drop {
				require.Error(t, cmd.Err())
				return
			}
			require.NoError(t, cmd.Err())
			txt, err := cmd.Text()
			require.NoError(t, err)
			require.Equal(t, "b", txt)
		}(i%3 == 0)
	}

	complete.Wait()
}
