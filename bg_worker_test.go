package piperedis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

func TestBGWorker(t *testing.T) {
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
	worker, err := newBGWorker(c, 16, time.Millisecond, 1024)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, worker.close())
	}()
	require.NoError(t, worker.do(context.Background(), "set", "a", "b").Err())

	const kConcurrency = 1024
	var complete sync.WaitGroup
	complete.Add(kConcurrency)

	for i := 0; i < kConcurrency; i++ {
		go func(drop bool) {
			defer complete.Done()
			ctx, cancel := context.WithCancel(context.Background()) // nolint: govet
			if drop {
				cancel()
			}
			cmd := worker.do(ctx, "get", "a")
			if !drop {
				cancel()
			}
			if drop {
				require.Error(t, cmd.Err())
				return // nolint: govet
			}
			require.NoError(t, cmd.Err())
			txt, err := cmd.Text()
			require.NoError(t, err)
			require.Equal(t, "b", txt)
		}(i%3 == 0)
	}

	complete.Wait()
}
