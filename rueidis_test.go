package echoratelimiter

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/time/rate"
)

func TestItLimitsConnections(t *testing.T) {
	server, url, cancel := server()
	defer cancel()

	server.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	}, defaultLimiter(t, 5, time.Second))

	do := client(url, 10)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	results := map[int]int{}
	lock := &sync.Mutex{}

	stop := make(chan struct{})
	for range runtime.NumCPU() {
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					code, err := do(ctx)
					if err != nil {
						continue
					}
					lock.Lock()
					results[code]++
					lock.Unlock()
				}
			}
		}()
	}

	time.Sleep(time.Second * 5)
	stop <- struct{}{}

	total := 0
	for _, val := range results {
		total += val
	}
	require.Equal(t, 50, total)
	require.Equal(t, 25, results[200])
	require.Equal(t, 25, results[429])
}

func client(url string, limit rate.Limit) func(context.Context) (int, error) {
	limiter := rate.NewLimiter(limit, 1)
	return func(ctx context.Context) (int, error) {
		if err := limiter.Wait(ctx); err != nil {
			return 0, err
		}
		resp, err := http.Get(url)
		if err != nil {
			return 0, err
		}
		return resp.StatusCode, nil
	}
}

func defaultLimiter(t *testing.T, rate rate.Limit, window time.Duration) echo.MiddlewareFunc {
	return middleware.RateLimiterWithConfig(middleware.RateLimiterConfig{
		Skipper: middleware.DefaultSkipper,
		IdentifierExtractor: func(c echo.Context) (string, error) {
			return c.RealIP(), nil
		},
		ErrorHandler: func(c echo.Context, err error) error {
			t.Log(err)
			return c.NoContent(http.StatusServiceUnavailable)
		},
		DenyHandler: func(c echo.Context, _ string, err error) error {
			return c.NoContent(http.StatusTooManyRequests)
		},
		Store: NewRueidisRateLimiter(context.Background(), RueidisRateLimiterConfig{
			Rate:   5,
			Window: time.Second,
			Client: redis(t),
			Logger: newSlogger(t),
		}),
	})
}

func server() (*echo.Echo, string, context.CancelFunc) {
	e := echo.New()
	srv := httptest.NewServer(e)
	return e, srv.URL, func() {
		e.Shutdown(context.Background())
	}
}

func redis(t *testing.T) rueidis.Client {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	redisCont, err := testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "ghcr.io/dragonflydb/dragonfly:latest",
				ExposedPorts: []string{"6379/tcp"},
				WaitingFor:   wait.ForListeningPort("6379/tcp"),
				Cmd: []string{
					"--proactor_threads=1",
					"--default_lua_flags=allow-undeclared-keys",
				},
			},
			Started: true,
			Logger:  testcontainers.TestLogger(t),
		},
	)
	require.Nil(t, err)
	redisHost, err := redisCont.Host(ctx)
	require.Nil(t, err)
	redisPort, err := redisCont.MappedPort(ctx, nat.Port("6379"))
	require.Nil(t, err)

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:   []string{fmt.Sprintf("%s:%d", redisHost, redisPort.Int())},
		MaxFlushDelay: time.Microsecond * 100,
	})
	require.Nil(t, err)
	return client
}

func newSlogger(t *testing.T) Logger {
	var buf bytes.Buffer
	slog := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	t.Cleanup(func() {
		t.Log(buf.String())
	})
	return slog
}
