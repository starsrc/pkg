package serverx_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/serverx"
)

type dummyServerConfig struct {
	readHeaderTimeout time.Duration
	shutdownTimeout   time.Duration
	readMaxBytes      int64
}

func (d dummyServerConfig) ReadHeaderTimeout() time.Duration { return d.readHeaderTimeout }
func (d dummyServerConfig) ShutdownTimeout() time.Duration   { return d.shutdownTimeout }
func (d dummyServerConfig) ReadMaxBytes() int64              { return d.readMaxBytes }

type testLifecycle struct {
	hooks []fx.Hook
}

func (l *testLifecycle) Append(hook fx.Hook) {
	l.hooks = append(l.hooks, hook)
}

func (l *testLifecycle) start(ctx context.Context) error {
	for _, hook := range l.hooks {
		if hook.OnStart != nil {
			if err := hook.OnStart(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *testLifecycle) stop(ctx context.Context) error {
	var errs error
	for i := len(l.hooks) - 1; i >= 0; i-- {
		if l.hooks[i].OnStop != nil {
			if err := l.hooks[i].OnStop(ctx); err != nil {
				errs = errors.Join(errs, err)
			}
		}
	}
	return errs
}

type testShutdowner struct {
	mu           sync.Mutex
	shutdownDone chan struct{}
	opts         []fx.ShutdownOption
	callCount    int64
	err          error
}

func newTestShutdowner(err error) *testShutdowner {
	return &testShutdowner{
		shutdownDone: make(chan struct{}, 1),
		err:          err,
	}
}

func (s *testShutdowner) Shutdown(opts ...fx.ShutdownOption) error {
	atomic.AddInt64(&s.callCount, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opts = opts
	select {
	case s.shutdownDone <- struct{}{}:
	default:
	}
	return s.err
}

func (s *testShutdowner) count() int64 {
	return atomic.LoadInt64(&s.callCount)
}

func allocateFreePort(t *testing.T) (int, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port, addr
}

func assertPortListening(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("port at %s is not listening within deadline", addr)
}

func assertPortClosed(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("port at %s did not close within deadline", addr)
}

func TestNewHTTPServer(t *testing.T) {
	t.Run("默认配置与h2c协议构造", func(t *testing.T) {
		dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("ok"))
		})
		srv := serverx.NewHTTPServer("127.0.0.1:8080", dummyHandler, nil)

		require.NotNil(t, srv)
		assert.Equal(t, "127.0.0.1:8080", srv.Addr)
		assert.Equal(t, 10*time.Second, srv.ReadHeaderTimeout)
		require.NotNil(t, srv.Protocols)
		assert.True(t, srv.Protocols.HTTP1())
		assert.True(t, srv.Protocols.UnencryptedHTTP2())
	})

	t.Run("MaxBytesHandler拦截超限请求体", func(t *testing.T) {
		_, addr := allocateFreePort(t)
		cfg := dummyServerConfig{
			readHeaderTimeout: 5 * time.Second,
			shutdownTimeout:   5 * time.Second,
			readMaxBytes:      16, // 最大 16 字节
		}

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
		})

		srv := serverx.NewHTTPServer(addr, handler, cfg)
		lc := &testLifecycle{}
		sd := newTestShutdowner(nil)
		serverx.RegisterSingleLifecycle(lc, sd, cfg, "test-server", srv)

		startCtx, cancelStart := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStart()
		require.NoError(t, lc.start(startCtx))
		t.Cleanup(func() {
			stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelStop()
			_ = lc.stop(stopCtx)
		})
		assertPortListening(t, addr)

		client := &http.Client{Timeout: 3 * time.Second}

		// 1. 发送符合限制的请求体
		respOK, err := client.Post("http://"+addr, "text/plain", strings.NewReader("short payload"))
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, respOK.StatusCode)
		_ = respOK.Body.Close()

		// 2. 发送超出限制的请求体（> 16 字节）
		respTooLarge, err := client.Post("http://"+addr, "text/plain", strings.NewReader("this payload definitely exceeds sixteen bytes"))
		require.NoError(t, err)
		assert.Equal(t, http.StatusRequestEntityTooLarge, respTooLarge.StatusCode)
		_ = respTooLarge.Body.Close()
	})
}

func TestRegisterLifecycle_SingleAndDualListeners(t *testing.T) {
	t.Run("单监听器正常平滑启停并在预算内排空在途请求", func(t *testing.T) {
		_, addr := allocateFreePort(t)
		cfg := serverx.ServerConfig{
			ServerStopTimeout: 5 * time.Second,
		}

		handlerEntered := make(chan struct{})
		releaseHandler := make(chan struct{})
		shutdownStarted := make(chan struct{})
		srv := serverx.NewHTTPServer(addr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(handlerEntered)
			<-releaseHandler
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("drained"))
		}), cfg)
		srv.RegisterOnShutdown(func() {
			close(shutdownStarted)
		})

		lc := &testLifecycle{}
		sd := newTestShutdowner(nil)
		serverx.RegisterSingleLifecycle(lc, sd, cfg, "single-server", srv)

		startCtx, cancelStart := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStart()
		require.NoError(t, lc.start(startCtx))
		stopped := false
		t.Cleanup(func() {
			if !stopped {
				select {
				case <-releaseHandler:
				default:
					close(releaseHandler)
				}
				stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelStop()
				_ = lc.stop(stopCtx)
			}
		})

		assertPortListening(t, addr)

		type clientResult struct {
			statusCode int
			body       string
			err        error
		}
		resCh := make(chan clientResult, 1)

		go func() {
			resp, err := http.Get("http://" + addr + "/drain-test")
			if err != nil {
				resCh <- clientResult{err: err}
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			resCh <- clientResult{statusCode: resp.StatusCode, body: string(body)}
		}()

		select {
		case <-handlerEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("请求未在超时内进入 Handler")
		}

		stopDone := make(chan error, 1)
		go func() {
			stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelStop()
			stopDone <- lc.stop(stopCtx)
		}()

		select {
		case <-shutdownStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("http.Server.Shutdown 未在超时内启动")
		}

		// 断言在释放 Handler 之前，lc.stop 确实处于阻塞等待在途 Handler 排空的状态
		select {
		case err := <-stopDone:
			t.Fatalf("在释放 Handler 前 lc.stop 已提前返回: %v", err)
		default:
		}

		close(releaseHandler)

		select {
		case res := <-resCh:
			require.NoError(t, res.err)
			assert.Equal(t, http.StatusOK, res.statusCode)
			assert.Equal(t, "drained", res.body)
		case <-time.After(3 * time.Second):
			t.Fatal("客户端等待响应超时")
		}

		select {
		case stopErr := <-stopDone:
			require.NoError(t, stopErr, "在途请求正常排空后 lc.stop 应返回 nil")
		case <-time.After(3 * time.Second):
			t.Fatal("lc.stop 停机超时")
		}
		stopped = true

		assertPortClosed(t, addr)
	})

	t.Run("双监听器正常平滑启停", func(t *testing.T) {
		_, addr1 := allocateFreePort(t)
		_, addr2 := allocateFreePort(t)

		cfg := serverx.ServerConfig{
			ServerStopTimeout: 5 * time.Second,
		}

		handlerEntered1 := make(chan struct{})
		handlerEntered2 := make(chan struct{})
		releaseHandler1 := make(chan struct{})
		releaseHandler2 := make(chan struct{})
		shutdownStarted1 := make(chan struct{})
		shutdownStarted2 := make(chan struct{})

		var (
			enteredOnce1  sync.Once
			enteredOnce2  sync.Once
			shutdownOnce1 sync.Once
			shutdownOnce2 sync.Once
		)

		srv1 := serverx.NewHTTPServer(addr1, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			enteredOnce1.Do(func() { close(handlerEntered1) })
			<-releaseHandler1
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("drained-1"))
		}), cfg)
		srv1.RegisterOnShutdown(func() {
			shutdownOnce1.Do(func() { close(shutdownStarted1) })
		})

		srv2 := serverx.NewHTTPServer(addr2, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			enteredOnce2.Do(func() { close(handlerEntered2) })
			<-releaseHandler2
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("drained-2"))
		}), cfg)
		srv2.RegisterOnShutdown(func() {
			shutdownOnce2.Do(func() { close(shutdownStarted2) })
		})

		lc := &testLifecycle{}
		sd := newTestShutdowner(nil)

		serverx.RegisterLifecycle(lc, sd, cfg,
			serverx.Listener{Name: "public-server", Server: srv1},
			serverx.Listener{Name: "internal-server", Server: srv2},
		)

		startCtx, cancelStart := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStart()
		require.NoError(t, lc.start(startCtx))
		stopped := false
		t.Cleanup(func() {
			if !stopped {
				select {
				case <-releaseHandler1:
				default:
					close(releaseHandler1)
				}
				select {
				case <-releaseHandler2:
				default:
					close(releaseHandler2)
				}
				stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelStop()
				_ = lc.stop(stopCtx)
			}
		})

		assertPortListening(t, addr1)
		assertPortListening(t, addr2)

		type clientResult struct {
			statusCode int
			body       string
			err        error
		}
		resCh1 := make(chan clientResult, 1)
		resCh2 := make(chan clientResult, 1)

		go func() {
			resp, err := http.Get("http://" + addr1 + "/drain-1")
			if err != nil {
				resCh1 <- clientResult{err: err}
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			resCh1 <- clientResult{statusCode: resp.StatusCode, body: string(body)}
		}()

		go func() {
			resp, err := http.Get("http://" + addr2 + "/drain-2")
			if err != nil {
				resCh2 <- clientResult{err: err}
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			resCh2 <- clientResult{statusCode: resp.StatusCode, body: string(body)}
		}()

		select {
		case <-handlerEntered1:
		case <-time.After(2 * time.Second):
			t.Fatal("请求 1 未在超时内进入 Handler")
		}

		select {
		case <-handlerEntered2:
		case <-time.After(2 * time.Second):
			t.Fatal("请求 2 未在超时内进入 Handler")
		}

		stopDone := make(chan error, 1)
		go func() {
			stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelStop()
			stopDone <- lc.stop(stopCtx)
		}()

		select {
		case <-shutdownStarted1:
		case <-time.After(2 * time.Second):
			t.Fatal("srv1 未在超时内启动 Shutdown")
		}

		select {
		case <-shutdownStarted2:
		case <-time.After(2 * time.Second):
			t.Fatal("srv2 未在超时内启动 Shutdown (必须并发调用 Shutdown)")
		}

		// 断言在释放 Handler 之前，lc.stop 尚未提前返回
		select {
		case err := <-stopDone:
			t.Fatalf("在释放 Handler 前 lc.stop 已提前返回: %v", err)
		default:
		}

		close(releaseHandler1)
		close(releaseHandler2)

		select {
		case res := <-resCh1:
			require.NoError(t, res.err)
			assert.Equal(t, http.StatusOK, res.statusCode)
			assert.Equal(t, "drained-1", res.body)
		case <-time.After(3 * time.Second):
			t.Fatal("客户端 1 等待响应超时")
		}

		select {
		case res := <-resCh2:
			require.NoError(t, res.err)
			assert.Equal(t, http.StatusOK, res.statusCode)
			assert.Equal(t, "drained-2", res.body)
		case <-time.After(3 * time.Second):
			t.Fatal("客户端 2 等待响应超时")
		}

		select {
		case stopErr := <-stopDone:
			require.NoError(t, stopErr, "在途请求正常排空后 lc.stop 应返回 nil")
		case <-time.After(3 * time.Second):
			t.Fatal("lc.stop 停机超时")
		}
		stopped = true

		assertPortClosed(t, addr1)
		assertPortClosed(t, addr2)
	})
}

func TestRegisterLifecycle_ListenErrorAndShutdowner(t *testing.T) {
	t.Run("单端口冲突监听失败自动触发应用退出且返回错误", func(t *testing.T) {
		occupiedListener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer occupiedListener.Close()

		occupiedAddr := occupiedListener.Addr().String()

		cfg := serverx.ServerConfig{
			ServerStopTimeout: 5 * time.Second,
		}
		srv := serverx.NewHTTPServer(occupiedAddr, http.NotFoundHandler(), cfg)

		app := fx.New(
			fx.NopLogger,
			fx.Invoke(func(lc fx.Lifecycle, sd fx.Shutdowner) {
				serverx.RegisterSingleLifecycle(lc, sd, cfg, "conflict-server", srv)
			}),
		)

		startCtx, cancelStart := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStart()
		require.NoError(t, app.Start(startCtx))
		stopped := false
		t.Cleanup(func() {
			if !stopped {
				stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelStop()
				_ = app.Stop(stopCtx)
			}
		})

		var sig fx.ShutdownSignal
		select {
		case sig = <-app.Wait():
		case <-time.After(1 * time.Second):
			t.Fatal("未在规定时间内调用 shutdowner.Shutdown")
		}

		assert.Equal(t, 1, sig.ExitCode)

		stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStop()
		stopErr := app.Stop(stopCtx)
		stopped = true
		require.Error(t, stopErr)
		assert.Contains(t, stopErr.Error(), "conflict-server error")
	})

	t.Run("双端口同时冲突仅触发一次应用退出", func(t *testing.T) {
		l1, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer l1.Close()

		l2, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer l2.Close()

		cfg := serverx.ServerConfig{
			ServerStopTimeout: 5 * time.Second,
		}
		srv1 := serverx.NewHTTPServer(l1.Addr().String(), http.NotFoundHandler(), cfg)
		srv2 := serverx.NewHTTPServer(l2.Addr().String(), http.NotFoundHandler(), cfg)

		lc := &testLifecycle{}
		sd := newTestShutdowner(nil)
		serverx.RegisterLifecycle(lc, sd, cfg,
			serverx.Listener{Name: "server-one", Server: srv1},
			serverx.Listener{Name: "server-two", Server: srv2},
		)

		startCtx, cancelStart := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStart()
		require.NoError(t, lc.start(startCtx))
		stopped := false
		t.Cleanup(func() {
			if !stopped {
				stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelStop()
				_ = lc.stop(stopCtx)
			}
		})

		select {
		case <-sd.shutdownDone:
		case <-time.After(1 * time.Second):
			t.Fatal("未在规定时间内调用 shutdowner.Shutdown")
		}

		// 等待两个监听器 goroutine 的失败处理路径均已执行完毕，
		// 确保在尚未调用 lc.stop（stopping 仍为 false）的前提下，
		// 即使没有 sync.Once 保护，第二个 goroutine 也有充足机会调用 shutdowner。
		time.Sleep(100 * time.Millisecond)

		// 确保 Shutdown 仅被调用一次（sync.Once 保证）
		assert.Equal(t, int64(1), sd.count())

		stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStop()
		stopErr := lc.stop(stopCtx)
		stopped = true
		require.Error(t, stopErr)
		assert.Contains(t, stopErr.Error(), "server-one error")
		assert.Contains(t, stopErr.Error(), "server-two error")
	})

	t.Run("应用停机通知返回错误时完整合并错误信息", func(t *testing.T) {
		occupiedListener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer occupiedListener.Close()

		cfg := serverx.ServerConfig{
			ServerStopTimeout: 5 * time.Second,
		}
		srv := serverx.NewHTTPServer(occupiedListener.Addr().String(), http.NotFoundHandler(), cfg)

		lc := &testLifecycle{}
		shutdownErr := errors.New("cannot initiate graceful shutdown: permission denied")
		sd := newTestShutdowner(shutdownErr)
		serverx.RegisterSingleLifecycle(lc, sd, cfg, "failing-shutdown-server", srv)

		startCtx, cancelStart := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStart()
		require.NoError(t, lc.start(startCtx))
		stopped := false
		t.Cleanup(func() {
			if !stopped {
				stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelStop()
				_ = lc.stop(stopCtx)
			}
		})

		select {
		case <-sd.shutdownDone:
		case <-time.After(1 * time.Second):
			t.Fatal("未在规定时间内调用 shutdowner.Shutdown")
		}

		stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStop()
		stopErr := lc.stop(stopCtx)
		stopped = true
		require.Error(t, stopErr)
		assert.Contains(t, stopErr.Error(), "failing-shutdown-server error")
		assert.Contains(t, stopErr.Error(), "request application shutdown: cannot initiate graceful shutdown: permission denied")
	})
}

func TestRegisterLifecycle_ShutdownTimeoutFallbackClose(t *testing.T) {
	t.Run("停机超时触发降级Close且合并错误", func(t *testing.T) {
		_, addr := allocateFreePort(t)
		cfg := dummyServerConfig{
			readHeaderTimeout: 5 * time.Second,
			shutdownTimeout:   50 * time.Millisecond, // 极短停机预算
			readMaxBytes:      1024,
		}

		handlerStarted := make(chan struct{})
		releaseHandler := make(chan struct{})
		handlerDone := make(chan struct{})

		srv := serverx.NewHTTPServer(addr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(handlerStarted)
			defer close(handlerDone)
			select {
			case <-releaseHandler:
				_, _ = w.Write([]byte("delayed"))
			case <-r.Context().Done():
			}
		}), cfg)

		lc := &testLifecycle{}
		sd := newTestShutdowner(nil)
		serverx.RegisterSingleLifecycle(lc, sd, cfg, "slow-server", srv)

		startCtx, cancelStart := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStart()
		require.NoError(t, lc.start(startCtx))
		stopped := false
		t.Cleanup(func() {
			if !stopped {
				stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelStop()
				_ = lc.stop(stopCtx)
			}
		})
		assertPortListening(t, addr)

		// 发起一个阻塞请求，通过缓冲通道收集客户端错误
		reqErrCh := make(chan error, 1)
		go func() {
			resp, err := http.Get("http://" + addr)
			if err == nil {
				_ = resp.Body.Close()
			}
			reqErrCh <- err
		}()

		select {
		case <-handlerStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("handler 未及时启动")
		}

		// 触发停止：由于存在挂起请求且 shutdownTimeout 仅 50ms，Shutdown 将超时并降级调用 Close()
		stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStop()
		stopErr := lc.stop(stopCtx)
		stopped = true

		require.Error(t, stopErr)
		assert.Contains(t, stopErr.Error(), "slow-server shutdown")
		assert.Contains(t, stopErr.Error(), context.DeadlineExceeded.Error())
		assertPortClosed(t, addr)

		// 在关闭 releaseHandler 之前，断言客户端已经由于服务端强制 Close() 收到连接错误
		select {
		case reqErr := <-reqErrCh:
			require.Error(t, reqErr, "服务端 Close() 必须强制中断活跃客户端请求")
		case <-time.After(1 * time.Second):
			t.Fatal("超时：服务端 Close() 后客户端请求未被强制切断")
		}

		close(releaseHandler)
		select {
		case <-handlerDone:
		case <-time.After(1 * time.Second):
			t.Fatal("handler 未及时退出")
		}
	})

	t.Run("双监听器同时停机超时触发降级Close且合并错误", func(t *testing.T) {
		_, addr1 := allocateFreePort(t)
		_, addr2 := allocateFreePort(t)
		cfg := dummyServerConfig{
			readHeaderTimeout: 5 * time.Second,
			shutdownTimeout:   50 * time.Millisecond, // 极短停机预算
			readMaxBytes:      1024,
		}

		handlerStarted1 := make(chan struct{})
		releaseHandler1 := make(chan struct{})
		handlerDone1 := make(chan struct{})

		srv1 := serverx.NewHTTPServer(addr1, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(handlerStarted1)
			defer close(handlerDone1)
			select {
			case <-releaseHandler1:
				_, _ = w.Write([]byte("delayed-1"))
			case <-r.Context().Done():
			}
		}), cfg)

		handlerStarted2 := make(chan struct{})
		releaseHandler2 := make(chan struct{})
		handlerDone2 := make(chan struct{})

		srv2 := serverx.NewHTTPServer(addr2, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(handlerStarted2)
			defer close(handlerDone2)
			select {
			case <-releaseHandler2:
				_, _ = w.Write([]byte("delayed-2"))
			case <-r.Context().Done():
			}
		}), cfg)

		lc := &testLifecycle{}
		sd := newTestShutdowner(nil)
		serverx.RegisterLifecycle(lc, sd, cfg,
			serverx.Listener{Name: "slow-http-1", Server: srv1},
			serverx.Listener{Name: "slow-http-2", Server: srv2},
		)

		startCtx, cancelStart := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStart()
		require.NoError(t, lc.start(startCtx))
		stopped := false
		t.Cleanup(func() {
			if !stopped {
				select {
				case <-releaseHandler1:
				default:
					close(releaseHandler1)
				}
				select {
				case <-releaseHandler2:
				default:
					close(releaseHandler2)
				}
				stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelStop()
				_ = lc.stop(stopCtx)
			}
		})
		assertPortListening(t, addr1)
		assertPortListening(t, addr2)

		reqErrCh1 := make(chan error, 1)
		go func() {
			resp, err := http.Get("http://" + addr1)
			if err == nil {
				_ = resp.Body.Close()
			}
			reqErrCh1 <- err
		}()

		reqErrCh2 := make(chan error, 1)
		go func() {
			resp, err := http.Get("http://" + addr2)
			if err == nil {
				_ = resp.Body.Close()
			}
			reqErrCh2 <- err
		}()

		select {
		case <-handlerStarted1:
		case <-time.After(2 * time.Second):
			t.Fatal("handler 1 未及时启动")
		}

		select {
		case <-handlerStarted2:
		case <-time.After(2 * time.Second):
			t.Fatal("handler 2 未及时启动")
		}

		stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStop()
		stopErr := lc.stop(stopCtx)
		stopped = true

		require.Error(t, stopErr)
		assert.Contains(t, stopErr.Error(), "slow-http-1 shutdown")
		assert.Contains(t, stopErr.Error(), "slow-http-2 shutdown")
		assert.Contains(t, stopErr.Error(), context.DeadlineExceeded.Error())
		assertPortClosed(t, addr1)
		assertPortClosed(t, addr2)

		select {
		case reqErr := <-reqErrCh1:
			require.Error(t, reqErr, "服务端 1 Close() 必须强制中断活跃客户端请求")
		case <-time.After(1 * time.Second):
			t.Fatal("超时：服务端 1 Close() 后客户端请求未被强制切断")
		}

		select {
		case reqErr := <-reqErrCh2:
			require.Error(t, reqErr, "服务端 2 Close() 必须强制中断活跃客户端请求")
		case <-time.After(1 * time.Second):
			t.Fatal("超时：服务端 2 Close() 后客户端请求未被强制切断")
		}

		close(releaseHandler1)
		close(releaseHandler2)
		select {
		case <-handlerDone1:
		case <-time.After(1 * time.Second):
			t.Fatal("handler 1 未及时退出")
		}
		select {
		case <-handlerDone2:
		case <-time.After(1 * time.Second):
			t.Fatal("handler 2 未及时退出")
		}
	})
}

func TestRegisterLifecycle_DefensiveInputs(t *testing.T) {
	cfg := serverx.ServerConfig{}

	t.Run("Lifecycle或Shutdowner为nil时安全跳过", func(t *testing.T) {
		assert.NotPanics(t, func() {
			serverx.RegisterLifecycle(nil, nil, cfg)
			serverx.RegisterLifecycle(&testLifecycle{}, nil, cfg)
			serverx.RegisterLifecycle(nil, newTestShutdowner(nil), cfg)
		})
	})

	t.Run("listeners为空或全nil时安全跳过", func(t *testing.T) {
		lc := &testLifecycle{}
		sd := newTestShutdowner(nil)
		serverx.RegisterLifecycle(lc, sd, cfg)
		serverx.RegisterLifecycle(lc, sd, cfg, serverx.Listener{Name: "nil-srv", Server: nil})
		assert.Empty(t, lc.hooks)
	})
}
