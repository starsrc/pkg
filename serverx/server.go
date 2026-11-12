package serverx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/fx"
)

// Config 定义 HTTP 服务构造与平滑停机所需的通用配置契约。
type Config interface {
	// ReadHeaderTimeout 返回读取请求头超时时间。
	ReadHeaderTimeout() time.Duration
	// ShutdownTimeout 返回 HTTP 服务专属的停机预算上限。
	ShutdownTimeout() time.Duration
	// ReadMaxBytes 返回读取请求体的最大字节数限制。
	ReadMaxBytes() int64
}

// Listener 定义被生命周期托管的命名 HTTP 服务监听器。
type Listener struct {
	Name   string
	Server *http.Server
}

// NewHTTPServer 构建标准 HTTP/1.1 + h2c (未加密 HTTP/2) 服务实例，
// 默认挂载 MaxBytesHandler 防止超大请求体 DoS，并配置合理的请求头读取超时。
func NewHTTPServer(addr string, handler http.Handler, cfg Config) *http.Server {
	readHeaderTimeout := 10 * time.Second
	var readMaxBytes int64 = 128 << 10 // 128KB
	if cfg != nil {
		if t := cfg.ReadHeaderTimeout(); t > 0 {
			readHeaderTimeout = t
		}
		if m := cfg.ReadMaxBytes(); m > 0 {
			readMaxBytes = m
		}
	}

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	return &http.Server{
		Addr:              addr,
		Handler:           http.MaxBytesHandler(handler, readMaxBytes),
		Protocols:         &protocols,
		ReadHeaderTimeout: readHeaderTimeout,
	}
}

// RegisterSingleLifecycle 为单个 HTTP 服务快速注册 Fx 启停生命周期钩子。
func RegisterSingleLifecycle(lc fx.Lifecycle, shutdowner fx.Shutdowner, cfg Config, name string, srv *http.Server) {
	RegisterLifecycle(lc, shutdowner, cfg, Listener{Name: name, Server: srv})
}

// RegisterLifecycle 为一个或多个命名 HTTP 服务监听器注册统一的 Fx 启停生命周期钩子：
// - OnStart：并行启动各个监听器，若发生非预期崩溃或端口冲突，自动级联调用 shutdowner.Shutdown(fx.ExitCode(1))；
// - OnStop：以 cfg.ShutdownTimeout() 为预算并发对所有服务执行 Shutdown；若超时则优雅降级为 Close()，并汇总返回所有错误。
func RegisterLifecycle(lc fx.Lifecycle, shutdowner fx.Shutdowner, cfg Config, listeners ...Listener) {
	if lc == nil || shutdowner == nil {
		return
	}

	var validListeners []Listener
	for _, l := range listeners {
		if l.Server != nil {
			validListeners = append(validListeners, l)
		}
	}
	if len(validListeners) == 0 {
		return
	}

	var stopping atomic.Bool
	var serveErr error
	var serveMu sync.Mutex
	serveDone := make(chan struct{})

	recordErr := func(err error) {
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return
		}
		serveMu.Lock()
		serveErr = errors.Join(serveErr, err)
		serveMu.Unlock()
	}

	var triggerShutdown sync.Once
	doShutdown := func() {
		if !stopping.Load() {
			triggerShutdown.Do(func() {
				var opts []fx.ShutdownOption
				serveMu.Lock()
				if serveErr != nil {
					opts = append(opts, fx.ExitCode(1))
				}
				serveMu.Unlock()
				if shutErr := shutdowner.Shutdown(opts...); shutErr != nil {
					recordErr(fmt.Errorf("request application shutdown: %w", shutErr))
				}
			})
		}
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			var wg sync.WaitGroup
			wg.Add(len(validListeners))

			for _, l := range validListeners {
				go func(item Listener) {
					defer wg.Done()
					if err := item.Server.ListenAndServe(); err != nil {
						name := item.Name
						if name == "" {
							name = "server"
						}
						recordErr(fmt.Errorf("%s error: %w", name, err))
					}
					doShutdown()
				}(l)
			}

			go func() {
				wg.Wait()
				close(serveDone)
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			stopping.Store(true)

			shutdownTimeout := 7 * time.Second
			if cfg != nil && cfg.ShutdownTimeout() > 0 {
				shutdownTimeout = cfg.ShutdownTimeout()
			}

			shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
			defer cancel()

			var shutdownWg sync.WaitGroup
			var shutdownMu sync.Mutex
			var shutdownErr error

			recordShutdownErr := func(err error) {
				if err == nil {
					return
				}
				shutdownMu.Lock()
				shutdownErr = errors.Join(shutdownErr, err)
				shutdownMu.Unlock()
			}

			shutdownWg.Add(len(validListeners))
			for _, l := range validListeners {
				go func(item Listener) {
					defer shutdownWg.Done()
					name := item.Name
					if name == "" {
						name = "server"
					}
					if err := item.Server.Shutdown(shutdownCtx); err != nil {
						err1 := fmt.Errorf("%s shutdown: %w", name, err)
						if closeErr := item.Server.Close(); closeErr != nil {
							err1 = errors.Join(err1, fmt.Errorf("%s close: %w", name, closeErr))
						}
						recordShutdownErr(err1)
					}
				}(l)
			}

			shutdownWg.Wait()
			<-serveDone

			serveMu.Lock()
			finalServeErr := serveErr
			serveMu.Unlock()

			return errors.Join(finalServeErr, shutdownErr)
		},
	})
}
