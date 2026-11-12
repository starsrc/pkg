package obsfx

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability"
)

// Ready 为 observability.Ready 的类型别名，标记可观测性底座已在 Fx 容器中完成初始化。
type Ready = observability.Ready

// Module 是服务端唯一的 Fx 可观测性薄装配模块。
// 提供 *observability.Runtime、*slog.Logger、*prometheus.Registry、prometheus.Gatherer 与 observability.Ready，
// 并将 rt.Shutdown 绑定至 Fx OnStop 钩子。
// 注意：严禁向 Fx 容器注入任何 connect.Interceptor（统一收敛至 serverx.NewInterceptors*）。
var Module = fx.Module("observability",
	fx.Provide(
		ProvideRuntime,
		ProvideLogger,
		ProvideRegistry,
		ProvideGatherer,
	),
)

// Params 定义初始化可观测性 Runtime 所需的 Fx 依赖注入参数。
type Params struct {
	fx.In

	Ctx        context.Context `optional:"true"`
	Lifecycle  fx.Lifecycle
	Config     observability.Config
	Options    []observability.Option `group:"observability_options"`
	Collectors []prometheus.Collector `group:"prometheus_collectors"`
}

// ProvideRuntime 初始化 *observability.Runtime 并将其优雅停机注册至 Fx Lifecycle。
func ProvideRuntime(p Params) (*observability.Runtime, observability.Ready, error) {
	ctx := p.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	opts := make([]observability.Option, 0, len(p.Options)+1)
	opts = append(opts, p.Options...)
	if len(p.Collectors) > 0 {
		opts = append(opts, observability.WithCollectors(p.Collectors...))
	}

	rt, err := observability.Setup(ctx, p.Config, opts...)
	if err != nil {
		return nil, observability.Ready{}, fmt.Errorf("obsfx: setup observability runtime: %w", err)
	}

	if p.Lifecycle != nil {
		p.Lifecycle.Append(fx.Hook{
			OnStop: rt.Shutdown,
		})
	}

	return rt, observability.Ready{}, nil
}

// ProvideLogger 从 Runtime 导出结构化 *slog.Logger。
func ProvideLogger(rt *observability.Runtime) *slog.Logger {
	return rt.Logger()
}

// ProvideRegistry 从 Runtime 导出实例级隔离的 *prometheus.Registry。
func ProvideRegistry(rt *observability.Runtime) *prometheus.Registry {
	return rt.Registry()
}

// ProvideGatherer 从 Runtime 导出 prometheus.Gatherer。
func ProvideGatherer(rt *observability.Runtime) prometheus.Gatherer {
	return rt.Gatherer()
}

// AsOption 将返回 observability.Option 的构造函数标注至 "observability_options" Value Group。
func AsOption(fn any) any {
	return fx.Annotate(
		fn,
		fx.As(new(observability.Option)),
		fx.ResultTags(`group:"observability_options"`),
	)
}

// AsCollector 将返回 prometheus.Collector 的构造函数标注至 "prometheus_collectors" Value Group。
func AsCollector(fn any) any {
	return fx.Annotate(
		fn,
		fx.As(new(prometheus.Collector)),
		fx.ResultTags(`group:"prometheus_collectors"`),
	)
}
