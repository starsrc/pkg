package serverx

import (
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"
)

// ServerConfig 定义通用 ConnectRPC / HTTP 服务端运行配置。
type ServerConfig struct {
	ServerHost              string        `env:"SERVER_HOST" envDefault:"0.0.0.0"`
	ServerPort              int           `env:"SERVER_PORT" envDefault:"8080"`
	InternalServerHost      string        `env:"INTERNAL_SERVER_HOST" envDefault:"0.0.0.0"`
	InternalServerPort      int           `env:"INTERNAL_SERVER_PORT" envDefault:"8090"`
	ServerReadHeaderTimeout time.Duration `env:"SERVER_READ_HEADER_TIMEOUT" envDefault:"10s"`
	ServerStopTimeout       time.Duration `env:"SERVER_STOP_TIMEOUT" envDefault:"10s"`
	ServerReadMaxBytes      int64         `env:"SERVER_READ_MAX_BYTES" envDefault:"131072"`
}

// EnvConfig 作为 ServerConfig 的别名，便于环境变量配置复用。
type EnvConfig = ServerConfig

// ServerAddress 返回公共服务监听地址（例如 "0.0.0.0:8080"）。
func (c ServerConfig) ServerAddress() string {
	return net.JoinHostPort(c.ServerHost, strconv.Itoa(c.ServerPort))
}

// InternalServerAddress 返回内网服务监听地址（例如 "0.0.0.0:8090"）。
func (c ServerConfig) InternalServerAddress() string {
	return net.JoinHostPort(c.InternalServerHost, strconv.Itoa(c.InternalServerPort))
}

// Addr 返回公共服务监听地址（兼容旧调用签名）。
func (c ServerConfig) Addr() string {
	return c.ServerAddress()
}

// InternalAddr 返回内网服务监听地址（兼容旧调用签名）。
func (c ServerConfig) InternalAddr() string {
	return c.InternalServerAddress()
}

// Port 返回公共服务监听端口。
func (c ServerConfig) Port() int {
	return c.ServerPort
}

// ApplyDefaultPort 在 ServerPort 为 0 或处于默认 8080 端口且未显式指定 SERVER_PORT 环境变量时，
// 将其设置为指定的 defaultPort。若环境变量或配置已显式声明端口（包括显式设置为 0 或 8080），则保留原值。
func (c *ServerConfig) ApplyDefaultPort(defaultPort int) {
	if c == nil {
		return
	}
	envVal, envSet := os.LookupEnv("SERVER_PORT")
	if (!envSet || strings.TrimSpace(envVal) == "") && (c.ServerPort == 0 || c.ServerPort == 8080) {
		c.ServerPort = defaultPort
	}
}

// WithDefaultPort 返回应用了默认端口后的 ServerConfig 副本。
func (c ServerConfig) WithDefaultPort(defaultPort int) ServerConfig {
	c.ApplyDefaultPort(defaultPort)
	return c
}

// ReadHeaderTimeout 返回读取请求头超时时间，若未配置或非正则默认 10 秒。
func (c ServerConfig) ReadHeaderTimeout() time.Duration {
	if c.ServerReadHeaderTimeout <= 0 {
		return 10 * time.Second
	}
	return c.ServerReadHeaderTimeout
}

// StopTimeout 返回应用停机的全局总截止时间，若未配置或非正则默认 10 秒。
func (c ServerConfig) StopTimeout() time.Duration {
	if c.ServerStopTimeout <= 0 {
		return 10 * time.Second
	}
	return c.ServerStopTimeout
}

// ShutdownTimeout 返回 HTTP 服务专属的停机预算上限。
// 按 StopTimeout 的 70% 动态折算，预留 30% 剩余时间给下层遥测与连接释放。
func (c ServerConfig) ShutdownTimeout() time.Duration {
	return (c.StopTimeout() * 7) / 10
}

// ReadMaxBytes 返回读取请求体的最大字节数限制，若未配置或非正则默认 128KB (131072 字节)。
func (c ServerConfig) ReadMaxBytes() int64 {
	if c.ServerReadMaxBytes <= 0 {
		return 128 << 10
	}
	return c.ServerReadMaxBytes
}

// Validate 默认执行单端口合法性校验。
func (c ServerConfig) Validate() error {
	return c.ValidateSinglePort()
}

// ValidateSinglePort 校验公共服务端口范围是否在 1..65535 内。
func (c ServerConfig) ValidateSinglePort() error {
	if c.ServerPort < 1 || c.ServerPort > 65535 {
		return fmt.Errorf("invalid SERVER_PORT %d: must be between 1 and 65535", c.ServerPort)
	}
	return nil
}

// isHostConflict 判断两个主机地址在同一端口下是否存在监听冲突。
// 支持识别未指定通配地址（如 0.0.0.0、:: 等所有 IPv4/IPv6 写法）、
// IPv4 映射 IPv6 等价 IP、以及主机名字面量忽略大小写匹配。
func isHostConflict(h1, h2 string) bool {
	h1 = strings.Trim(strings.TrimSpace(h1), "[]")
	h2 = strings.Trim(strings.TrimSpace(h2), "[]")

	if h1 == "" || h2 == "" {
		return true
	}

	addr1, err1 := netip.ParseAddr(h1)
	addr2, err2 := netip.ParseAddr(h2)

	if (err1 == nil && addr1.IsUnspecified()) || (err2 == nil && addr2.IsUnspecified()) {
		return true
	}

	if err1 == nil && err2 == nil {
		return addr1.Unmap() == addr2.Unmap()
	}

	return strings.EqualFold(h1, h2)
}

// ValidateDualPorts 校验公共服务与内网服务双端口的合法性与无冲突性。
func (c ServerConfig) ValidateDualPorts() error {
	if c.ServerPort < 1 || c.ServerPort > 65535 {
		return fmt.Errorf("invalid SERVER_PORT %d: must be between 1 and 65535", c.ServerPort)
	}
	if c.InternalServerPort < 1 || c.InternalServerPort > 65535 {
		return fmt.Errorf("invalid INTERNAL_SERVER_PORT %d: must be between 1 and 65535", c.InternalServerPort)
	}

	if c.ServerPort == c.InternalServerPort && isHostConflict(c.ServerHost, c.InternalServerHost) {
		return fmt.Errorf("public and internal server addresses conflict: %s vs %s", c.ServerAddress(), c.InternalServerAddress())
	}

	return nil
}
