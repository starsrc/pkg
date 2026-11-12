package serverx_test

import (
	"os"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/pkg/serverx"
)

func TestServerConfig_Defaults(t *testing.T) {
	var cfg serverx.ServerConfig
	err := env.ParseWithOptions(&cfg, env.Options{
		Environment: map[string]string{},
	})
	require.NoError(t, err)

	assert.Equal(t, "0.0.0.0", cfg.ServerHost)
	assert.Equal(t, 8080, cfg.ServerPort)
	assert.Equal(t, "0.0.0.0", cfg.InternalServerHost)
	assert.Equal(t, 8090, cfg.InternalServerPort)
	assert.Equal(t, 10*time.Second, cfg.ServerReadHeaderTimeout)
	assert.Equal(t, 10*time.Second, cfg.ServerStopTimeout)
	assert.Equal(t, int64(131072), cfg.ServerReadMaxBytes)

	assert.Equal(t, "0.0.0.0:8080", cfg.ServerAddress())
	assert.Equal(t, "0.0.0.0:8090", cfg.InternalServerAddress())
	assert.Equal(t, "0.0.0.0:8080", cfg.Addr())
	assert.Equal(t, "0.0.0.0:8090", cfg.InternalAddr())

	assert.Equal(t, 10*time.Second, cfg.ReadHeaderTimeout())
	assert.Equal(t, 10*time.Second, cfg.StopTimeout())
	assert.Equal(t, 7*time.Second, cfg.ShutdownTimeout())
	assert.Equal(t, int64(131072), cfg.ReadMaxBytes())

	require.NoError(t, cfg.ValidateSinglePort())
	require.NoError(t, cfg.Validate())
	require.NoError(t, cfg.ValidateDualPorts())
}

func TestServerConfig_EnvParse(t *testing.T) {
	customEnv := map[string]string{
		"SERVER_HOST":                "127.0.0.1",
		"SERVER_PORT":                "9000",
		"INTERNAL_SERVER_HOST":       "127.0.0.1",
		"INTERNAL_SERVER_PORT":       "9001",
		"SERVER_READ_HEADER_TIMEOUT": "8s",
		"SERVER_STOP_TIMEOUT":        "20s",
		"SERVER_READ_MAX_BYTES":      "1048576",
	}

	var cfg serverx.ServerConfig
	err := env.ParseWithOptions(&cfg, env.Options{
		Environment: customEnv,
	})
	require.NoError(t, err)

	assert.Equal(t, "127.0.0.1", cfg.ServerHost)
	assert.Equal(t, 9000, cfg.ServerPort)
	assert.Equal(t, "127.0.0.1", cfg.InternalServerHost)
	assert.Equal(t, 9001, cfg.InternalServerPort)
	assert.Equal(t, "127.0.0.1:9000", cfg.ServerAddress())
	assert.Equal(t, "127.0.0.1:9001", cfg.InternalServerAddress())
	assert.Equal(t, 8*time.Second, cfg.ReadHeaderTimeout())
	assert.Equal(t, 20*time.Second, cfg.StopTimeout())
	assert.Equal(t, 14*time.Second, cfg.ShutdownTimeout()) // 20s * 7 / 10 = 14s
	assert.Equal(t, int64(1048576), cfg.ReadMaxBytes())

	require.NoError(t, cfg.ValidateDualPorts())
}

func TestServerConfig_Fallbacks(t *testing.T) {
	t.Run("零值与负值兜底验证", func(t *testing.T) {
		cfg := serverx.ServerConfig{
			ServerReadHeaderTimeout: -1 * time.Second,
			ServerStopTimeout:       0,
			ServerReadMaxBytes:      -100,
		}

		assert.Equal(t, 10*time.Second, cfg.ReadHeaderTimeout())
		assert.Equal(t, 10*time.Second, cfg.StopTimeout())
		assert.Equal(t, 7*time.Second, cfg.ShutdownTimeout())
		assert.Equal(t, int64(128<<10), cfg.ReadMaxBytes())
	})

	t.Run("显式正值超时折算70%", func(t *testing.T) {
		cfg := serverx.ServerConfig{
			ServerStopTimeout: 30 * time.Second,
		}
		assert.Equal(t, 21*time.Second, cfg.ShutdownTimeout())
	})
}

func TestServerConfig_ValidateSinglePort(t *testing.T) {
	tests := []struct {
		name    string
		port    int
		wantErr bool
	}{
		{name: "有效端口1", port: 1, wantErr: false},
		{name: "有效端口8080", port: 8080, wantErr: false},
		{name: "有效端口65535", port: 65535, wantErr: false},
		{name: "无效端口0", port: 0, wantErr: true},
		{name: "无效负数端口", port: -1, wantErr: true},
		{name: "超出最大端口65536", port: 65536, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := serverx.ServerConfig{ServerPort: tt.port}
			errSingle := cfg.ValidateSinglePort()
			errDefault := cfg.Validate()

			if tt.wantErr {
				require.Error(t, errSingle)
				assert.Contains(t, errSingle.Error(), "invalid SERVER_PORT")
				require.Error(t, errDefault)
				assert.Contains(t, errDefault.Error(), "invalid SERVER_PORT")
			} else {
				require.NoError(t, errSingle)
				require.NoError(t, errDefault)
			}
		})
	}
}

func TestServerConfig_ValidateDualPorts(t *testing.T) {
	tests := []struct {
		name         string
		host         string
		port         int
		internalHost string
		internalPort int
		wantErrMsg   string
	}{
		{
			name:         "正常独立端口",
			host:         "0.0.0.0",
			port:         8080,
			internalHost: "0.0.0.0",
			internalPort: 8090,
			wantErrMsg:   "",
		},
		{
			name:         "公共端口越界",
			host:         "0.0.0.0",
			port:         0,
			internalHost: "0.0.0.0",
			internalPort: 8090,
			wantErrMsg:   "invalid SERVER_PORT",
		},
		{
			name:         "内网端口越界",
			host:         "0.0.0.0",
			port:         8080,
			internalHost: "0.0.0.0",
			internalPort: 70000,
			wantErrMsg:   "invalid INTERNAL_SERVER_PORT",
		},
		{
			name:         "同Host同端口冲突",
			host:         "127.0.0.1",
			port:         8080,
			internalHost: "127.0.0.1",
			internalPort: 8080,
			wantErrMsg:   "conflict",
		},
		{
			name:         "通配Host与具体Host同端口冲突",
			host:         "0.0.0.0",
			port:         8080,
			internalHost: "127.0.0.1",
			internalPort: 8080,
			wantErrMsg:   "conflict",
		},
		{
			name:         "空Host与通配Host同端口冲突",
			host:         "",
			port:         8080,
			internalHost: "0.0.0.0",
			internalPort: 8080,
			wantErrMsg:   "conflict",
		},
		{
			name:         "IPv6通配符同端口冲突",
			host:         "::",
			port:         8080,
			internalHost: "127.0.0.1",
			internalPort: 8080,
			wantErrMsg:   "conflict",
		},
		{
			name:         "不同具体IP同端口合法不冲突",
			host:         "127.0.0.1",
			port:         8080,
			internalHost: "192.168.1.1",
			internalPort: 8080,
			wantErrMsg:   "",
		},
		{
			name:         "IPv6同一地址压缩形式与展开形式同端口冲突",
			host:         "::1",
			port:         8080,
			internalHost: "0:0:0:0:0:0:0:1",
			internalPort: 8080,
			wantErrMsg:   "conflict",
		},
		{
			name:         "IPv6展开通配地址与具体地址同端口冲突",
			host:         "0:0:0:0:0:0:0:0",
			port:         8080,
			internalHost: "127.0.0.1",
			internalPort: 8080,
			wantErrMsg:   "conflict",
		},
		{
			name:         "IPv4与IPv4映射IPv6等价地址同端口冲突",
			host:         "127.0.0.1",
			port:         8080,
			internalHost: "::ffff:127.0.0.1",
			internalPort: 8080,
			wantErrMsg:   "conflict",
		},
		{
			name:         "带方括号IPv6地址同端口冲突",
			host:         "[::1]",
			port:         8080,
			internalHost: "[0:0:0:0:0:0:0:1]",
			internalPort: 8080,
			wantErrMsg:   "conflict",
		},
		{
			name:         "主机名大小写不敏感匹配同端口冲突",
			host:         "localhost",
			port:         8080,
			internalHost: "LOCALHOST",
			internalPort: 8080,
			wantErrMsg:   "conflict",
		},
		{
			name:         "不同主机名同端口不冲突",
			host:         "public.domain",
			port:         8080,
			internalHost: "internal.domain",
			internalPort: 8080,
			wantErrMsg:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := serverx.ServerConfig{
				ServerHost:         tt.host,
				ServerPort:         tt.port,
				InternalServerHost: tt.internalHost,
				InternalServerPort: tt.internalPort,
			}
			err := cfg.ValidateDualPorts()
			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestServerConfig_DefaultPortAndPortGetter(t *testing.T) {
	t.Run("ServerPort为0时应用默认端口", func(t *testing.T) {
		oldVal, exists := os.LookupEnv("SERVER_PORT")
		require.NoError(t, os.Unsetenv("SERVER_PORT"))
		t.Cleanup(func() {
			if exists {
				_ = os.Setenv("SERVER_PORT", oldVal)
			} else {
				_ = os.Unsetenv("SERVER_PORT")
			}
		})

		cfg := serverx.ServerConfig{ServerPort: 0}
		cfg.ApplyDefaultPort(8081)
		assert.Equal(t, 8081, cfg.ServerPort)
		assert.Equal(t, 8081, cfg.Port())
	})

	t.Run("ServerPort为8080且SERVER_PORT环境变量未设置时应用默认端口", func(t *testing.T) {
		oldVal, exists := os.LookupEnv("SERVER_PORT")
		require.NoError(t, os.Unsetenv("SERVER_PORT"))
		t.Cleanup(func() {
			if exists {
				_ = os.Setenv("SERVER_PORT", oldVal)
			} else {
				_ = os.Unsetenv("SERVER_PORT")
			}
		})

		cfg := serverx.ServerConfig{ServerPort: 8080}
		cfg.ApplyDefaultPort(8081)
		assert.Equal(t, 8081, cfg.ServerPort)
		assert.Equal(t, 8081, cfg.Port())
	})

	t.Run("ServerPort为8080但SERVER_PORT环境变量显式设置时保留8080", func(t *testing.T) {
		t.Setenv("SERVER_PORT", "8080")

		cfg := serverx.ServerConfig{ServerPort: 8080}
		cfg.ApplyDefaultPort(8081)
		assert.Equal(t, 8080, cfg.ServerPort)
		assert.Equal(t, 8080, cfg.Port())
	})

	t.Run("ServerPort为显式非8080非0端口时保留原值", func(t *testing.T) {
		cfg := serverx.ServerConfig{ServerPort: 9090}
		cfg.ApplyDefaultPort(8081)
		assert.Equal(t, 9090, cfg.ServerPort)
		assert.Equal(t, 9090, cfg.Port())
	})

	t.Run("WithDefaultPort返回带有默认端口的新副本", func(t *testing.T) {
		oldVal, exists := os.LookupEnv("SERVER_PORT")
		require.NoError(t, os.Unsetenv("SERVER_PORT"))
		t.Cleanup(func() {
			if exists {
				_ = os.Setenv("SERVER_PORT", oldVal)
			} else {
				_ = os.Unsetenv("SERVER_PORT")
			}
		})
		orig := serverx.ServerConfig{ServerPort: 8080}
		updated := orig.WithDefaultPort(8085)
		assert.Equal(t, 8080, orig.ServerPort)
		assert.Equal(t, 8085, updated.ServerPort)
		assert.Equal(t, 8085, updated.Port())
	})

	t.Run("nil指针调用ApplyDefaultPort安全不panic", func(t *testing.T) {
		var cfg *serverx.ServerConfig
		assert.NotPanics(t, func() {
			cfg.ApplyDefaultPort(8081)
		})
	})
}
