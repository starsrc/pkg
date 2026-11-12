package muxguard

import "net/http"

var savedMux *http.ServeMux

func init() {
	savedMux = http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()
}

// Restore 恢复在加载 net/http/pprof 前保存的原始 http.DefaultServeMux，
// 确保引入 pprof 处理器不会污染全局默认路由表。
func Restore() {
	if savedMux != nil {
		http.DefaultServeMux = savedMux
		savedMux = nil
	}
}
