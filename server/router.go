package server

import (
	"fmt"
	"net/http"
)

func HelloHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, my trading")
}

func StartRouter() {
	go StartTrade()

	fs := http.FileServer(http.Dir("./web/"))
	http.Handle("/", fs)
	http.HandleFunc("/hello", HelloHandler)
	http.HandleFunc("/api/dashboard", GetDashboard)
	http.HandleFunc("/api/bars", GetBarsHandler)
	http.HandleFunc("/api/liquidate", LiquidateHandler)
	http.HandleFunc("/api/restart", RestartHandler)

	fmt.Println("服务已启动，监听端口 :8400...")
	if err := http.ListenAndServe("127.0.0.1:8400", nil); err != nil {
		panic(err)
	}
}
