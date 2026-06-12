package main

import (
	"fmt"
	"mytrading/logger"
	"mytrading/server"
)

func main() {

	logger.Init()
	defer logger.Sync()

	logger.Info("mytrading 启动中...")
	fmt.Println("mytrading 启动中...")

	// 开启服务做路由入口
	server.StartRouter()

}
