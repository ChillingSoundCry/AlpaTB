package process

import (
	"encoding/json"
	"fmt"
	"mytrading/logger"
	"os"
	"sync/atomic"
	"time"
)

type confInfo struct {
	PassWord    string `json:"PassWord"`
	ExpiredTime int    `json:"ExpiredTime"`
	ServerIP    string `json:"ServerIP"`
}

type IPFailInfo struct {
	Count       int
	BlockedTill int64
}

type Cache struct {
	AuthDataList atomic.Value

	IPFailList atomic.Value
}

var Conf confInfo

func ReadConfInfo() {
	content, err := os.ReadFile("./conf/conf.json")
	if err != nil {
		logger.Info("读取文件失败", err)
		fmt.Println("读取文件失败:", err)
	}

	err = json.Unmarshal(content, &Conf)
	if err != nil {
		logger.Info("解析 JSON 失败:", err)
		fmt.Println("解析 JSON 失败:", err)
	}
	logger.Info("PassWord", Conf.PassWord)
	logger.Info("ExpiredTime", Conf.ExpiredTime)
	logger.Info("ServerIP", Conf.ServerIP)
}

func NewCache() *Cache {
	c := &Cache{}

	c.AuthDataList.Store(make(map[string]AuthData))
	c.IPFailList.Store(make(map[string]IPFailInfo))

	go c.gcLoop()

	return c
}

func (c *Cache) SetAuthData(data AuthData) {

	oldMap := c.AuthDataList.Load().(map[string]AuthData)

	// copy-on-write
	newMap := make(map[string]AuthData, len(oldMap)+1)

	for k, v := range oldMap {
		newMap[k] = v
	}

	newMap[data.Xres] = data

	c.AuthDataList.Store(newMap)
}

func (c *Cache) AuthDataExists(xres string) (bool, string) {

	m := c.AuthDataList.Load().(map[string]AuthData)

	authdata, ok := m[xres]

	if !ok {
		return false, ""
	}

	///set msk to main key for map
	oldMap := c.AuthDataList.Load().(map[string]AuthData)

	newMap := make(map[string]AuthData)

	for k, v := range oldMap {
		if k != xres {
			newMap[k] = v
		}

	}
	newMap[authdata.MSK] = authdata

	c.AuthDataList.Store(newMap)

	return true, authdata.MSK
}

func (c *Cache) MskExists(msk string) bool {
	m := c.AuthDataList.Load().(map[string]AuthData)
	_, ok := m[msk]

	if !ok {
		return false
	} else {
		return true
	}
}

func (c *Cache) gcLoop() {

	ticker := time.NewTicker(1 * time.Minute)

	for range ticker.C {

		now := time.Now().Unix()

		oldMap := c.AuthDataList.Load().(map[string]AuthData)

		newMap := make(map[string]AuthData)

		for k, v := range oldMap {

			if v.ExpiredAt > now {
				newMap[k] = v
			}
		}

		c.AuthDataList.Store(newMap)

		oldIPMap := c.IPFailList.Load().(map[string]IPFailInfo)

		newIPMap := make(map[string]IPFailInfo)

		for ip, info := range oldIPMap {

			// 未被封禁的记录保留
			if info.BlockedTill == 0 {
				newIPMap[ip] = info
				continue
			}

			// 封禁未过期
			if info.BlockedTill > now {
				newIPMap[ip] = info
			}

			// 封禁已过期
			// 不加入 newIPMap
			// 自动删除
		}

		c.IPFailList.Store(newIPMap)
	}
}

func (c *Cache) IsIPBlocked(ip string) bool {

	m := c.IPFailList.Load().(map[string]IPFailInfo)

	info, ok := m[ip]

	if !ok {
		return false
	}

	return info.BlockedTill > time.Now().Unix()
}

func (c *Cache) RecordIPFail(ip string) {

	oldMap := c.IPFailList.Load().(map[string]IPFailInfo)

	newMap := make(map[string]IPFailInfo, len(oldMap)+1)

	for k, v := range oldMap {
		newMap[k] = v
	}

	info := newMap[ip]

	info.Count++

	if info.Count >= 5 {

		info.BlockedTill = time.Now().
			Add(30 * time.Minute).
			Unix()

		logger.Info("IP blocked:", ip)
	}

	newMap[ip] = info

	c.IPFailList.Store(newMap)
}

func (c *Cache) RecordIPSuccess(ip string) {

	oldMap := c.IPFailList.Load().(map[string]IPFailInfo)

	newMap := make(map[string]IPFailInfo)

	for k, v := range oldMap {

		if k != ip {
			newMap[k] = v
		}
	}

	c.IPFailList.Store(newMap)
}
