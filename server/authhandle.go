package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"

	"mytrading/logger"
	"mytrading/process"
)

var globalCache = process.NewCache()

func GetClientIP(r *http.Request) string {
	ip := r.Header.Get("X-Forwarded-For")
	if ip != "" {
		return strings.Split(ip, ",")[0]
	}

	ip = r.Header.Get("X-Real-IP")
	if ip != "" {
		return ip
	}

	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return host
}

func AuthChallenge(w http.ResponseWriter, r *http.Request) {
	logger.Info("entery AuthChallenge")
	fmt.Println("entery AuthChallenge")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	eapData := process.GetEapData("Here2go!")
	globalCache.SetAuthData(eapData)
	data := map[string]interface{}{
		"code": 200,
		"rand": eapData.Rand,
		"autn": eapData.Autn,
	}
	w.Header().Set("Content-Type", "application/json")
	jsonData, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "JSON 转换失败", http.StatusInternalServerError)
		return
	}
	//fmt.Println(jsonData)

	w.Write(jsonData)
}

type VerifyRequest struct {
	Res string `json:"res"`
}

func AuthVerify(w http.ResponseWriter, r *http.Request) {
	logger.Info("entery AuthVerify")
	fmt.Println("entery AuthVerify")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	ip := GetClientIP(r)
	if globalCache.IsIPBlocked(ip) {

		w.WriteHeader(http.StatusForbidden)

		w.Write([]byte("ip blocked"))
		logger.Info("ip blocked")

		return
	}

	var req VerifyRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "bad json", 400)
		return
	}

	isVertify, msk := globalCache.AuthDataExists(req.Res)
	data := map[string]interface{}{
		"code":   400,
		"result": "auth failed",
	}
	if isVertify == true {
		data = map[string]interface{}{
			"code":   200,
			"result": "success",
			"msk":    msk,
		}
		globalCache.RecordIPSuccess(ip)
	} else {
		globalCache.RecordIPFail(ip)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsonData, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "JSON 转换失败", http.StatusInternalServerError)
		return
	}
	//fmt.Println(jsonData)

	w.Write(jsonData)
}
