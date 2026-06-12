package process

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"time"
)

// GenerateRand 生成 RAND (16字节)
func GenerateRand() ([]byte, error) {
	randBytes := make([]byte, 16)
	_, err := rand.Read(randBytes)
	if err != nil {
		return nil, err
	}
	return randBytes, nil
}

type AuthData struct {
	Rand      string `json:"rand"`
	Autn      string `json:"autn"`
	MSK       string `json:"msk"`
	Xres      string `json:"xres"`
	ExpiredAt int64  `json:"expiredAt"`
}

func GetEapData(Ki string) AuthData {
	randVal, err := GenerateRand()
	if err != nil {
		fmt.Println("生成 RAND 出错:", err)
		return AuthData{}
	}

	fmt.Println("RAND:", hex.EncodeToString(randVal))
	//message := append([]byte(Ki), randVal...)
	// 获取分钟级时间戳
	minuteTS := time.Now().Unix() / 60
	timestampStr := fmt.Sprintf("%d", minuteTS)
	//message = append(message, []byte(timestamp)...)
	messageStr := Ki + hex.EncodeToString(randVal) + timestampStr
	message := []byte(messageStr)

	autn := hmac.New(sha256.New, []byte(Ki))
	autn.Write(message)
	Autn := autn.Sum(nil)
	fmt.Println("Autn:", hex.EncodeToString(Autn))

	xres := hmac.New(sha256.New, []byte(Ki))
	xres.Write([]byte(Ki + hex.EncodeToString(randVal)))
	Xres := xres.Sum(nil)

	msk := GenerateMSK(Ki, randVal)

	data := AuthData{
		Rand:      hex.EncodeToString(randVal),
		Autn:      hex.EncodeToString(Autn),
		MSK:       hex.EncodeToString(msk),
		Xres:      hex.EncodeToString(Xres),
		ExpiredAt: time.Now().Add(time.Duration(1) * time.Hour).Unix(),
	}

	return data
}

// Extract 步骤：从输入密钥材料 (IKM) 提取固定长度的伪随机密钥 (PRK)
func Extract(hash func() hash.Hash, ikm, salt []byte) []byte {
	if salt == nil {
		salt = make([]byte, hash().Size())
	}
	h := hmac.New(hash, salt)
	h.Write(ikm)
	return h.Sum(nil)
}

// Expand 步骤：从 PRK 扩展出所需长度的密钥材料
func Expand(hash func() hash.Hash, prk, info []byte, length int) []byte {
	hashSize := hash().Size()
	n := (length + hashSize - 1) / hashSize
	okm := make([]byte, 0, n*hashSize)
	var t []byte

	for i := 1; i <= n; i++ {
		h := hmac.New(hash, prk)
		h.Write(t)
		h.Write(info)
		h.Write([]byte{byte(i)})
		t = h.Sum(nil)
		okm = append(okm, t...)
	}
	return okm[:length]
}

func GenerateMSK(k string, rand []byte) []byte {

	prk := Extract(sha256.New, []byte(k), rand)
	msk := Expand(sha256.New, prk, []byte("Master Session Key"), 32)

	return msk
}
