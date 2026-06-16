package process

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"
)

const (
	defaultBaseURL = "https://paper-api.alpaca.markets"
	defaultDataURL = "https://data.alpaca.markets"
	maxErrorLogLen = 200
	maxSnapshots   = 2000
)

// -----------------------
// Configuration
// -----------------------

type Config struct {
	APIKey      string
	APISecret   string
	BaseURL     string
	DataURL     string
	DataFeed    string
	HTTPTimeout time.Duration
	Interval    time.Duration
}

func LoadConfig() Config {
	apiKey := strings.TrimSpace(os.Getenv("APCA_API_KEY_ID"))
	apiSecret := strings.TrimSpace(os.Getenv("APCA_API_SECRET_KEY"))
	if apiKey == "" || apiSecret == "" {
		log.Fatal("missing APCA_API_KEY_ID or APCA_API_SECRET_KEY environment variables")
	}
	intervalSec := 30
	if v := strings.TrimSpace(os.Getenv("BOT_INTERVAL_SECONDS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			intervalSec = n
		}
	}
	return Config{
		APIKey:      apiKey,
		APISecret:   apiSecret,
		BaseURL:     getenvDefault("APCA_BASE_URL", defaultBaseURL),
		DataURL:     getenvDefault("APCA_DATA_URL", defaultDataURL),
		DataFeed:    getenvDefault("APCA_DATA_FEED", "iex"),
		HTTPTimeout: 12 * time.Second,
		Interval:    time.Duration(intervalSec) * time.Second,
	}
}

func getenvDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// -----------------------
// Alpaca REST client
// -----------------------

type AlpacaClient struct {
	baseURL string
	dataURL string
	feed    string
	apiKey  string
	secret  string
	client  *http.Client
}

func NewAlpacaClient(cfg Config) *AlpacaClient {
	return &AlpacaClient{
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		dataURL: strings.TrimRight(cfg.DataURL, "/"),
		feed:    cfg.DataFeed,
		apiKey:  cfg.APIKey,
		secret:  cfg.APISecret,
		client:  &http.Client{Timeout: cfg.HTTPTimeout},
	}
}

func (c *AlpacaClient) doJSON(ctx context.Context, method, url string, body any, out any) error {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("APCA-API-KEY-ID", c.apiKey)
	req.Header.Set("APCA-API-SECRET-KEY", c.secret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("alpaca %s %s: status=%d body=%s", method, url, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(b, out)
}

type PortfolioHistory struct {
	Timestamp []int64   `json:"timestamp"`
	Equity    []float64 `json:"equity"`
}

func (c *AlpacaClient) GetPortfolioHistory7D(ctx context.Context) (float64, error) {
	u, _ := url.Parse(c.baseURL + "/v2/account/portfolio/history")
	q := u.Query()
	q.Set("period", "7D")
	q.Set("timeframe", "1D")
	u.RawQuery = q.Encode()
	var out PortfolioHistory
	if err := c.doJSON(ctx, http.MethodGet, u.String(), nil, &out); err != nil {
		return 0, err
	}
	if len(out.Equity) == 0 {
		return 0, errors.New("no portfolio history data")
	}
	if len(out.Timestamp) == len(out.Equity) && len(out.Timestamp) > 0 {
		oldestIdx := 0
		for i := 1; i < len(out.Timestamp); i++ {
			if out.Timestamp[i] < out.Timestamp[oldestIdx] {
				oldestIdx = i
			}
		}
		return out.Equity[oldestIdx], nil
	}
	return out.Equity[0], nil
}

type Account struct {
	ID               string `json:"id"`
	Status           string `json:"status"`
	Equity           string `json:"equity"`
	Cash             string `json:"cash"`
	BuyingPower      string `json:"buying_power"`
	LongMarketValue  string `json:"long_market_value"`
	ShortMarketValue string `json:"short_market_value"`
}

type Clock struct {
	Timestamp time.Time `json:"timestamp"`
	IsOpen    bool      `json:"is_open"`
	NextOpen  time.Time `json:"next_open"`
	NextClose time.Time `json:"next_close"`
}

type Position struct {
	AssetID        string `json:"asset_id"`
	Symbol         string `json:"symbol"`
	AvgEntryPrice  string `json:"avg_entry_price"`
	Qty            string `json:"qty"`
	Side           string `json:"side"`
	MarketValue    string `json:"market_value"`
	UnrealizedPL   string `json:"unrealized_pl"`
	UnrealizedPLPC string `json:"unrealized_plpc"`
	CurrentPrice   string `json:"current_price"`
}

type Order struct {
	ID             string     `json:"id"`
	ClientOrderID  string     `json:"client_order_id"`
	Symbol         string     `json:"symbol"`
	Side           string     `json:"side"`
	Type           string     `json:"type"`
	Qty            string     `json:"qty"`
	LimitPrice     string     `json:"limit_price"`
	FilledQty      string     `json:"filled_qty"`
	FilledAvgPrice string     `json:"filled_avg_price"`
	Status         string     `json:"status"`
	CreatedAt      *time.Time `json:"created_at"`
	FilledAt       *time.Time `json:"filled_at"`
}

type OrderRequest struct {
	Symbol        string `json:"symbol"`
	Qty           string `json:"qty,omitempty"`
	Side          string `json:"side"`
	Type          string `json:"type"`
	TimeInForce   string `json:"time_in_force"`
	LimitPrice    string `json:"limit_price,omitempty"`
	ExtendedHours bool   `json:"extended_hours,omitempty"`
	ClientOrderID string `json:"client_order_id,omitempty"`
}

type QuoteResponse struct {
	Quote struct {
		AskPrice float64 `json:"ap"`
		BidPrice float64 `json:"bp"`
	} `json:"quote"`
}

type TradeResponse struct {
	Trade struct {
		Price float64 `json:"p"`
	} `json:"trade"`
}

// -----------------------
// New API: GetOrder
// -----------------------
func (c *AlpacaClient) GetOrder(ctx context.Context, orderID string) (*Order, error) {
	var out Order
	if err := c.doJSON(ctx, http.MethodGet, c.baseURL+"/v2/orders/"+orderID, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *AlpacaClient) GetAccount(ctx context.Context) (*Account, error) {
	var out Account
	if err := c.doJSON(ctx, http.MethodGet, c.baseURL+"/v2/account", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *AlpacaClient) GetClock(ctx context.Context) (*Clock, error) {
	var out Clock
	if err := c.doJSON(ctx, http.MethodGet, c.baseURL+"/v2/clock", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *AlpacaClient) GetPosition(ctx context.Context, symbol string) (*Position, error) {
	var out Position
	if err := c.doJSON(ctx, http.MethodGet, c.baseURL+"/v2/positions/"+symbol, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *AlpacaClient) GetPositions(ctx context.Context) ([]Position, error) {
	var out []Position
	if err := c.doJSON(ctx, http.MethodGet, c.baseURL+"/v2/positions", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *AlpacaClient) ListOrders(ctx context.Context, status string) ([]Order, error) {
	u := c.baseURL + "/v2/orders?nested=true&limit=500"
	if status != "" {
		u += "&status=" + status
	}
	var out []Order
	if err := c.doJSON(ctx, http.MethodGet, u, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *AlpacaClient) CancelOrder(ctx context.Context, orderID string) error {
	return c.doJSON(ctx, http.MethodDelete, c.baseURL+"/v2/orders/"+orderID, nil, nil)
}

func (c *AlpacaClient) GetReferencePrice(ctx context.Context, symbol string) (float64, error) {
	var qResp QuoteResponse
	if err := c.doJSON(ctx, http.MethodGet, fmt.Sprintf("%s/v2/stocks/%s/quotes/latest?feed=%s", c.dataURL, symbol, c.feed), nil, &qResp); err == nil {
		if qResp.Quote.AskPrice > 0 && qResp.Quote.BidPrice > 0 {
			return (qResp.Quote.AskPrice + qResp.Quote.BidPrice) / 2.0, nil
		}
	}
	var tResp TradeResponse
	if err := c.doJSON(ctx, http.MethodGet, fmt.Sprintf("%s/v2/stocks/%s/trades/latest?feed=%s", c.dataURL, symbol, c.feed), nil, &tResp); err == nil {
		if tResp.Trade.Price > 0 {
			return tResp.Trade.Price, nil
		}
	}
	return 0, errors.New("unable to parse reference price")
}

func (c *AlpacaClient) PlaceOrder(ctx context.Context, req OrderRequest) (*Order, error) {
	var out Order
	if err := c.doJSON(ctx, http.MethodPost, c.baseURL+"/v2/orders", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *AlpacaClient) CloseAllPositions(ctx context.Context) error {
	return c.doJSON(ctx, http.MethodDelete, c.baseURL+"/v2/positions?cancel_orders=true", nil, nil)
}

func parseFloatString(s string) float64 {
	f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f
}

func gridPriceKey(price float64) string {
	return fmt.Sprintf("%.2f", price)
}

func isHTTPStatusError(err error, code int) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), fmt.Sprintf("status=%d", code))
}

// -----------------------
// Data Structures
// -----------------------

type HoldingSummary struct {
	Symbol        string  `json:"symbol"`
	Qty           float64 `json:"qty"`
	AvgEntryPrice float64 `json:"avg_entry_price"`
	MarketValue   float64 `json:"market_value"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`
	CurrentPrice  float64 `json:"current_price"`
	Side          string  `json:"side"`
}

type PerformanceSummary struct {
	InitialEquity float64 `json:"initial_equity"`
	CurrentEquity float64 `json:"current_equity"`
	RealizedPnL   float64 `json:"realized_pnl"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`
	TotalPnL      float64 `json:"total_pnl"`
	ReturnPct     float64 `json:"return_pct"`
}

type DailySnapshot struct {
	Time   time.Time `json:"time"`
	Equity float64   `json:"equity"`
}

type ErrorRecord struct {
	Time     time.Time `json:"time"`
	Strategy string    `json:"strategy"`
	Error    string    `json:"error"`
}

type OrderSummary struct {
	ID            string  `json:"id"`
	ClientOrderID string  `json:"client_order_id"`
	Symbol        string  `json:"symbol"`
	Side          string  `json:"side"`
	Type          string  `json:"type"`
	Qty           float64 `json:"qty"`
	FilledQty     float64 `json:"filled_qty"`
	LimitPrice    float64 `json:"limit_price"`
	Status        string  `json:"status"`
	CreatedAt     string  `json:"created_at"`
	Strategy      string  `json:"strategy"`
}

// -----------------------
// Strategy Interface
// -----------------------

type Strategy interface {
	Name() string
	Symbol() string
	Tick(ctx context.Context, acct *Account, clock *Clock) error
	Config() map[string]interface{}
}

type BaseStrategy struct {
	client *AlpacaClient
	mu     sync.Mutex
}

// -----------------------
// Grid Strategy
// -----------------------

type GridConfig struct {
	Symbol         string
	Levels         int
	SpacingPct     float64
	QtyPerOrder    float64
	SeedQty        float64
	MinPrice       float64
	MaxPrice       float64
	RecenterPct    float64
	MaxPositionQty float64 // 最大仓位限制
	UseTrendFilter bool    // 网格趋势过滤开关
}

type GridStrategy struct {
	BaseStrategy
	cfg            GridConfig
	initialized    bool
	centerPrice    float64
	lastBuildAt    time.Time
	pendingOrders  map[string]time.Time
	ma20Mu         sync.Mutex
	ma20CacheDay   string
	ma20CacheAllow bool
}

func NewGridStrategy(client *AlpacaClient, cfg GridConfig) *GridStrategy {
	if cfg.Levels < 1 {
		cfg.Levels = 1
	}
	if cfg.SpacingPct <= 0 {
		cfg.SpacingPct = 0.01
	}
	if cfg.QtyPerOrder <= 0 {
		cfg.QtyPerOrder = 1
	}
	if cfg.RecenterPct <= 0 {
		cfg.RecenterPct = 0.05 // 网格动态重心默认调整为 5%
	}
	return &GridStrategy{
		BaseStrategy:  BaseStrategy{client: client},
		cfg:           cfg,
		pendingOrders: make(map[string]time.Time),
	}
}

func (g *GridStrategy) Name() string   { return "grid" }
func (g *GridStrategy) Symbol() string { return g.cfg.Symbol }
func (g *GridStrategy) Config() map[string]interface{} {
	return map[string]interface{}{
		"symbol":        g.cfg.Symbol,
		"levels":        g.cfg.Levels,
		"spacing_pct":   g.cfg.SpacingPct,
		"qty_per_order": g.cfg.QtyPerOrder,
	}
}

func (g *GridStrategy) checkTrendMA20(ctx context.Context) bool {
	if !g.cfg.UseTrendFilter {
		return true
	}

	today := time.Now().UTC().Format("2006-01-02")

	g.ma20Mu.Lock()
	if g.ma20CacheDay == today {
		allowed := g.ma20CacheAllow
		g.ma20Mu.Unlock()
		return allowed
	}
	g.ma20Mu.Unlock()

	end := time.Now().UTC()
	start := end.AddDate(0, 0, -60)

	bars, err := g.client.GetBars(ctx, g.cfg.Symbol, "1Day", start, end, 100)
	allowed := false
	if err == nil && len(bars) > 0 {
		if len(bars) > 20 {
			bars = bars[len(bars)-20:]
		}
		sum := 0.0
		for _, b := range bars {
			sum += b.Close
		}
		ma20 := sum / float64(len(bars))
		price, priceErr := g.client.GetReferencePrice(ctx, g.cfg.Symbol)
		if priceErr == nil && price > 0 {
			allowed = price > ma20
		}
	}

	g.ma20Mu.Lock()
	g.ma20CacheDay = today
	g.ma20CacheAllow = allowed
	g.ma20Mu.Unlock()

	return allowed
}

func (g *GridStrategy) Tick(ctx context.Context, acct *Account, clock *Clock) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	price, err := g.client.GetReferencePrice(ctx, g.cfg.Symbol)
	if err != nil {
		return err
	}
	openOrders, err := g.client.ListOrders(ctx, "open")
	if err != nil {
		return err
	}
	openOrders = filterOrdersBySymbol(openOrders, g.cfg.Symbol)
	posQty, _ := currentPositionQty(ctx, g.client, g.cfg.Symbol)
	buyingPower := parseFloatString(acct.BuyingPower)
	pendingBuyQty := 0.0
	for _, o := range openOrders {
		if strings.ToLower(o.Side) == "buy" {
			q := parseFloatString(o.Qty)
			fq := parseFloatString(o.FilledQty)
			if q > fq {
				pendingBuyQty += (q - fq)
			}
		}
	}

	if !g.initialized {
		if len(openOrders) > 0 {
			g.centerPrice = price
			g.initialized = true
			g.lastBuildAt = time.Now()
			return nil
		}
		// 传递 pendingBuyQty 入内辅助仓位判定
		return g.rebuildGrid(ctx, price, posQty, pendingBuyQty, buyingPower)
	}

	drift := 0.0
	if g.centerPrice > 0 {
		drift = math.Abs(price-g.centerPrice) / g.centerPrice
	}
	if drift >= g.cfg.RecenterPct {
		if err := g.cancelAllSymbolOrders(ctx, openOrders); err != nil {
			return err
		}
		return g.rebuildGrid(ctx, price, posQty, 0.0, buyingPower)
	}

	return g.maintainGrid(ctx, price, posQty, pendingBuyQty, openOrders, buyingPower)
}

func (g *GridStrategy) cancelAllSymbolOrders(ctx context.Context, orders []Order) error {
	for _, o := range orders {
		if err := g.client.CancelOrder(ctx, o.ID); err != nil {
			return err
		}
	}
	return nil
}

func (g *GridStrategy) rebuildGrid(ctx context.Context, center, posQty, pendingBuyQty, buyingPower float64) error {
	g.centerPrice = center
	g.initialized = true
	g.lastBuildAt = time.Now()
	g.pendingOrders = make(map[string]time.Time)

	if posQty <= 0 && g.cfg.SeedQty > 0 {
		notional := g.cfg.SeedQty * center
		if buyingPower >= notional {
			_, err := g.client.PlaceOrder(ctx, OrderRequest{
				Symbol:        g.cfg.Symbol,
				Qty:           fmt.Sprintf("%.6f", g.cfg.SeedQty),
				Side:          "buy",
				Type:          "market",
				TimeInForce:   "day",
				ClientOrderID: fmt.Sprintf("grid-seed-%s-%d", g.cfg.Symbol, time.Now().UnixNano()),
			})
			if err != nil {
				return err
			}
			return nil
		}
		return nil
	}

	isUptrend := g.checkTrendMA20(ctx) // 趋势过滤检查
	availableSellQty := posQty
	usedBuyPrices := map[string]bool{}
	usedSellPrices := map[string]bool{}

	for i := 1; i <= g.cfg.Levels; i++ {
		off := float64(i) * g.cfg.SpacingPct
		buyPrice := center * (1 - off)
		sellPrice := center * (1 + off)

		buyKey := gridPriceKey(buyPrice)
		sellKey := gridPriceKey(sellPrice)

		if !usedBuyPrices[buyKey] && buyPrice > 0 && (g.cfg.MinPrice <= 0 || buyPrice >= g.cfg.MinPrice) && isUptrend {
			projectedQty := posQty + pendingBuyQty + g.cfg.QtyPerOrder
			if g.cfg.MaxPositionQty <= 0 || projectedQty <= g.cfg.MaxPositionQty {
				newBP := g.placeBuyIfSafe(ctx, i, buyPrice, g.cfg.QtyPerOrder, buyingPower)
				if newBP < buyingPower {
					buyingPower = newBP
					pendingBuyQty += g.cfg.QtyPerOrder
					usedBuyPrices[buyKey] = true
				}
			}
		}

		if !usedSellPrices[sellKey] && sellPrice > 0 && (g.cfg.MaxPrice <= 0 || sellPrice <= g.cfg.MaxPrice) {
			qty := math.Min(g.cfg.QtyPerOrder, availableSellQty)
			if qty > 0 {
				g.placeSellIfSafe(ctx, i, sellPrice, qty)
				availableSellQty -= qty
				usedSellPrices[sellKey] = true
			}
		}
	}
	return nil
}

func (g *GridStrategy) maintainGrid(ctx context.Context, center, posQty, pendingBuyQty float64, openOrders []Order, buyingPower float64) error {
	openBuyPrices := map[string]bool{}
	openSellPrices := map[string]bool{}
	openSellQty := 0.0

	for _, o := range openOrders {
		priceKey := gridPriceKey(parseFloatString(o.LimitPrice))
		side := strings.ToLower(strings.TrimSpace(o.Side))
		if priceKey == "0.00" {
			continue
		}
		delete(g.pendingOrders, side+"-"+priceKey)
		switch side {
		case "buy":
			openBuyPrices[priceKey] = true
		case "sell":
			openSellPrices[priceKey] = true
			openSellQty += parseFloatString(o.Qty)
		}
	}

	isUptrend := g.checkTrendMA20(ctx)
	availableSellQty := math.Max(0, posQty-openSellQty)
	usedBuyPrices := map[string]bool{}
	usedSellPrices := map[string]bool{}

	for i := 1; i <= g.cfg.Levels; i++ {
		off := float64(i) * g.cfg.SpacingPct
		buyPrice := center * (1 - off)
		sellPrice := center * (1 + off)

		buyKey := gridPriceKey(buyPrice)
		sellKey := gridPriceKey(sellPrice)

		if !usedBuyPrices[buyKey] && !openBuyPrices[buyKey] && buyPrice > 0 && (g.cfg.MinPrice <= 0 || buyPrice >= g.cfg.MinPrice) && isUptrend {
			if g.cfg.MaxPositionQty <= 0 || (posQty+pendingBuyQty+g.cfg.QtyPerOrder) <= g.cfg.MaxPositionQty {
				newBP := g.placeBuyIfSafe(ctx, i, buyPrice, g.cfg.QtyPerOrder, buyingPower)
				if newBP < buyingPower {
					buyingPower = newBP
					pendingBuyQty += g.cfg.QtyPerOrder
					usedBuyPrices[buyKey] = true
				}
			}
		}

		if !usedSellPrices[sellKey] && !openSellPrices[sellKey] && availableSellQty > 0 && sellPrice > 0 && (g.cfg.MaxPrice <= 0 || sellPrice <= g.cfg.MaxPrice) {
			qty := math.Min(g.cfg.QtyPerOrder, availableSellQty)
			if qty > 0 {
				g.placeSellIfSafe(ctx, i, sellPrice, qty)
				availableSellQty -= qty
				usedSellPrices[sellKey] = true
			}
		}
	}
	return nil
}

func (g *GridStrategy) placeBuyIfSafe(ctx context.Context, level int, price, qty, bp float64) float64 {
	priceKey := gridPriceKey(price)
	key := "buy-" + priceKey
	if g.isPending(key) {
		return bp
	}
	notional := price * qty
	if bp < notional {
		return bp
	}
	_, err := g.client.PlaceOrder(ctx, OrderRequest{
		Symbol:        g.cfg.Symbol,
		Qty:           fmt.Sprintf("%.6f", qty),
		Side:          "buy",
		Type:          "limit",
		TimeInForce:   "day",
		LimitPrice:    priceKey,
		ClientOrderID: fmt.Sprintf("grid-buy-%s-%d-%s-%d", g.cfg.Symbol, level, priceKey, time.Now().UnixNano()),
	})
	if err == nil {
		g.pendingOrders[key] = time.Now()
		return bp - notional
	}
	return bp
}

func (g *GridStrategy) placeSellIfSafe(ctx context.Context, level int, price, qty float64) {
	priceKey := gridPriceKey(price)
	key := "sell-" + priceKey
	if g.isPending(key) {
		return
	}
	_, err := g.client.PlaceOrder(ctx, OrderRequest{
		Symbol:        g.cfg.Symbol,
		Qty:           fmt.Sprintf("%.6f", qty),
		Side:          "sell",
		Type:          "limit",
		TimeInForce:   "day",
		LimitPrice:    priceKey,
		ClientOrderID: fmt.Sprintf("grid-sell-%s-%d-%s-%d", g.cfg.Symbol, level, priceKey, time.Now().UnixNano()),
	})
	if err == nil {
		g.pendingOrders[key] = time.Now()
	}
}

func (g *GridStrategy) isPending(key string) bool {
	if t, ok := g.pendingOrders[key]; ok {
		if time.Since(t) < 60*time.Second {
			return true
		}
		delete(g.pendingOrders, key)
	}
	return false
}

func (g *GridStrategy) levelIndex(center, price float64) int {
	if center <= 0 || price <= 0 || g.cfg.SpacingPct <= 0 {
		return 0
	}
	ratio := price / center
	diff := math.Abs(1 - ratio)
	idx := int(math.Round(diff / g.cfg.SpacingPct))
	if idx < 1 {
		idx = 1
	}
	return idx
}

// -----------------------
// Open/Close Strategy
// -----------------------

type OpenCloseConfig struct {
	Symbol                 string
	Qty                    float64
	BuyMinutesBeforeOpen   int
	SellMinutesBeforeClose int
	SellMinutesBeforeOpen  int
	BuyMinutesBeforeClose  int
}

type OpenCloseStrategy struct {
	BaseStrategy
	cfg           OpenCloseConfig
	lastBuyDate   string
	lastSellDate  string
	pendingBuyID  string
	pendingSellID string
}

func NewOpenCloseStrategy(client *AlpacaClient, cfg OpenCloseConfig) *OpenCloseStrategy {
	if cfg.Qty <= 0 {
		cfg.Qty = 1
	}
	if cfg.SellMinutesBeforeOpen <= 0 {
		cfg.SellMinutesBeforeOpen = cfg.BuyMinutesBeforeOpen
	}
	if cfg.BuyMinutesBeforeClose <= 0 {
		cfg.BuyMinutesBeforeClose = cfg.SellMinutesBeforeClose
	}
	if cfg.SellMinutesBeforeOpen <= 0 {
		cfg.SellMinutesBeforeOpen = 5
	}
	if cfg.BuyMinutesBeforeClose <= 0 {
		cfg.BuyMinutesBeforeClose = 5
	}
	return &OpenCloseStrategy{
		BaseStrategy: BaseStrategy{client: client},
		cfg:          cfg,
	}
}

func (s *OpenCloseStrategy) Name() string   { return "open-close" }
func (s *OpenCloseStrategy) Symbol() string { return s.cfg.Symbol }
func (s *OpenCloseStrategy) Config() map[string]interface{} {
	return map[string]interface{}{
		"symbol":                    s.cfg.Symbol,
		"qty":                       s.cfg.Qty,
		"buy_minutes_before_open":   s.cfg.BuyMinutesBeforeOpen,
		"sell_minutes_before_close": s.cfg.SellMinutesBeforeClose,
		"sell_minutes_before_open":  s.cfg.SellMinutesBeforeOpen,
		"buy_minutes_before_close":  s.cfg.BuyMinutesBeforeClose,
	}
}

func (s *OpenCloseStrategy) Tick(ctx context.Context, acct *Account, clock *Clock) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := clock.Timestamp
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if s.pendingBuyID != "" {
		if o, err := s.client.GetOrder(ctx, s.pendingBuyID); err == nil {
			if o.Status == "filled" {
				s.lastBuyDate = clock.NextClose.Format("2006-01-02")
				s.pendingBuyID = ""
			} else if o.Status == "canceled" || o.Status == "rejected" {
				s.pendingBuyID = ""
			}
		}
	}
	if s.pendingSellID != "" {
		if o, err := s.client.GetOrder(ctx, s.pendingSellID); err == nil {
			if o.Status == "filled" {
				s.lastSellDate = clock.NextOpen.Format("2006-01-02")
				s.pendingSellID = ""
			} else if o.Status == "canceled" || o.Status == "rejected" {
				s.pendingSellID = ""
			}
		}
	}
	if !clock.IsOpen && clock.NextOpen.After(now) {
		timeUntilOpen := clock.NextOpen.Sub(now)
		sellDateStr := clock.NextOpen.Format("2006-01-02")
		if timeUntilOpen <= time.Duration(s.cfg.SellMinutesBeforeOpen)*time.Minute && s.lastSellDate != sellDateStr && s.pendingSellID == "" {
			if err := s.executeSellBeforeOpen(ctx, sellDateStr); err != nil {
				return err
			}
		}
	}

	if clock.IsOpen && clock.NextClose.After(now) {
		timeUntilClose := clock.NextClose.Sub(now)
		buyDateStr := clock.NextClose.Format("2006-01-02")
		if timeUntilClose <= time.Duration(s.cfg.BuyMinutesBeforeClose)*time.Minute && s.lastBuyDate != buyDateStr && s.pendingBuyID == "" {
			if err := s.executeBuyBeforeClose(ctx, acct, buyDateStr); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *OpenCloseStrategy) executeSellBeforeOpen(ctx context.Context, dateStr string) error {
	if hasActiveOrder(ctx, s.client, s.cfg.Symbol) {
		return nil
	}
	qtyHeld, err := currentPositionQty(ctx, s.client, s.cfg.Symbol)
	if err != nil || qtyHeld <= 0 {
		return err
	}
	sellQty := math.Min(qtyHeld, s.cfg.Qty)
	if sellQty <= 0 {
		return nil
	}

	out, err := s.client.PlaceOrder(ctx, OrderRequest{
		Symbol:        s.cfg.Symbol,
		Qty:           fmt.Sprintf("%.6f", sellQty),
		Side:          "sell",
		Type:          "market",
		TimeInForce:   "day",
		ExtendedHours: false,
		ClientOrderID: fmt.Sprintf("open-sell-%s-%d", s.cfg.Symbol, time.Now().UnixNano()),
	})
	if err == nil {
		s.pendingSellID = out.ID
	}
	return err
}

func (s *OpenCloseStrategy) executeBuyBeforeClose(ctx context.Context, acct *Account, dateStr string) error {
	if hasActiveOrder(ctx, s.client, s.cfg.Symbol) {
		return nil
	}
	qtyHeld, err := currentPositionQty(ctx, s.client, s.cfg.Symbol)
	if err != nil || qtyHeld > 0 {
		return err
	}
	price, err := s.client.GetReferencePrice(ctx, s.cfg.Symbol)
	if err != nil {
		return err
	}
	notional := s.cfg.Qty * price
	if parseFloatString(acct.BuyingPower) < notional {
		return errors.New("open-close: insufficient buying power")
	}

	out, err := s.client.PlaceOrder(ctx, OrderRequest{
		Symbol:        s.cfg.Symbol,
		Qty:           fmt.Sprintf("%.6f", s.cfg.Qty),
		Side:          "buy",
		Type:          "market",
		TimeInForce:   "day",
		ExtendedHours: false,
		ClientOrderID: fmt.Sprintf("close-buy-%s-%d", s.cfg.Symbol, time.Now().UnixNano()),
	})
	if err == nil {
		s.pendingBuyID = out.ID
	}
	return err
}

func filterOrdersBySymbol(orders []Order, symbol string) []Order {
	out := make([]Order, 0, len(orders))
	for _, o := range orders {
		if strings.EqualFold(o.Symbol, symbol) {
			out = append(out, o)
		}
	}
	return out
}

func isActiveOrderStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "open", "new", "accepted", "pending_new", "partially_filled":
		return true
	default:
		return false
	}
}

func hasActiveOrder(ctx context.Context, client *AlpacaClient, symbol string) bool {
	orders, err := client.ListOrders(ctx, "open")
	if err != nil {
		return false
	}
	for _, o := range filterOrdersBySymbol(orders, symbol) {
		if isActiveOrderStatus(o.Status) {
			return true
		}
	}
	return false
}

func currentPositionQty(ctx context.Context, client *AlpacaClient, symbol string) (float64, error) {
	pos, err := client.GetPosition(ctx, symbol)
	if err != nil {
		if isHTTPStatusError(err, http.StatusNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return parseFloatString(pos.Qty), nil
}

// -----------------------
// Bot Core Metrics
// -----------------------

type TradeRecord struct {
	Time          time.Time `json:"time"`
	Symbol        string    `json:"symbol"`
	Side          string    `json:"side"`
	Qty           float64   `json:"qty"`
	Price         float64   `json:"price"`
	OrderID       string    `json:"order_id"`
	ClientOrderID string    `json:"client_order_id"`
	Strategy      string    `json:"strategy"`
}

type PositionLot struct {
	Qty   float64
	Price float64
	Time  time.Time
}

type StrategyStats struct {
	Name          string
	Symbol        string
	TradeCount    int
	RealizedPnL   float64
	UnrealizedPnL float64
	TotalPnL      float64
	ReturnPct     float64
	PositionQty   float64
	AvgCost       float64
	LastPrice     float64
}

type strategyLedger struct {
	lots        map[string][]PositionLot
	realizedPnL float64
	tradeCount  int
}

type Bot struct {
	client            *AlpacaClient
	mu                sync.RWMutex
	strategies        map[string]Strategy
	interval          time.Duration
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	startAt           time.Time
	initialEquity     float64
	errorLog          []ErrorRecord
	errorLogMu        sync.Mutex
	snapshots         []DailySnapshot
	snapshotMu        sync.Mutex
	tradeRecords      []TradeRecord
	seenFillQty       map[string]float64
	globalLots        map[string][]PositionLot
	globalRealizedPnL float64
	lastPrices        map[string]float64
	livePositions     map[string]HoldingSummary
	strategyStats     map[string]*StrategyStats
	strategyLedgers   map[string]*strategyLedger
	isRunning         bool
	stopMu            sync.Mutex
	stopFunc          context.CancelFunc
	runCtx            context.Context
}

func NewBot(client *AlpacaClient, interval time.Duration) *Bot {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Bot{
		client:          client,
		strategies:      map[string]Strategy{},
		interval:        interval,
		seenFillQty:     map[string]float64{},
		globalLots:      map[string][]PositionLot{},
		lastPrices:      map[string]float64{},
		livePositions:   map[string]HoldingSummary{},
		strategyStats:   map[string]*StrategyStats{},
		strategyLedgers: map[string]*strategyLedger{},
	}
}

func (b *Bot) RegisterStrategy(s Strategy) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.strategies[s.Name()] = s
	if _, ok := b.strategyStats[s.Name()]; !ok {
		b.strategyStats[s.Name()] = &StrategyStats{Name: s.Name(), Symbol: s.Symbol()}
	}
	if _, ok := b.strategyLedgers[s.Name()]; !ok {
		b.strategyLedgers[s.Name()] = &strategyLedger{lots: map[string][]PositionLot{}}
	}
}

func (b *Bot) runOnce(ctx context.Context) {
	b.recordSnapshot(ctx)
	_ = b.syncOrderFills(ctx)
	b.mu.RLock()
	symbols := make([]string, 0, len(b.strategies))
	for _, s := range b.strategies {
		symbols = append(symbols, s.Symbol())
	}
	b.mu.RUnlock()

	livePrices := make(map[string]float64)
	for _, sym := range symbols {
		if price, err := b.client.GetReferencePrice(ctx, sym); err == nil && price > 0 {
			livePrices[sym] = price
		}
	}
	positionsFetched := false
	livePositions := make(map[string]HoldingSummary)
	if positions, err := b.client.GetPositions(ctx); err == nil {
		positionsFetched = true
		for _, p := range positions {
			sum := positionToHoldingSummary(p)
			key := strings.ToUpper(strings.TrimSpace(sum.Symbol))
			livePositions[key] = sum
		}
	}
	b.mu.Lock()
	for sym, price := range livePrices {
		b.lastPrices[sym] = price
	}
	if positionsFetched {
		b.livePositions = livePositions
	}
	b.recalcStrategyStatsLocked()
	b.mu.Unlock()

	acct, err := b.client.GetAccount(ctx)
	if err != nil {
		b.logError("system", "fetch account failed: "+err.Error())
		return
	}
	clock, err := b.client.GetClock(ctx)
	if err != nil {
		b.logError("system", "fetch clock failed: "+err.Error())
		return
	}
	b.mu.RLock()
	strategies := make([]Strategy, 0, len(b.strategies))
	for _, s := range b.strategies {
		strategies = append(strategies, s)
	}
	b.mu.RUnlock()
	for _, s := range strategies {
		if err := s.Tick(ctx, acct, clock); err != nil {
			b.logError(s.Name(), err.Error())
		}
	}
}

func positionToHoldingSummary(p Position) HoldingSummary {
	qty := parseFloatString(p.Qty)
	avg := parseFloatString(p.AvgEntryPrice)
	mv := parseFloatString(p.MarketValue)
	upnl := parseFloatString(p.UnrealizedPL)
	cur := parseFloatString(p.CurrentPrice)
	if cur <= 0 && qty > 0 && mv > 0 {
		cur = mv / qty
	}
	return HoldingSummary{
		Symbol:        p.Symbol,
		Qty:           qty,
		AvgEntryPrice: avg,
		MarketValue:   mv,
		UnrealizedPnL: upnl,
		CurrentPrice:  cur,
		Side:          p.Side,
	}
}

func (b *Bot) recordSnapshot(ctx context.Context) {
	acct, err := b.client.GetAccount(ctx)
	if err != nil {
		return
	}
	b.snapshotMu.Lock()
	defer b.snapshotMu.Unlock()
	b.snapshots = append(b.snapshots, DailySnapshot{
		Time:   time.Now().UTC(),
		Equity: parseFloatString(acct.Equity),
	})
	if len(b.snapshots) > maxSnapshots {
		b.snapshots = b.snapshots[len(b.snapshots)-maxSnapshots:]
	}
}

func (b *Bot) logError(strategy, msg string) {
	b.errorLogMu.Lock()
	defer b.errorLogMu.Unlock()
	b.errorLog = append(b.errorLog, ErrorRecord{Time: time.Now().UTC(), Strategy: strategy, Error: msg})
	if len(b.errorLog) > maxErrorLogLen {
		b.errorLog = b.errorLog[len(b.errorLog)-maxErrorLogLen:]
	}
}

func (b *Bot) syncOrderFills(ctx context.Context) error {
	orders, err := b.client.ListOrders(ctx, "all")
	if err != nil {
		return err
	}

	recentIDs := make(map[string]struct{}, len(orders))

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, o := range orders {
		recentIDs[o.ID] = struct{}{}

		filledQty := parseFloatString(o.FilledQty)
		if filledQty <= 0 || b.seenFillQty[o.ID] >= filledQty {
			continue
		}
		delta := filledQty - b.seenFillQty[o.ID]
		price := parseFloatString(o.FilledAvgPrice)
		if price <= 0 {
			continue
		}
		fillTime := time.Now().UTC()
		if o.FilledAt != nil {
			fillTime = *o.FilledAt
		}
		stratName := detectStrategyName(o.ClientOrderID)
		b.tradeRecords = append(b.tradeRecords, TradeRecord{
			Time:          fillTime,
			Symbol:        o.Symbol,
			Side:          o.Side,
			Qty:           delta,
			Price:         price,
			OrderID:       o.ID,
			ClientOrderID: o.ClientOrderID,
			Strategy:      stratName,
		})
		b.applyGlobalFill(o.Symbol, o.Side, delta, price)
		b.applyStrategyFill(stratName, o.Symbol, o.Side, delta, price)
		b.seenFillQty[o.ID] = filledQty
		b.lastPrices[o.Symbol] = price
	}

	for id := range b.seenFillQty {
		if _, ok := recentIDs[id]; !ok {
			delete(b.seenFillQty, id)
		}
	}

	b.recalcStrategyStatsLocked()
	return nil
}

func detectStrategyName(clientOrderID string) string {
	id := strings.ToLower(strings.TrimSpace(clientOrderID))
	switch {
	case strings.Contains(id, "grid"):
		return "grid"
	case strings.Contains(id, "open-sell"),
		strings.Contains(id, "close-buy"),
		strings.Contains(id, "open-buy"),
		strings.Contains(id, "close-sell"),
		strings.Contains(id, "open-close"),
		strings.Contains(id, "overnight"):
		return "open-close"
	default:
		return "unknown"
	}
}

func (b *Bot) applyGlobalFill(symbol, side string, qty, price float64) {
	if side == "buy" {
		b.globalLots[symbol] = append(b.globalLots[symbol], PositionLot{Qty: qty, Price: price})
	} else {
		b.globalLots[symbol] = consumeLots(b.globalLots[symbol], qty, price, &b.globalRealizedPnL)
	}
}

func (b *Bot) applyStrategyFill(stratName, symbol, side string, qty, price float64) {
	ledger := b.strategyLedgers[stratName]
	if ledger == nil {
		return
	}
	ledger.tradeCount++
	if side == "buy" {
		ledger.lots[symbol] = append(ledger.lots[symbol], PositionLot{Qty: qty, Price: price})
	} else {
		ledger.lots[symbol] = consumeLots(ledger.lots[symbol], qty, price, &ledger.realizedPnL)
	}
}

func consumeLots(lots []PositionLot, sellQty, sellPrice float64, pnl *float64) []PositionLot {
	remaining := sellQty
	newLots := []PositionLot{}
	for _, lot := range lots {
		if remaining <= 0 {
			newLots = append(newLots, lot)
			continue
		}
		use := math.Min(remaining, lot.Qty)
		*pnl += use * (sellPrice - lot.Price)
		lot.Qty -= use
		remaining -= use
		if lot.Qty > 1e-6 {
			newLots = append(newLots, lot)
		}
	}
	return newLots
}

func (b *Bot) recalcStrategyStatsLocked() {
	for name, stat := range b.strategyStats {
		ledger := b.strategyLedgers[name]
		if ledger == nil {
			continue
		}
		stat.TradeCount = ledger.tradeCount
		stat.RealizedPnL = ledger.realizedPnL

		live, ok := b.livePositions[strings.ToUpper(strings.TrimSpace(stat.Symbol))]
		if ok {
			actualQty := live.Qty
			if actualQty <= 0 {
				ledger.lots[stat.Symbol] = nil
			} else {
				ledgerQty := 0.0
				for _, lot := range ledger.lots[stat.Symbol] {
					ledgerQty += lot.Qty
				}
				if ledgerQty > actualQty+1e-6 {
					ledger.lots[stat.Symbol] = []PositionLot{{Qty: actualQty, Price: live.AvgEntryPrice}}
				}
			}
			stat.PositionQty = live.Qty
			stat.AvgCost = live.AvgEntryPrice
			last := live.CurrentPrice
			if last <= 0 {
				last = b.lastPrices[stat.Symbol]
			}
			stat.LastPrice = last
			if live.UnrealizedPnL != 0 || live.Qty == 0 {
				stat.UnrealizedPnL = live.UnrealizedPnL
			} else if live.Qty > 0 && stat.AvgCost > 0 && last > 0 {
				if strings.ToLower(strings.TrimSpace(live.Side)) == "short" {
					stat.UnrealizedPnL = live.Qty * (stat.AvgCost - last)
				} else {
					stat.UnrealizedPnL = live.Qty * (last - stat.AvgCost)
				}
			} else {
				stat.UnrealizedPnL = 0
			}
			stat.TotalPnL = stat.RealizedPnL + stat.UnrealizedPnL
			continue
		}

		qty, cost, unrealized := 0.0, 0.0, 0.0
		mark := b.lastPrices[stat.Symbol]
		for _, lot := range ledger.lots[stat.Symbol] {
			qty += lot.Qty
			cost += lot.Qty * lot.Price
			if mark > 0 {
				unrealized += lot.Qty * (mark - lot.Price)
			}
		}
		if qty > 0 {
			stat.AvgCost = cost / qty
		} else {
			stat.AvgCost = 0
		}
		stat.PositionQty = qty
		stat.LastPrice = mark
		stat.UnrealizedPnL = unrealized
		stat.TotalPnL = stat.RealizedPnL + stat.UnrealizedPnL
	}
}

// -----------------------
// Data Retrieval Methods
// -----------------------

func (b *Bot) AccountHistory() []DailySnapshot {
	b.snapshotMu.Lock()
	defer b.snapshotMu.Unlock()
	cp := make([]DailySnapshot, len(b.snapshots))
	copy(cp, b.snapshots)
	return cp
}

func (b *Bot) ErrorLog() []ErrorRecord {
	b.errorLogMu.Lock()
	defer b.errorLogMu.Unlock()
	cp := make([]ErrorRecord, len(b.errorLog))
	copy(cp, b.errorLog)
	return cp
}

func (b *Bot) StrategyConfigs() map[string]interface{} {
	b.mu.RLock()
	defer b.mu.RUnlock()
	configs := make(map[string]interface{})
	for name, s := range b.strategies {
		configs[name] = s.Config()
	}
	return configs
}

func (b *Bot) AllOrders(ctx context.Context) ([]OrderSummary, error) {
	orders, err := b.client.ListOrders(ctx, "all")
	if err != nil {
		return nil, err
	}
	out := make([]OrderSummary, 0, len(orders))
	for _, o := range orders {
		createdAt := ""
		if o.CreatedAt != nil {
			createdAt = o.CreatedAt.Format(time.RFC3339)
		}
		strategy := detectStrategyName(o.ClientOrderID)
		out = append(out, OrderSummary{
			ID:            o.ID,
			ClientOrderID: o.ClientOrderID,
			Symbol:        o.Symbol,
			Side:          o.Side,
			Type:          o.Type,
			Qty:           parseFloatString(o.Qty),
			FilledQty:     parseFloatString(o.FilledQty),
			LimitPrice:    parseFloatString(o.LimitPrice),
			Status:        o.Status,
			CreatedAt:     createdAt,
			Strategy:      strategy,
		})
	}
	return out, nil
}

func (b *Bot) LatestPrices() map[string]float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	cp := make(map[string]float64, len(b.lastPrices))
	for k, v := range b.lastPrices {
		cp[k] = v
	}
	return cp
}

func (b *Bot) TotalAssets(ctx context.Context) (map[string]any, error) {
	acct, err := b.client.GetAccount(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"equity":       parseFloatString(acct.Equity),
		"cash":         parseFloatString(acct.Cash),
		"buying_power": parseFloatString(acct.BuyingPower),
	}, nil
}

func (b *Bot) Positions(ctx context.Context) ([]HoldingSummary, error) {
	positions, err := b.client.GetPositions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]HoldingSummary, 0, len(positions))
	for _, p := range positions {
		out = append(out, positionToHoldingSummary(p))
	}
	return out, nil
}

func (b *Bot) Trades() []TradeRecord {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]TradeRecord, len(b.tradeRecords))
	copy(out, b.tradeRecords)
	return out
}

func (b *Bot) Performance(ctx context.Context) (PerformanceSummary, error) {
	acct, err := b.client.GetAccount(ctx)
	if err != nil {
		return PerformanceSummary{}, err
	}
	currentEquity := parseFloatString(acct.Equity)
	periodEquity := b.initialEquity
	if histEquity, err := b.client.GetPortfolioHistory7D(ctx); err == nil && histEquity > 0 {
		periodEquity = histEquity
	}
	totalPnL := currentEquity - periodEquity
	positions, err := b.client.GetPositions(ctx)
	if err != nil {
		return PerformanceSummary{}, err
	}
	unrealized := 0.0
	for _, p := range positions {
		qty := parseFloatString(p.Qty)
		avg := parseFloatString(p.AvgEntryPrice)
		cur := parseFloatString(p.CurrentPrice)
		side := strings.ToLower(strings.TrimSpace(p.Side))
		if qty <= 0 || avg <= 0 || cur <= 0 {
			continue
		}
		if side == "short" {
			unrealized += qty * (avg - cur)
		} else {
			unrealized += qty * (cur - avg)
		}
	}
	realized := totalPnL - unrealized
	ret := 0.0
	if b.initialEquity > 0 {
		ret = (currentEquity - b.initialEquity) / b.initialEquity * 100
	}
	return PerformanceSummary{
		InitialEquity: periodEquity,
		CurrentEquity: currentEquity,
		RealizedPnL:   realized,
		UnrealizedPnL: unrealized,
		TotalPnL:      totalPnL,
		ReturnPct:     ret,
	}, nil
}

func (b *Bot) StrategyPerformance(name string, ctx context.Context) (StrategyStats, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	stat, ok := b.strategyStats[name]
	if !ok {
		return StrategyStats{}, fmt.Errorf("strategy %s not found", name)
	}
	return *stat, nil
}

func (b *Bot) StrategySummaries(ctx context.Context) ([]StrategyStats, error) {
	b.mu.RLock()
	keys := make([]string, 0, len(b.strategyStats))
	for name := range b.strategyStats {
		keys = append(keys, name)
	}
	b.mu.RUnlock()
	sort.Strings(keys)
	out := make([]StrategyStats, 0, len(keys))
	for _, name := range keys {
		stat, _ := b.StrategyPerformance(name, ctx)
		out = append(out, stat)
	}
	return out, nil
}

func (b *Bot) Status() map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()
	strats := make([]string, 0, len(b.strategies))
	for k := range b.strategies {
		strats = append(strats, k)
	}
	return map[string]any{
		"started_at":  b.startAt,
		"strategies":  strats,
		"trade_count": len(b.tradeRecords),
	}
}

// -----------------------
// Safe Dashboard Print
// -----------------------

func (b *Bot) StartMonitor(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = printRichDashboard(ctx, b)
			}
		}
	}()
}

func printRichDashboard(ctx context.Context, bot *Bot) error {
	acct, err := bot.client.GetAccount(ctx)
	if err != nil {
		return err
	}
	positions, _ := bot.client.GetPositions(ctx)
	bot.mu.RLock()
	runtimeStr := time.Since(bot.startAt).Round(time.Second).String()
	totalTrades := len(bot.tradeRecords)
	bot.mu.RUnlock()
	equity := parseFloatString(acct.Equity)
	cash := parseFloatString(acct.Cash)
	bp := parseFloatString(acct.BuyingPower)
	fmt.Printf(" 实时量化面板 | 时间: %s | 运行: %s\n", time.Now().Format("15:04:05"), runtimeStr)
	fmt.Printf(" -----------------------------------------------------------------------\n")
	fmt.Printf(" 资产: $%-.2f | 现金: $%-.2f | 购买力: $%-.2f\n", equity, cash, bp)
	fmt.Printf(" 交易笔数: %d 笔 \n", totalTrades)
	fmt.Printf("\n [ 策略状态 ]\n")
	summaries, _ := bot.StrategySummaries(ctx)
	for _, stat := range summaries {
		fmt.Printf("  %-12s | 标的: %-5s | 持仓: %-6.2f | PnL: $%+.2f \n",
			stat.Name, stat.Symbol, stat.PositionQty, stat.TotalPnL)
	}
	if len(positions) > 0 {
		fmt.Printf("\n [ 实时仓位 ]\n")
		for _, p := range positions {
			fmt.Printf("   %-5s | 数量: %-6s | 均价: $%-.2f | 现价: $%-.2f \n",
				p.Symbol, p.Qty, parseFloatString(p.AvgEntryPrice), parseFloatString(p.CurrentPrice))
		}
	}
	fmt.Printf("========================================================================\n")
	return nil
}

type Bar struct {
	Time   string  `json:"t"`
	Open   float64 `json:"o"`
	High   float64 `json:"h"`
	Low    float64 `json:"l"`
	Close  float64 `json:"c"`
	Volume float64 `json:"v"`
}

type BarView struct {
	Time   float64 `json:"t"`
	Open   float64 `json:"o"`
	High   float64 `json:"h"`
	Low    float64 `json:"l"`
	Close  float64 `json:"c"`
	Volume float64 `json:"v"`
}

type BarsResponse struct {
	Bars          []Bar  `json:"bars"`
	Symbol        string `json:"symbol"`
	NextPageToken string `json:"next_page_token"`
}

func (c *AlpacaClient) GetBars(ctx context.Context, symbol, timeframe string, start, end time.Time, limit int) ([]Bar, error) {
	u, _ := url.Parse(fmt.Sprintf("%s/v2/stocks/%s/bars", c.dataURL, symbol))
	q := u.Query()
	q.Set("timeframe", timeframe)
	if !start.IsZero() {
		q.Set("start", start.Format(time.RFC3339))
	}
	if !end.IsZero() {
		q.Set("end", end.Format(time.RFC3339))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	q.Set("feed", c.feed)
	u.RawQuery = q.Encode()
	var resp BarsResponse
	if err := c.doJSON(ctx, http.MethodGet, u.String(), nil, &resp); err != nil {
		return nil, err
	}
	return resp.Bars, nil
}

func (b *Bot) GetHistoricalBars(ctx context.Context, symbol string) ([]BarView, error) {
	end := time.Now().UTC()
	start := end.AddDate(0, 0, -3)
	bars, err := b.client.GetBars(ctx, symbol, "1Hour", start, end, 500)
	if err != nil {
		return nil, err
	}
	result := make([]BarView, 0, len(bars))
	for _, bar := range bars {
		t, err := time.Parse(time.RFC3339, bar.Time)
		if err != nil {
			continue
		}
		result = append(result, BarView{
			Time:   float64(t.Unix()),
			Open:   bar.Open,
			High:   bar.High,
			Low:    bar.Low,
			Close:  bar.Close,
			Volume: bar.Volume,
		})
	}
	return result, nil
}

func (b *Bot) Start(ctx context.Context) error {
	b.mu.Lock()
	if b.isRunning {
		b.mu.Unlock()
		return errors.New("bot already running")
	}
	acct, err := b.client.GetAccount(ctx)
	if err != nil {
		b.mu.Unlock()
		return err
	}
	b.startAt = time.Now().UTC()
	b.initialEquity = parseFloatString(acct.Equity)
	runCtx, cancel := context.WithCancel(ctx)
	b.runCtx = runCtx
	b.stopFunc = cancel
	b.isRunning = true
	b.mu.Unlock()
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		ticker := time.NewTicker(b.interval)
		defer ticker.Stop()
		b.runOnce(runCtx)
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				b.runOnce(runCtx)
			}
		}
	}()
	return nil
}

func (b *Bot) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopFunc != nil {
		b.stopFunc()
		b.stopFunc = nil
	}
	b.isRunning = false
}

func (b *Bot) IsRunning() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.isRunning
}

func (b *Bot) Restart(ctx context.Context) error {
	b.Stop()
	b.wg.Wait()
	return b.Start(ctx)
}

func (b *Bot) LiquidateAll(ctx context.Context) error {
	log.Println("正在停止机器人策略轮询...")
	b.Stop()
	b.wg.Wait()
	if err := b.client.CloseAllPositions(ctx); err != nil {
		return fmt.Errorf("一键清仓接口调用失败: %w", err)
	}
	deadline := time.After(15 * time.Second)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			remaining, err := b.client.GetPositions(ctx)
			if err == nil && len(remaining) > 0 {
				syms := make([]string, 0, len(remaining))
				for _, p := range remaining {
					syms = append(syms, p.Symbol)
				}
				return fmt.Errorf("一键平仓指令下发成功，但 15s 内未完全成交，残留持仓标的: %v", syms)
			}
			log.Println("一键清仓完毕（超时校验通过，无残留仓位）。")
			return nil
		case <-tick.C:
			remaining, err := b.client.GetPositions(ctx)
			if err == nil && len(remaining) == 0 {
				log.Println("⚡ 仓位已成功归零，平仓完毕。")
				return nil
			}
			log.Println("正在等待仓位清空成交中...")
		}
	}
}
