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

	"github.com/gorilla/websocket"
)

const (
	defaultBaseURL      = "https://paper-api.alpaca.markets"
	defaultDataURL      = "https://data.alpaca.markets"
	maxErrorLogLen      = 200
	maxSnapshots        = 2000
	maxTradeRecords     = 10000
	processBuildVersion = "grid-4h-regime-gtc-v7"

	priceStaleThreshold = 45 * time.Second
	priceRESTTimeout    = 5 * time.Second

	tradePreferenceWindow = 20 * time.Second
	maxQuoteSpreadPct     = 0.03

	indicatorCacheTTL = 5 * time.Minute
	ma20ValueCacheTTL = 30 * time.Minute
	riskStateCacheTTL = 1 * time.Minute
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
	IsPaper     bool
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

	baseURL := getenvDefault("APCA_BASE_URL", defaultBaseURL)
	isPaper := strings.Contains(strings.ToLower(baseURL), "paper") || strings.Contains(strings.ToLower(baseURL), "sandbox")
	if !isPaper && strings.TrimSpace(os.Getenv("LIVE_TRADING_ACK")) != "I_UNDERSTAND_REAL_MONEY_RISK" {
		log.Fatal("live endpoint refused: set LIVE_TRADING_ACK=I_UNDERSTAND_REAL_MONEY_RISK after completing paper and risk checks")
	}

	return Config{
		APIKey:      apiKey,
		APISecret:   apiSecret,
		BaseURL:     baseURL,
		DataURL:     getenvDefault("APCA_DATA_URL", defaultDataURL),
		DataFeed:    getenvDefault("APCA_DATA_FEED", "iex"),
		HTTPTimeout: 12 * time.Second,
		Interval:    time.Duration(intervalSec) * time.Second,
		IsPaper:     isPaper,
	}
}

func getenvDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func getenvFloat(key string, fallback float64) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		log.Printf("invalid %s=%q; using %.6f", key, raw, fallback)
		return fallback
	}
	return value
}

func getenvBool(key string, fallback bool) bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if raw == "" {
		return fallback
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		log.Printf("invalid %s=%q; using %t", key, raw, fallback)
		return fallback
	}
}

// -----------------------
// Alpaca REST client
// -----------------------

type AlpacaClient struct {
	baseURL    string
	dataURL    string
	feed       string
	isPaper    bool
	apiKey     string
	secret     string
	client     *http.Client
	priceCache *PriceCache

	wsMu          sync.Mutex
	marketCancel  context.CancelFunc
	tradingCancel context.CancelFunc
	marketConn    *websocket.Conn
	tradingConn   *websocket.Conn
}

func NewAlpacaClient(cfg Config) *AlpacaClient {
	return &AlpacaClient{
		baseURL:    strings.TrimRight(cfg.BaseURL, "/"),
		dataURL:    strings.TrimRight(cfg.DataURL, "/"),
		feed:       cfg.DataFeed,
		isPaper:    cfg.IsPaper,
		apiKey:     cfg.APIKey,
		secret:     cfg.APISecret,
		client:     &http.Client{Timeout: cfg.HTTPTimeout},
		priceCache: NewPriceCache(),
	}
}

func (c *AlpacaClient) TradingMode() string {
	if c != nil && c.isPaper {
		return "paper"
	}
	return "live"
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
	Timestamp    []int64    `json:"timestamp"`
	Equity       []float64  `json:"equity"`
	ProfitLoss   []*float64 `json:"profit_loss"`
	ProfitLossPC []*float64 `json:"profit_loss_pct"`
	BaseValue    float64    `json:"base_value"`
}

type PortfolioPeriod struct {
	StartEquity float64
	TotalPnL    float64
	ReturnPct   float64 // percentage points, e.g. 1.25 means 1.25%
	HasPnL      bool
	HasReturn   bool
}

func (c *AlpacaClient) GetPortfolioHistory7D(ctx context.Context) (PortfolioPeriod, error) {
	u, _ := url.Parse(c.baseURL + "/v2/account/portfolio/history")
	q := u.Query()
	q.Set("period", "7D")
	q.Set("timeframe", "1D")
	u.RawQuery = q.Encode()
	var out PortfolioHistory
	if err := c.doJSON(ctx, http.MethodGet, u.String(), nil, &out); err != nil {
		return PortfolioPeriod{}, err
	}
	if len(out.Equity) == 0 {
		return PortfolioPeriod{}, errors.New("no portfolio history data")
	}

	result := PortfolioPeriod{StartEquity: out.BaseValue}
	if len(out.Timestamp) == len(out.Equity) && len(out.Timestamp) > 0 {
		oldestIdx := 0
		for i := 1; i < len(out.Timestamp); i++ {
			if out.Timestamp[i] < out.Timestamp[oldestIdx] {
				oldestIdx = i
			}
		}
		if result.StartEquity <= 0 {
			result.StartEquity = out.Equity[oldestIdx]
		}
	} else if result.StartEquity <= 0 {
		result.StartEquity = out.Equity[0]
	}

	// Alpaca's profit_loss series is cash-flow adjusted and is therefore more
	// reliable than subtracting two equity points when deposits/withdrawals exist.
	if value, ok := lastNonNilFloat(out.ProfitLoss); ok {
		result.TotalPnL = value
		result.HasPnL = true
	}
	if value, ok := lastNonNilFloat(out.ProfitLossPC); ok {
		result.ReturnPct = value * 100
		result.HasReturn = true
	} else if result.StartEquity > 0 {
		result.ReturnPct = result.TotalPnL / result.StartEquity * 100
		result.HasReturn = result.HasPnL
	}
	return result, nil
}

func lastNonNilFloat(values []*float64) (float64, bool) {
	for i := len(values) - 1; i >= 0; i-- {
		if values[i] != nil && !math.IsNaN(*values[i]) && !math.IsInf(*values[i], 0) {
			return *values[i], true
		}
	}
	return 0, false
}

type Account struct {
	ID                       string `json:"id"`
	Status                   string `json:"status"`
	Equity                   string `json:"equity"`
	Cash                     string `json:"cash"`
	BuyingPower              string `json:"buying_power"`
	NonMarginableBuyingPower string `json:"non_marginable_buying_power"`
	LongMarketValue          string `json:"long_market_value"`
	ShortMarketValue         string `json:"short_market_value"`
	MaintenanceMargin        string `json:"maintenance_margin"`
	TradingBlocked           bool   `json:"trading_blocked"`
	AccountBlocked           bool   `json:"account_blocked"`
	TradeSuspendedByUser     bool   `json:"trade_suspended_by_user"`
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
	UpdatedAt      *time.Time `json:"updated_at"`
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
		AskPrice  float64   `json:"ap"`
		BidPrice  float64   `json:"bp"`
		Timestamp time.Time `json:"t"`
	} `json:"quote"`
}

type TradeResponse struct {
	Trade struct {
		Price     float64   `json:"p"`
		Timestamp time.Time `json:"t"`
	} `json:"trade"`
}

func (c *AlpacaClient) GetLatestQuotePrice(ctx context.Context, symbol string) (float64, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return 0, errors.New("symbol is required")
	}

	u, err := url.Parse(fmt.Sprintf("%s/v2/stocks/%s/quotes/latest", c.dataURL, url.PathEscape(symbol)))
	if err != nil {
		return 0, err
	}
	if feed := strings.TrimSpace(c.feed); feed != "" {
		q := u.Query()
		q.Set("feed", feed)
		u.RawQuery = q.Encode()
	}

	var out QuoteResponse
	if err := c.doJSON(ctx, http.MethodGet, u.String(), nil, &out); err != nil {
		return 0, err
	}

	price, reason := quoteReferencePrice(out.Quote.BidPrice, out.Quote.AskPrice)
	if price <= 0 {
		return 0, fmt.Errorf(
			"latest quote for %s is unusable: %s (bid=%.6f ask=%.6f)",
			symbol, reason, out.Quote.BidPrice, out.Quote.AskPrice,
		)
	}
	return price, nil
}

func (c *AlpacaClient) GetLatestTradePrice(ctx context.Context, symbol string) (float64, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return 0, errors.New("symbol is required")
	}

	u, err := url.Parse(fmt.Sprintf("%s/v2/stocks/%s/trades/latest", c.dataURL, url.PathEscape(symbol)))
	if err != nil {
		return 0, err
	}
	if feed := strings.TrimSpace(c.feed); feed != "" {
		q := u.Query()
		q.Set("feed", feed)
		u.RawQuery = q.Encode()
	}

	var out TradeResponse
	if err := c.doJSON(ctx, http.MethodGet, u.String(), nil, &out); err != nil {
		return 0, err
	}
	if out.Trade.Price <= 0 {
		return 0, fmt.Errorf("latest trade for %s contains no usable price", symbol)
	}
	return out.Trade.Price, nil
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

func (c *AlpacaClient) GetOrderByClientOrderID(ctx context.Context, clientOrderID string) (*Order, error) {
	clientOrderID = strings.TrimSpace(clientOrderID)
	if clientOrderID == "" {
		return nil, errors.New("client order id is required")
	}
	u, err := url.Parse(c.baseURL + "/v2/orders:by_client_order_id")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("client_order_id", clientOrderID)
	u.RawQuery = q.Encode()
	var out Order
	if err := c.doJSON(ctx, http.MethodGet, u.String(), nil, &out); err != nil {
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
	return c.ListOrdersSince(ctx, status, time.Time{})
}

func (c *AlpacaClient) ListOrdersSince(ctx context.Context, status string, after time.Time) ([]Order, error) {
	u, err := url.Parse(c.baseURL + "/v2/orders")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("nested", "true")
	q.Set("limit", "500")
	q.Set("direction", "desc")
	if strings.TrimSpace(status) != "" {
		q.Set("status", strings.TrimSpace(status))
	}
	if !after.IsZero() {
		q.Set("after", after.UTC().Format(time.RFC3339))
	}
	u.RawQuery = q.Encode()

	var out []Order
	if err := c.doJSON(ctx, http.MethodGet, u.String(), nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *AlpacaClient) CancelOrder(ctx context.Context, orderID string) error {
	return c.doJSON(ctx, http.MethodDelete, c.baseURL+"/v2/orders/"+orderID, nil, nil)
}

func (c *AlpacaClient) GetReferencePrice(ctx context.Context, symbol string) (float64, error) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return 0, errors.New("symbol is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if price, ok := c.LatestPrice(symbol); ok && price > 0 {
		return price, nil
	}

	// Last trade is the primary fallback. Give trade and quote their own timeout;
	// a slow trade endpoint must not consume the entire budget for the quote.
	tradeCtx, tradeCancel := context.WithTimeout(ctx, priceRESTTimeout)
	tradePrice, tradeErr := c.GetLatestTradePrice(tradeCtx, symbol)
	tradeCancel()
	if tradeErr == nil && tradePrice > 0 {
		c.CacheMarketPrice(symbol, tradePrice, "rest-trade", time.Time{})
		return tradePrice, nil
	}

	quoteCtx, quoteCancel := context.WithTimeout(ctx, priceRESTTimeout)
	quotePrice, quoteErr := c.GetLatestQuotePrice(quoteCtx, symbol)
	quoteCancel()
	if quoteErr == nil && quotePrice > 0 {
		c.CacheMarketPrice(symbol, quotePrice, "rest-quote", time.Time{})
		return quotePrice, nil
	}

	return 0, fmt.Errorf(
		"no usable market price for %s (latest trade: %v; latest quote: %v)",
		symbol, tradeErr, quoteErr,
	)
}

func (c *AlpacaClient) PlaceOrder(ctx context.Context, req OrderRequest) (*Order, error) {
	var out Order
	if err := c.doJSON(ctx, http.MethodPost, c.baseURL+"/v2/orders", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *AlpacaClient) PlaceOrderIdempotent(ctx context.Context, req OrderRequest) (*Order, error) {
	if strings.TrimSpace(req.ClientOrderID) == "" {
		return nil, errors.New("idempotent order requires client_order_id")
	}
	out, submitErr := c.PlaceOrder(ctx, req)
	if submitErr == nil {
		return out, nil
	}

	// A timeout can occur after the broker accepted the order. Reconcile by the
	// stable client id using a detached, read-only context before reporting failure.
	lookupCtx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	existing, lookupErr := c.GetOrderByClientOrderID(lookupCtx, req.ClientOrderID)
	if lookupErr == nil && existing != nil {
		status := strings.ToLower(strings.TrimSpace(existing.Status))
		if status != "rejected" && status != "canceled" && status != "expired" {
			return existing, nil
		}
	}
	return nil, fmt.Errorf("submit order %s failed: %w (reconcile: %v)", req.ClientOrderID, submitErr, lookupErr)
}

type BulkActionResult struct {
	ID     string          `json:"id"`
	Symbol string          `json:"symbol"`
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

func (c *AlpacaClient) CancelAllOrders(ctx context.Context) ([]BulkActionResult, error) {
	var out []BulkActionResult
	if err := c.doJSON(ctx, http.MethodDelete, c.baseURL+"/v2/orders", nil, &out); err != nil {
		return nil, err
	}
	return out, validateBulkResults("cancel orders", out)
}

func (c *AlpacaClient) CloseAllPositions(ctx context.Context) ([]BulkActionResult, error) {
	var out []BulkActionResult
	if err := c.doJSON(ctx, http.MethodDelete, c.baseURL+"/v2/positions?cancel_orders=true", nil, &out); err != nil {
		return nil, err
	}
	return out, validateBulkResults("close positions", out)
}

func validateBulkResults(action string, results []BulkActionResult) error {
	failed := make([]string, 0)
	for _, result := range results {
		if result.Status >= 200 && result.Status < 300 {
			continue
		}
		target := strings.TrimSpace(result.Symbol)
		if target == "" {
			target = strings.TrimSpace(result.ID)
		}
		failed = append(failed, fmt.Sprintf("%s(status=%d body=%s)", target, result.Status, truncateLog(string(result.Body), 160)))
	}
	if len(failed) > 0 {
		return fmt.Errorf("%s partially failed: %s", action, strings.Join(failed, "; "))
	}
	return nil
}

func parseFloatString(s string) float64 {
	f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f
}

func priceIncrement(price float64) float64 {
	if price < 1 {
		return 0.0001
	}
	return 0.01
}

func normalizeLimitPrice(price float64, side string) float64 {
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0
	}
	inc := priceIncrement(price)
	scaled := price / inc
	if strings.EqualFold(side, "sell") {
		return math.Ceil(scaled-1e-9) * inc
	}
	return math.Floor(scaled+1e-9) * inc
}

func formatLimitPrice(price float64, side string) string {
	normalized := normalizeLimitPrice(price, side)
	if normalized < 1 {
		return fmt.Sprintf("%.4f", normalized)
	}
	return fmt.Sprintf("%.2f", normalized)
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
	Period        string  `json:"period"`
	InitialEquity float64 `json:"initial_equity"`
	CurrentEquity float64 `json:"current_equity"`
	RealizedPnL   float64 `json:"realized_pnl"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`
	TotalPnL      float64 `json:"total_pnl"`
	ReturnPct     float64 `json:"return_pct"`
}

type RiskConfig struct {
	CashOnly             bool
	MaxDailyLossPct      float64
	MaxDrawdownPct       float64
	MaxGrossExposurePct  float64
	MaxSymbolExposurePct float64
	LiquidateOnRiskHalt  bool
	StateFile            string
}

func LoadRiskConfig() RiskConfig {
	cfg := RiskConfig{
		CashOnly:             getenvBool("RISK_CASH_ONLY", true),
		MaxDailyLossPct:      getenvFloat("RISK_MAX_DAILY_LOSS_PCT", 0.01),
		MaxDrawdownPct:       getenvFloat("RISK_MAX_DRAWDOWN_PCT", 0.05),
		MaxGrossExposurePct:  getenvFloat("RISK_MAX_GROSS_EXPOSURE_PCT", 0.75),
		MaxSymbolExposurePct: getenvFloat("RISK_MAX_SYMBOL_EXPOSURE_PCT", 0.5),
		LiquidateOnRiskHalt:  getenvBool("RISK_LIQUIDATE_ON_HALT", true),
		StateFile:            getenvDefault("RISK_STATE_FILE", "bot_risk_state.json"),
	}
	for name, value := range map[string]*float64{
		"RISK_MAX_DAILY_LOSS_PCT":      &cfg.MaxDailyLossPct,
		"RISK_MAX_DRAWDOWN_PCT":        &cfg.MaxDrawdownPct,
		"RISK_MAX_GROSS_EXPOSURE_PCT":  &cfg.MaxGrossExposurePct,
		"RISK_MAX_SYMBOL_EXPOSURE_PCT": &cfg.MaxSymbolExposurePct,
	} {
		if *value < 0 || *value > 1 {
			log.Printf("%s must be between 0 and 1; risk-safe default retained", name)
			switch name {
			case "RISK_MAX_DAILY_LOSS_PCT":
				*value = 0.01
			case "RISK_MAX_DRAWDOWN_PCT":
				*value = 0.05
			case "RISK_MAX_GROSS_EXPOSURE_PCT":
				*value = 0.35
			case "RISK_MAX_SYMBOL_EXPOSURE_PCT":
				*value = 0.12
			}
		}
	}
	return cfg
}

type RiskState struct {
	Halted           bool    `json:"halted"`
	Reason           string  `json:"reason"`
	SessionDay       string  `json:"session_day"`
	SessionEquity    float64 `json:"session_equity"`
	HighWaterEquity  float64 `json:"high_water_equity"`
	DailyLossPct     float64 `json:"daily_loss_pct"`
	DrawdownPct      float64 `json:"drawdown_pct"`
	GrossExposurePct float64 `json:"gross_exposure_pct"`
	UpdatedAt        string  `json:"updated_at"`
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
	ID             string  `json:"id"`
	ClientOrderID  string  `json:"client_order_id"`
	Symbol         string  `json:"symbol"`
	Side           string  `json:"side"`
	Type           string  `json:"type"`
	Qty            float64 `json:"qty"`
	FilledQty      float64 `json:"filled_qty"`
	LimitPrice     float64 `json:"limit_price"`
	FilledAvgPrice float64 `json:"filled_avg_price"`
	Status         string  `json:"status"`
	CreatedAt      string  `json:"created_at"`
	FilledAt       string  `json:"filled_at"`
	UpdatedAt      string  `json:"updated_at"`
	Strategy       string  `json:"strategy"`
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

type FillAwareStrategy interface {
	OnFill(orderID, side string, qty, price float64, at time.Time)
}

type BaseStrategy struct {
	client *AlpacaClient
	mu     sync.Mutex
}

// -----------------------
// Grid Strategy
// -----------------------

type GridConfig struct {
	Symbol              string
	Levels              int
	SpacingPct          float64
	MinSpacingPct       float64
	MaxSpacingPct       float64
	QtyPerOrder         float64
	SeedQty             float64
	OrderNotional       float64 // preferred live sizing; whole-share quantity is derived per price
	MaxPositionNotional float64
	MinPrice            float64
	MaxPrice            float64
	RecenterPct         float64
	MaxPositionQty      float64 // 最大仓位限制
	UseTrendFilter      bool    // 网格趋势过滤开关
	ATRPeriod           int
	ATRMultiplier       float64
	CenterMode          string
	CenterEMAPeriod     int
	CenterVWAPLookback  int
	ADXPeriod           int
	ADXTrendThreshold   float64
	ADXRangeThreshold   float64
	EntryFilterMode     string // off, soft, strict; soft avoids a single-indicator veto
	MACDFastPeriod      int
	MACDSlowPeriod      int
	MACDSignalPeriod    int
	MACDBearishPct      float64 // strong-bearish histogram threshold as a fraction of price
	MinBearishBuyLevel  int     // in soft mode, keep deeper grid orders during strong downtrends

	DailyBuyNotionalLimit float64       // 当日已成交 + 未成交买单的最大金额
	BuyCooldown           time.Duration // 从最近一次实际买入成交开始计算
	RebuildCooldown       time.Duration // 网格重建冷却

	// Safety controls. Zero values are replaced with conservative defaults.
	MinProfitPct          float64 // 卖价至少高于持仓均价的比例，默认 0.5%
	BuyMarketBufferPct    float64 // 新买单至少低于当前市价的比例，默认 0.1%
	SellMarketBufferPct   float64 // 卖价至少高于当前市价的比例，默认 0.1%
	MaxSeedPremiumPct     float64 // 市价高于中心过多时不追种子仓，默认 0.5%
	MaxOpenBuyOrders      int     // 同一策略最多同时存在的买单，默认 min(levels, 3)
	AllowOrdersWhenClosed bool    // 默认 false，仅在正常交易时段创建或重建订单

	// 4H regime controls. The grid anchor changes only after a confirmed regime
	// transition; ordinary price drift and a new trading day do not rebuild it.
	RegimeTimeframe             string
	RegimeFastEMAPeriod         int
	RegimeSlowEMAPeriod         int
	RegimeATRPeriod             int
	RegimeADXPeriod             int
	RegimeTrendADX              float64
	RegimeRangeADX              float64
	RegimeBreakoutLookback      int
	RegimeBreakoutATRMultiplier float64
	RegimeConfirmBars           int
	RegimeRefreshInterval       time.Duration
	BullBuyLevels               int
	UseDayOrders                bool   // false => GTC; retained only as an explicit escape hatch
	GridStateFile               string // persists the active 4H anchor across restarts
}

type gridRegime string

const (
	gridRegimeRange gridRegime = "range"
	gridRegimeBull  gridRegime = "bull_breakout"
	gridRegimeBear  gridRegime = "bear_breakdown"
)

type fourHourSnapshot struct {
	Regime       gridRegime
	BarTime      time.Time
	Close        float64
	FastEMA      float64
	SlowEMA      float64
	ATR          float64
	ADX          float64
	PlusDI       float64
	MinusDI      float64
	BreakoutHigh float64
	BreakoutLow  float64
	Center       float64
	Spacing      float64
	Reason       string
}

type gridPersistentState struct {
	Version     int        `json:"version"`
	Symbol      string     `json:"symbol"`
	Regime      gridRegime `json:"regime"`
	Center      float64    `json:"center"`
	Spacing     float64    `json:"spacing"`
	LastBarTime string     `json:"last_bar_time"`
	UpdatedAt   string     `json:"updated_at"`
}

type gridInventoryLot struct {
	BuyOrderID string
	Qty        float64
	EntryPrice float64
	BoughtAt   time.Time
}

type GridStrategy struct {
	BaseStrategy
	cfg            GridConfig
	initialized    bool
	centerPrice    float64
	currentSpacing float64
	lastBuildAt    time.Time
	lastBuyAt      time.Time
	lastRebuildAt  time.Time

	pendingOrders    map[string]time.Time
	uncertainIntents map[string]orderIntent

	dailyBuyDate       string
	dailyBuyNotional   float64
	riskStateUpdatedAt time.Time

	rebuildPending bool
	pendingCenter  float64
	pendingSpacing float64

	ma20Mu             sync.Mutex
	ma20CacheDay       string
	ma20CacheAllow     bool
	ma20Value          float64
	ma20LastPrice      float64
	ma20LastCheckedAt  time.Time
	ma20ValueUpdatedAt time.Time

	lastBuyBlockReason  string
	lastBuyBlockLogAt   time.Time
	lastMarketClosedLog time.Time

	indicatorMu        sync.Mutex
	indicatorExpiresAt time.Time
	indicatorUpdatedAt time.Time
	cachedATR          float64
	cachedEMA          float64
	cachedVWAP         float64
	cachedADX          float64
	cachedPlusDI       float64
	cachedMinusDI      float64
	cachedMACD         float64
	cachedMACDSignal   float64
	cachedMACDHist     float64
	adxMode            string
	entryFilterState   string

	regime             gridRegime
	regimeSnapshot     fourHourSnapshot
	regimeCheckedAt    time.Time
	regimeStateLoaded  bool
	inventoryLots      []gridInventoryLot
}

type orderIntent struct {
	ClientOrderID string
	CreatedAt     time.Time
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
	if cfg.OrderNotional < 0 {
		cfg.OrderNotional = 0
	}
	if cfg.MaxPositionNotional < 0 {
		cfg.MaxPositionNotional = 0
	}
	if cfg.RecenterPct <= 0 {
		cfg.RecenterPct = 0.05
	}
	if cfg.BuyCooldown <= 0 {
		cfg.BuyCooldown = 2 * time.Minute
	}
	if cfg.RebuildCooldown <= 0 {
		cfg.RebuildCooldown = 15 * time.Minute
	}
	if cfg.DailyBuyNotionalLimit <= 0 {
		cfg.DailyBuyNotionalLimit = 1000
	}
	if cfg.MinProfitPct <= 0 {
		cfg.MinProfitPct = 0.005
	}
	if cfg.BuyMarketBufferPct < 0 {
		cfg.BuyMarketBufferPct = 0
	}
	if cfg.BuyMarketBufferPct == 0 {
		cfg.BuyMarketBufferPct = 0.001
	}
	if cfg.SellMarketBufferPct < 0 {
		cfg.SellMarketBufferPct = 0
	}
	if cfg.SellMarketBufferPct == 0 {
		cfg.SellMarketBufferPct = 0.001
	}
	if cfg.MaxSeedPremiumPct <= 0 {
		cfg.MaxSeedPremiumPct = 0.005
	}
	if cfg.MaxOpenBuyOrders <= 0 {
		cfg.MaxOpenBuyOrders = minInt(cfg.Levels, 3)
	}
	if cfg.MinSpacingPct < 0 {
		cfg.MinSpacingPct = 0
	}
	if cfg.MaxSpacingPct > 0 && cfg.MaxSpacingPct < cfg.MinSpacingPct {
		cfg.MaxSpacingPct = 0
	}
	if cfg.ATRPeriod <= 0 {
		cfg.ATRPeriod = 14
	}
	if cfg.ATRMultiplier <= 0 {
		cfg.ATRMultiplier = 1.0
	}
	cfg.CenterMode = strings.ToLower(strings.TrimSpace(cfg.CenterMode))
	if cfg.CenterMode == "" {
		cfg.CenterMode = "price"
	}
	if cfg.CenterEMAPeriod <= 0 {
		cfg.CenterEMAPeriod = 20
	}
	if cfg.CenterVWAPLookback <= 0 {
		cfg.CenterVWAPLookback = 30
	}
	if cfg.ADXPeriod <= 0 {
		cfg.ADXPeriod = 14
	}
	if cfg.ADXTrendThreshold <= 0 {
		cfg.ADXTrendThreshold = 25
	}
	if cfg.ADXRangeThreshold <= 0 || cfg.ADXRangeThreshold >= cfg.ADXTrendThreshold {
		cfg.ADXRangeThreshold = cfg.ADXTrendThreshold * 0.8
	}
	cfg.EntryFilterMode = strings.ToLower(strings.TrimSpace(cfg.EntryFilterMode))
	if cfg.EntryFilterMode == "" {
		cfg.EntryFilterMode = "soft"
	}
	if cfg.EntryFilterMode != "off" && cfg.EntryFilterMode != "soft" && cfg.EntryFilterMode != "strict" {
		cfg.EntryFilterMode = "soft"
	}
	if cfg.MACDFastPeriod <= 0 {
		cfg.MACDFastPeriod = 12
	}
	if cfg.MACDSlowPeriod <= cfg.MACDFastPeriod {
		cfg.MACDSlowPeriod = 26
	}
	if cfg.MACDSignalPeriod <= 0 {
		cfg.MACDSignalPeriod = 9
	}
	if cfg.MACDBearishPct <= 0 {
		cfg.MACDBearishPct = 0.001
	}
	if cfg.MinBearishBuyLevel <= 0 {
		cfg.MinBearishBuyLevel = minInt(3, cfg.Levels)
	}
	if strings.TrimSpace(cfg.RegimeTimeframe) == "" {
		cfg.RegimeTimeframe = "4Hour"
	}
	if cfg.RegimeFastEMAPeriod <= 0 {
		cfg.RegimeFastEMAPeriod = 20
	}
	if cfg.RegimeSlowEMAPeriod <= cfg.RegimeFastEMAPeriod {
		cfg.RegimeSlowEMAPeriod = 50
	}
	if cfg.RegimeATRPeriod <= 0 {
		cfg.RegimeATRPeriod = 14
	}
	if cfg.RegimeADXPeriod <= 0 {
		cfg.RegimeADXPeriod = 14
	}
	if cfg.RegimeTrendADX <= 0 {
		cfg.RegimeTrendADX = 25
	}
	if cfg.RegimeRangeADX <= 0 || cfg.RegimeRangeADX >= cfg.RegimeTrendADX {
		cfg.RegimeRangeADX = 18
	}
	if cfg.RegimeBreakoutLookback < 5 {
		cfg.RegimeBreakoutLookback = 20
	}
	if cfg.RegimeBreakoutATRMultiplier <= 0 {
		cfg.RegimeBreakoutATRMultiplier = 0.25
	}
	if cfg.RegimeConfirmBars < 1 {
		cfg.RegimeConfirmBars = 2
	}
	if cfg.RegimeRefreshInterval <= 0 {
		cfg.RegimeRefreshInterval = 5 * time.Minute
	}
	if cfg.BullBuyLevels <= 0 || cfg.BullBuyLevels > cfg.Levels {
		cfg.BullBuyLevels = minInt(3, cfg.Levels)
	}
	if strings.TrimSpace(cfg.GridStateFile) == "" {
		mode := "unknown"
		if client != nil {
			mode = client.TradingMode()
		}
		cfg.GridStateFile = "grid_state_" + mode + "_" + strings.ToLower(strings.TrimSpace(cfg.Symbol)) + ".json"
	}

	g := &GridStrategy{
		BaseStrategy:     BaseStrategy{client: client},
		cfg:              cfg,
		currentSpacing:   cfg.SpacingPct,
		pendingOrders:    make(map[string]time.Time),
		uncertainIntents: make(map[string]orderIntent),
	}
	g.loadGridState()
	return g
}

func (g *GridStrategy) Name() string {
	return "grid-" + strings.ToUpper(strings.TrimSpace(g.cfg.Symbol))
}

func (g *GridStrategy) Symbol() string { return g.cfg.Symbol }

func (g *GridStrategy) gridTimeInForce() string {
	if g.cfg.UseDayOrders {
		return "day"
	}
	return "gtc"
}

func (g *GridStrategy) loadGridState() {
	path := strings.TrimSpace(g.cfg.GridStateFile)
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("grid %s state read failed: %v", g.cfg.Symbol, err)
		}
		return
	}
	var state gridPersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("grid %s state decode failed: %v", g.cfg.Symbol, err)
		return
	}
	if state.Version != 2 || !strings.EqualFold(state.Symbol, g.cfg.Symbol) || state.Center <= 0 || state.Spacing <= 0 {
		log.Printf("grid %s ignoring incompatible persistent state", g.cfg.Symbol)
		return
	}
	switch state.Regime {
	case gridRegimeRange, gridRegimeBull, gridRegimeBear:
	default:
		return
	}
	g.regime = state.Regime
	g.centerPrice = state.Center
	g.currentSpacing = state.Spacing
	g.initialized = true
	g.regimeStateLoaded = true
	if parsed, err := time.Parse(time.RFC3339, state.LastBarTime); err == nil {
		g.regimeSnapshot.BarTime = parsed
	}
}

func (g *GridStrategy) persistGridStateLocked() {
	path := strings.TrimSpace(g.cfg.GridStateFile)
	if path == "" || g.centerPrice <= 0 || g.currentSpacing <= 0 || g.regime == "" {
		return
	}
	state := gridPersistentState{
		Version:     2,
		Symbol:      strings.ToUpper(strings.TrimSpace(g.cfg.Symbol)),
		Regime:      g.regime,
		Center:      g.centerPrice,
		Spacing:     g.currentSpacing,
		UpdatedAt:   time.Now().UTC().Format(time.RFC3339),
	}
	if !g.regimeSnapshot.BarTime.IsZero() {
		state.LastBarTime = g.regimeSnapshot.BarTime.Format(time.RFC3339)
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		log.Printf("grid %s state encode failed: %v", g.cfg.Symbol, err)
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		log.Printf("grid %s state write failed: %v", g.cfg.Symbol, err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		log.Printf("grid %s state replace failed: %v", g.cfg.Symbol, err)
	}
}

func (g *GridStrategy) Config() map[string]interface{} {
	g.mu.Lock()
	regime := g.regime
	regimeSnapshot := g.regimeSnapshot
	activeCenter := g.centerPrice
	activeSpacing := g.currentSpacing
	lotCount := len(g.inventoryLots)
	lotQty := 0.0
	for _, lot := range g.inventoryLots {
		lotQty += lot.Qty
	}
	g.mu.Unlock()

	g.ma20Mu.Lock()
	ma20Allowed := g.ma20CacheAllow
	ma20Value := g.ma20Value
	ma20Price := g.ma20LastPrice
	ma20CheckedAt := g.ma20LastCheckedAt
	g.ma20Mu.Unlock()

	g.indicatorMu.Lock()
	adxValue := g.cachedADX
	plusDI := g.cachedPlusDI
	minusDI := g.cachedMinusDI
	macdValue := g.cachedMACD
	macdSignal := g.cachedMACDSignal
	macdHist := g.cachedMACDHist
	adxMode := g.adxMode
	entryFilterState := g.entryFilterState
	indicatorUpdatedAt := g.indicatorUpdatedAt
	g.indicatorMu.Unlock()

	cfg := map[string]interface{}{
		"symbol":                   g.cfg.Symbol,
		"levels":                   g.cfg.Levels,
		"spacing_pct":              g.cfg.SpacingPct,
		"min_spacing_pct":          g.cfg.MinSpacingPct,
		"max_spacing_pct":          g.cfg.MaxSpacingPct,
		"qty_per_order":            g.cfg.QtyPerOrder,
		"seed_qty":                 g.cfg.SeedQty,
		"order_notional":           g.cfg.OrderNotional,
		"max_position_notional":    g.cfg.MaxPositionNotional,
		"min_price":                g.cfg.MinPrice,
		"max_price":                g.cfg.MaxPrice,
		"recenter_pct":             g.cfg.RecenterPct,
		"max_position_qty":         g.cfg.MaxPositionQty,
		"use_trend_filter":         g.cfg.UseTrendFilter,
		"atr_period":               g.cfg.ATRPeriod,
		"atr_multiplier":           g.cfg.ATRMultiplier,
		"center_mode":              g.cfg.CenterMode,
		"center_ema_period":        g.cfg.CenterEMAPeriod,
		"center_vwap_lookback":     g.cfg.CenterVWAPLookback,
		"adx_period":               g.cfg.ADXPeriod,
		"adx_trend_threshold":      g.cfg.ADXTrendThreshold,
		"adx_range_threshold":      g.cfg.ADXRangeThreshold,
		"entry_filter_mode":        g.cfg.EntryFilterMode,
		"macd_fast_period":         g.cfg.MACDFastPeriod,
		"macd_slow_period":         g.cfg.MACDSlowPeriod,
		"macd_signal_period":       g.cfg.MACDSignalPeriod,
		"macd_bearish_pct":         g.cfg.MACDBearishPct,
		"min_bearish_buy_level":    g.cfg.MinBearishBuyLevel,
		"daily_buy_notional_limit": g.cfg.DailyBuyNotionalLimit,
		"buy_cooldown":             g.cfg.BuyCooldown.String(),
		"rebuild_cooldown":         g.cfg.RebuildCooldown.String(),
		"min_profit_pct":           g.cfg.MinProfitPct,
		"buy_market_buffer_pct":    g.cfg.BuyMarketBufferPct,
		"sell_market_buffer_pct":   g.cfg.SellMarketBufferPct,
		"max_seed_premium_pct":     g.cfg.MaxSeedPremiumPct,
		"max_open_buy_orders":      g.cfg.MaxOpenBuyOrders,
		"allow_orders_when_closed": g.cfg.AllowOrdersWhenClosed,
		"regime_timeframe":                    g.cfg.RegimeTimeframe,
		"regime_fast_ema_period":              g.cfg.RegimeFastEMAPeriod,
		"regime_slow_ema_period":              g.cfg.RegimeSlowEMAPeriod,
		"regime_atr_period":                   g.cfg.RegimeATRPeriod,
		"regime_adx_period":                   g.cfg.RegimeADXPeriod,
		"regime_trend_adx":                    g.cfg.RegimeTrendADX,
		"regime_range_adx":                    g.cfg.RegimeRangeADX,
		"regime_breakout_lookback":            g.cfg.RegimeBreakoutLookback,
		"regime_breakout_atr_multiplier":      g.cfg.RegimeBreakoutATRMultiplier,
		"regime_confirm_bars":                 g.cfg.RegimeConfirmBars,
		"regime_refresh_interval":             g.cfg.RegimeRefreshInterval.String(),
		"bull_buy_levels":                     g.cfg.BullBuyLevels,
		"grid_order_time_in_force":            g.gridTimeInForce(),
		"active_4h_regime":                    string(regime),
		"active_4h_center":                    activeCenter,
		"active_4h_spacing_pct":               activeSpacing,
		"inventory_lot_count":                 lotCount,
		"inventory_lot_qty":                   lotQty,
		"regime_4h_close":                     regimeSnapshot.Close,
		"regime_4h_fast_ema":                  regimeSnapshot.FastEMA,
		"regime_4h_slow_ema":                  regimeSnapshot.SlowEMA,
		"regime_4h_atr":                       regimeSnapshot.ATR,
		"regime_4h_adx":                       regimeSnapshot.ADX,
		"regime_4h_plus_di":                   regimeSnapshot.PlusDI,
		"regime_4h_minus_di":                  regimeSnapshot.MinusDI,
		"regime_4h_breakout_high":             regimeSnapshot.BreakoutHigh,
		"regime_4h_breakout_low":              regimeSnapshot.BreakoutLow,
		"regime_reason":                       regimeSnapshot.Reason,
	}
	if !regimeSnapshot.BarTime.IsZero() {
		cfg["regime_4h_bar_time"] = regimeSnapshot.BarTime.Format(time.RFC3339)
	}

	cfg["ma20_allowed"] = ma20Allowed
	cfg["ma20_value"] = ma20Value
	cfg["ma20_last_price"] = ma20Price
	if !ma20CheckedAt.IsZero() {
		cfg["ma20_last_checked_at"] = ma20CheckedAt.Format(time.RFC3339)
	}
	g.ma20Mu.Lock()
	ma20ValueUpdatedAt := g.ma20ValueUpdatedAt
	g.ma20Mu.Unlock()
	if !ma20ValueUpdatedAt.IsZero() {
		cfg["ma20_value_updated_at"] = ma20ValueUpdatedAt.Format(time.RFC3339)
	}

	cfg["adx_value"] = adxValue
	cfg["adx_plus_di"] = plusDI
	cfg["adx_minus_di"] = minusDI
	cfg["adx_mode"] = adxMode
	cfg["macd_value"] = macdValue
	cfg["macd_signal"] = macdSignal
	cfg["macd_histogram"] = macdHist
	cfg["entry_filter_state"] = entryFilterState
	if !indicatorUpdatedAt.IsZero() {
		cfg["indicator_last_updated_at"] = indicatorUpdatedAt.Format(time.RFC3339)
	}

	return cfg
}

func (g *GridStrategy) checkTrendMA20(ctx context.Context, currentPrice float64) bool {
	if !g.cfg.UseTrendFilter {
		return true
	}

	now := time.Now().UTC()
	g.ma20Mu.Lock()
	ma20Value := g.ma20Value
	ma20UpdatedAt := g.ma20ValueUpdatedAt
	g.ma20Mu.Unlock()

	if ma20Value <= 0 || ma20UpdatedAt.IsZero() || now.Sub(ma20UpdatedAt) >= ma20ValueCacheTTL {
		value, err := g.loadCompletedMA20(ctx, now)
		if err != nil {
			if ma20Value <= 0 {
				log.Printf("grid %s MA20 unavailable: %v", g.cfg.Symbol, err)
				return false
			}
			log.Printf("grid %s MA20 refresh failed; using cached %.6f: %v", g.cfg.Symbol, ma20Value, err)
		} else {
			ma20Value = value
			g.ma20Mu.Lock()
			g.ma20Value = value
			g.ma20ValueUpdatedAt = now
			g.ma20CacheDay = now.Format("2006-01-02")
			g.ma20Mu.Unlock()
		}
	}

	if currentPrice <= 0 {
		log.Printf("grid %s MA20 current price unavailable", g.cfg.Symbol)
		return false
	}

	allowed := currentPrice > ma20Value
	g.ma20Mu.Lock()
	g.ma20CacheAllow = allowed
	g.ma20LastPrice = currentPrice
	g.ma20LastCheckedAt = now
	g.ma20Mu.Unlock()
	return allowed
}

func (g *GridStrategy) loadCompletedMA20(ctx context.Context, now time.Time) (float64, error) {
	end := now
	if ny, err := time.LoadLocation("America/New_York"); err == nil {
		local := now.In(ny)
		startOfToday := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, ny)
		end = startOfToday.UTC()
	}
	start := end.AddDate(0, 0, -90)

	bars, err := g.client.GetBars(ctx, g.cfg.Symbol, "1Day", start, end, 100)
	if err != nil {
		return 0, err
	}
	if len(bars) < 20 {
		return 0, fmt.Errorf("requires 20 completed daily bars, got %d", len(bars))
	}

	bars = bars[len(bars)-20:]
	sum := 0.0
	valid := 0
	for _, bar := range bars {
		if bar.Close <= 0 {
			continue
		}
		sum += bar.Close
		valid++
	}
	if valid < 20 {
		return 0, fmt.Errorf("requires 20 valid closes, got %d", valid)
	}
	return sum / float64(valid), nil
}

func parseBarTime(raw string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	if err == nil {
		return parsed.UTC(), nil
	}
	parsed, err = time.Parse(time.RFC3339, strings.TrimSpace(raw))
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func (g *GridStrategy) completedFourHourBars(ctx context.Context, now time.Time) ([]Bar, error) {
	end := now.UTC()
	start := end.AddDate(0, 0, -180)
	bars, err := g.client.GetBars(ctx, g.cfg.Symbol, g.cfg.RegimeTimeframe, start, end, 1000)
	if err != nil {
		return nil, err
	}
	completed := make([]Bar, 0, len(bars))
	for _, bar := range bars {
		if bar.Open <= 0 || bar.High <= 0 || bar.Low <= 0 || bar.Close <= 0 {
			continue
		}
		barTime, err := parseBarTime(bar.Time)
		if err != nil {
			continue
		}
		// Historical endpoints can include the currently-forming aggregate. Never
		// let an unfinished 4H candle switch the live trading regime. The final
		// regular-session aggregate is shorter than four hours, so cap it at 16:00
		// New York time instead of waiting until 17:30.
		barEnd := barTime.Add(4 * time.Hour)
		if ny, locationErr := time.LoadLocation("America/New_York"); locationErr == nil {
			localStart := barTime.In(ny)
			marketOpen := time.Date(localStart.Year(), localStart.Month(), localStart.Day(), 9, 30, 0, 0, ny)
			marketClose := time.Date(localStart.Year(), localStart.Month(), localStart.Day(), 16, 0, 0, 0, ny)
			if !localStart.Before(marketOpen) && localStart.Before(marketClose) && barEnd.After(marketClose.UTC()) {
				barEnd = marketClose.UTC()
			}
		}
		if barEnd.After(end) {
			continue
		}
		completed = append(completed, bar)
	}
	return completed, nil
}

func highestHigh(bars []Bar) float64 {
	value := 0.0
	for _, bar := range bars {
		if bar.High > value {
			value = bar.High
		}
	}
	return value
}

func lowestLow(bars []Bar) float64 {
	value := 0.0
	for _, bar := range bars {
		if bar.Low <= 0 {
			continue
		}
		if value <= 0 || bar.Low < value {
			value = bar.Low
		}
	}
	return value
}

func (g *GridStrategy) fourHourCandidate(bars []Bar, index int) (gridRegime, fourHourSnapshot) {
	if index < 1 || index >= len(bars) {
		return "", fourHourSnapshot{}
	}
	needed := maxInt(g.cfg.RegimeSlowEMAPeriod, g.cfg.RegimeBreakoutLookback+1)
	needed = maxInt(needed, g.cfg.RegimeADXPeriod+2)
	needed = maxInt(needed, g.cfg.RegimeATRPeriod+1)
	if index+1 < needed {
		return "", fourHourSnapshot{}
	}

	series := bars[:index+1]
	last := bars[index]
	fastEMA := computeEMA(series, g.cfg.RegimeFastEMAPeriod)
	slowEMA := computeEMA(series, g.cfg.RegimeSlowEMAPeriod)
	atr := computeATR(series, g.cfg.RegimeATRPeriod)
	adx, plusDI, minusDI := computeADX(series, g.cfg.RegimeADXPeriod)
	lookbackStart := index - g.cfg.RegimeBreakoutLookback
	if lookbackStart < 0 {
		lookbackStart = 0
	}
	prior := bars[lookbackStart:index]
	breakoutHigh := highestHigh(prior)
	breakoutLow := lowestLow(prior)
	buffer := atr * g.cfg.RegimeBreakoutATRMultiplier
	emaGapPct := 0.0
	if last.Close > 0 {
		emaGapPct = math.Abs(fastEMA-slowEMA) / last.Close
	}

	breakoutUp := breakoutHigh > 0 && last.Close > breakoutHigh+buffer
	breakoutDown := breakoutLow > 0 && last.Close < breakoutLow-buffer
	strongBull := fastEMA > slowEMA && plusDI > minusDI && adx >= g.cfg.RegimeTrendADX && last.Close >= fastEMA
	strongBear := fastEMA < slowEMA && minusDI > plusDI && adx >= g.cfg.RegimeTrendADX && last.Close <= fastEMA

	mode := gridRegime("")
	reason := "4h neutral; keep previous regime"
	switch {
	case strongBull && (breakoutUp || emaGapPct >= 0.005):
		mode = gridRegimeBull
		if breakoutUp {
			reason = "4h close confirmed above breakout channel with bullish EMA/ADX"
		} else {
			reason = "4h bullish EMA/ADX continuation"
		}
	case strongBear && (breakoutDown || emaGapPct >= 0.005):
		mode = gridRegimeBear
		if breakoutDown {
			reason = "4h close confirmed below breakout channel with bearish EMA/ADX"
		} else {
			reason = "4h bearish EMA/ADX continuation"
		}
	case adx <= g.cfg.RegimeRangeADX || emaGapPct <= 0.003:
		mode = gridRegimeRange
		reason = "4h ADX/EMA compression confirmed range regime"
	}

	center := fastEMA
	if mode == gridRegimeRange && fastEMA > 0 && slowEMA > 0 {
		center = (fastEMA + slowEMA) / 2
	}
	if center <= 0 {
		center = last.Close
	}
	spacing := g.cfg.SpacingPct
	if last.Close > 0 && atr > 0 {
		spacing = atr / last.Close * g.cfg.ATRMultiplier
	}
	if g.cfg.MinSpacingPct > 0 && spacing < g.cfg.MinSpacingPct {
		spacing = g.cfg.MinSpacingPct
	}
	if g.cfg.MaxSpacingPct > 0 && spacing > g.cfg.MaxSpacingPct {
		spacing = g.cfg.MaxSpacingPct
	}
	if spacing <= 0 {
		spacing = 0.01
	}
	barTime, _ := parseBarTime(last.Time)
	return mode, fourHourSnapshot{
		Regime:       mode,
		BarTime:      barTime,
		Close:        last.Close,
		FastEMA:      fastEMA,
		SlowEMA:      slowEMA,
		ATR:          atr,
		ADX:          adx,
		PlusDI:       plusDI,
		MinusDI:      minusDI,
		BreakoutHigh: breakoutHigh,
		BreakoutLow:  breakoutLow,
		Center:       center,
		Spacing:      spacing,
		Reason:       reason,
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (g *GridStrategy) refreshFourHourRegime(ctx context.Context) (bool, error) {
	now := time.Now().UTC()
	if !g.regimeCheckedAt.IsZero() && now.Sub(g.regimeCheckedAt) < g.cfg.RegimeRefreshInterval {
		return false, nil
	}
	g.regimeCheckedAt = now
	bars, err := g.completedFourHourBars(ctx, now)
	if err != nil {
		return false, err
	}
	minimum := maxInt(g.cfg.RegimeSlowEMAPeriod, g.cfg.RegimeBreakoutLookback+1)
	minimum = maxInt(minimum, g.cfg.RegimeADXPeriod+2) + g.cfg.RegimeConfirmBars
	if len(bars) < minimum {
		return false, fmt.Errorf("grid %s requires at least %d completed 4h bars, got %d", g.cfg.Symbol, minimum, len(bars))
	}

	lastIndex := len(bars) - 1
	_, latest := g.fourHourCandidate(bars, lastIndex)
	if latest.BarTime.IsZero() {
		return false, errors.New("latest completed 4h bar is unusable")
	}
	if latest.BarTime.Equal(g.regimeSnapshot.BarTime) && g.regimeSnapshot.Close > 0 {
		return false, nil
	}

	confirmed := gridRegime("")
	for offset := 0; offset < g.cfg.RegimeConfirmBars; offset++ {
		candidate, _ := g.fourHourCandidate(bars, lastIndex-offset)
		if candidate == "" {
			confirmed = ""
			break
		}
		if offset == 0 {
			confirmed = candidate
		} else if candidate != confirmed {
			confirmed = ""
			break
		}
	}
	if g.regime == "" {
		g.regime = gridRegimeRange
	}
	changed := confirmed != "" && confirmed != g.regime
	if changed {
		g.regime = confirmed
		latest.Reason = fmt.Sprintf("regime changed to %s after %d completed 4h bars: %s", confirmed, g.cfg.RegimeConfirmBars, latest.Reason)
	} else if confirmed == "" {
		latest.Reason = "4h confirmation incomplete; existing regime retained"
	}
	latest.Regime = g.regime
	g.regimeSnapshot = latest
	return changed, nil
}

func (g *GridStrategy) Tick(ctx context.Context, acct *Account, clock *Clock) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if clock != nil && !clock.IsOpen && !g.cfg.AllowOrdersWhenClosed {
		now := time.Now()
		if g.lastMarketClosedLog.IsZero() || now.Sub(g.lastMarketClosedLog) >= 15*time.Minute {
			log.Printf("grid %s paused: regular market is closed; next_open=%s", g.cfg.Symbol, clock.NextOpen.Format(time.RFC3339))
			g.lastMarketClosedLog = now
		}
		return nil
	}

	price, err := g.client.GetReferencePrice(ctx, g.cfg.Symbol)
	if err != nil {
		return err
	}

	allOpenOrders, err := g.client.ListOrders(ctx, "open")
	if err != nil {
		return err
	}
	openOrders := filterGridOrdersBySymbol(allOpenOrders, g.cfg.Symbol)

	position, err := currentPositionState(ctx, g.client, g.cfg.Symbol)
	if err != nil {
		return fmt.Errorf("grid %s position lookup failed: %w", g.cfg.Symbol, err)
	}
	if math.Abs(position.Qty) > 1e-9 && position.Side == "short" {
		return fmt.Errorf("grid %s is long-only but account position is short", g.cfg.Symbol)
	}
	g.syncInventoryLotsWithPosition(position)

	pendingBuyQty := 0.0
	openBuyOrders := 0
	for _, order := range openOrders {
		if strings.EqualFold(order.Side, "buy") && isActiveOrderStatus(order.Status) {
			remaining := remainingOrderQty(order)
			if remaining > 0 {
				pendingBuyQty += remaining
				openBuyOrders++
			}
		}
	}

	if err := g.refreshBuyRiskState(ctx, time.Now()); err != nil {
		log.Printf("grid %s risk-state refresh failed; using in-memory values: %v", g.cfg.Symbol, err)
	}

	regimeChanged, regimeErr := g.refreshFourHourRegime(ctx)
	if regimeErr != nil {
		if !g.initialized {
			return fmt.Errorf("grid %s cannot initialize 4h regime: %w", g.cfg.Symbol, regimeErr)
		}
		log.Printf("grid %s 4h regime refresh failed; retaining %s grid: %v", g.cfg.Symbol, g.regime, regimeErr)
	}

	if err := g.refreshIndicatorSnapshot(ctx); err != nil {
		log.Printf("grid %s 4h indicator refresh failed; using retained values: %v", g.cfg.Symbol, err)
	}

	targetCenter := g.regimeSnapshot.Center
	if targetCenter <= 0 {
		targetCenter = g.determineCenter(price)
	}
	spacing := g.regimeSnapshot.Spacing
	if spacing <= 0 {
		spacing = g.calculateSpacing(price)
	}

	if !g.initialized {
		// One-time migration: old DAY/1H orders have no durable 4H state or lot
		// ownership. Remove them before writing v7 state. If the process stops
		// during cancellation, the next start safely repeats this migration.
		if len(openOrders) > 0 {
			if err := g.cancelAllSymbolOrders(ctx, openOrders); err != nil {
				return err
			}
			return nil
		}
		if g.regime == "" {
			g.regime = gridRegimeRange
		}
		g.centerPrice = targetCenter
		g.currentSpacing = spacing
		g.initialized = true
		g.lastBuildAt = time.Now()
		g.lastRebuildAt = g.lastBuildAt
		g.persistGridStateLocked()
		return g.rebuildGrid(ctx, g.centerPrice, g.currentSpacing, position, pendingBuyQty, openBuyOrders, parseFloatString(acct.BuyingPower), price)
	}

	if regimeChanged {
		g.rebuildPending = true
		g.pendingCenter = targetCenter
		g.pendingSpacing = spacing
	}
	if g.rebuildPending {
		oldCenter := g.centerPrice
		cancelled, err := g.cancelEntryOrders(ctx, openOrders)
		if err != nil {
			return err
		}
		g.centerPrice = g.pendingCenter
		g.currentSpacing = g.pendingSpacing
		g.lastRebuildAt = time.Now()
		g.rebuildPending = false
		g.pendingCenter = 0
		g.pendingSpacing = 0
		g.persistGridStateLocked()
		log.Printf("grid %s 4h regime switched to %s: center %.4f -> %.4f spacing=%.4f reason=%s", g.cfg.Symbol, g.regime, oldCenter, g.centerPrice, g.currentSpacing, g.regimeSnapshot.Reason)
		if cancelled {
			return nil
		}
	}

	// No calendar-day or ordinary price-drift rebuild. The persisted anchor is
	// changed only by a confirmed 4H regime transition.
	return g.maintainGrid(ctx, g.centerPrice, g.currentSpacing, position, pendingBuyQty, openBuyOrders, openOrders, parseFloatString(acct.BuyingPower), price)
}

func (g *GridStrategy) cancelEntryOrders(ctx context.Context, orders []Order) (bool, error) {
	cancelled := false
	for _, order := range orders {
		if !isGridOrderForSymbol(order, g.cfg.Symbol) || !strings.EqualFold(order.Side, "buy") {
			continue
		}
		if err := g.client.CancelOrder(ctx, order.ID); err != nil {
			if isHTTPStatusError(err, http.StatusNotFound) || isHTTPStatusError(err, http.StatusUnprocessableEntity) {
				continue
			}
			return cancelled, fmt.Errorf("cancel grid entry order %s: %w", order.ID, err)
		}
		cancelled = true
	}
	return cancelled, nil
}

func (g *GridStrategy) cancelAllSymbolOrders(ctx context.Context, orders []Order) error {
	for _, order := range orders {
		if !isGridOrderForSymbol(order, g.cfg.Symbol) {
			continue
		}
		if err := g.client.CancelOrder(ctx, order.ID); err != nil {
			if isHTTPStatusError(err, http.StatusNotFound) || isHTTPStatusError(err, http.StatusUnprocessableEntity) {
				continue
			}
			return fmt.Errorf("cancel grid order %s: %w", order.ID, err)
		}
	}
	return nil
}

func tradingDayBounds(now time.Time) (string, time.Time, time.Time) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		location = time.UTC
	}
	local := now.In(location)
	startLocal := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
	return startLocal.Format("2006-01-02"), startLocal.UTC(), startLocal.AddDate(0, 0, 1).UTC()
}

func (g *GridStrategy) resetDailyStatsIfNeeded(now time.Time) {
	day, _, _ := tradingDayBounds(now)
	if g.dailyBuyDate != day {
		g.dailyBuyDate = day
		g.dailyBuyNotional = 0
		g.riskStateUpdatedAt = time.Time{}
	}
}

func (g *GridStrategy) refreshBuyRiskState(ctx context.Context, now time.Time) error {
	g.resetDailyStatsIfNeeded(now)
	if !g.riskStateUpdatedAt.IsZero() && now.Sub(g.riskStateUpdatedAt) < riskStateCacheTTL {
		return nil
	}

	day, start, end := tradingDayBounds(now)
	orders, err := g.client.ListOrdersSince(ctx, "all", start)
	if err != nil {
		return err
	}

	used := 0.0
	var latestBuyFill time.Time
	for _, order := range orders {
		if !isGridOrderForSymbol(order, g.cfg.Symbol) || !strings.EqualFold(order.Side, "buy") {
			continue
		}
		orderTime := time.Time{}
		if order.CreatedAt != nil {
			orderTime = *order.CreatedAt
		}
		if !orderTime.IsZero() && (orderTime.Before(start) || !orderTime.Before(end)) {
			continue
		}

		filledQty := parseFloatString(order.FilledQty)
		filledPrice := parseFloatString(order.FilledAvgPrice)
		if filledPrice <= 0 {
			filledPrice = parseFloatString(order.LimitPrice)
		}
		if filledQty > 0 && filledPrice > 0 {
			used += filledQty * filledPrice
			fillTime := orderTime
			if order.FilledAt != nil {
				fillTime = *order.FilledAt
			} else if order.UpdatedAt != nil {
				fillTime = *order.UpdatedAt
			}
			if fillTime.After(latestBuyFill) {
				latestBuyFill = fillTime
			}
		}

		if isActiveOrderStatus(order.Status) {
			remaining := remainingOrderQty(order)
			limitPrice := parseFloatString(order.LimitPrice)
			if remaining > 0 && limitPrice > 0 {
				used += remaining * limitPrice
			}
		}
	}

	g.dailyBuyDate = day
	g.dailyBuyNotional = used
	g.riskStateUpdatedAt = now
	if latestBuyFill.After(g.lastBuyAt) {
		g.lastBuyAt = latestBuyFill
	}
	return nil
}

func (g *GridStrategy) canBuy(now time.Time, notional float64, openBuyOrders int) bool {
	g.resetDailyStatsIfNeeded(now)

	if notional <= 0 {
		g.logBuyBlocked(now, "order notional is not positive")
		return false
	}
	if g.cfg.MaxOpenBuyOrders > 0 && openBuyOrders >= g.cfg.MaxOpenBuyOrders {
		g.logBuyBlocked(now, fmt.Sprintf("maximum open buy orders reached; open=%d limit=%d", openBuyOrders, g.cfg.MaxOpenBuyOrders))
		return false
	}
	if g.cfg.BuyCooldown > 0 && !g.lastBuyAt.IsZero() && now.Sub(g.lastBuyAt) < g.cfg.BuyCooldown {
		remaining := g.cfg.BuyCooldown - now.Sub(g.lastBuyAt)
		g.logBuyBlocked(now, fmt.Sprintf("buy cooldown active after fill; remaining=%s", remaining.Round(time.Second)))
		return false
	}
	if g.cfg.DailyBuyNotionalLimit > 0 && g.dailyBuyNotional+notional > g.cfg.DailyBuyNotionalLimit+1e-9 {
		g.logBuyBlocked(now, fmt.Sprintf(
			"daily buy limit exceeded; used_or_reserved=%.2f requested=%.2f limit=%.2f",
			g.dailyBuyNotional, notional, g.cfg.DailyBuyNotionalLimit,
		))
		return false
	}
	return true
}

func (g *GridStrategy) logBuyBlocked(now time.Time, reason string) {
	if reason == "" {
		return
	}
	if reason != g.lastBuyBlockReason || g.lastBuyBlockLogAt.IsZero() || now.Sub(g.lastBuyBlockLogAt) >= 5*time.Minute {
		log.Printf("grid %s buy blocked: %s", g.cfg.Symbol, reason)
		g.lastBuyBlockReason = reason
		g.lastBuyBlockLogAt = now
	}
}

func (g *GridStrategy) reserveBuyOrder(now time.Time, notional float64) {
	g.resetDailyStatsIfNeeded(now)
	g.dailyBuyNotional += math.Max(0, notional)
	g.riskStateUpdatedAt = now
}

func (g *GridStrategy) OnFill(orderID, side string, qty, price float64, at time.Time) {
	if qty <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if at.IsZero() {
		at = time.Now().UTC()
	}
	switch strings.ToLower(strings.TrimSpace(side)) {
	case "buy":
		g.inventoryLots = append(g.inventoryLots, gridInventoryLot{
			BuyOrderID: orderID,
			Qty:        qty,
			EntryPrice: price,
			BoughtAt:   at,
		})
		if at.After(g.lastBuyAt) {
			g.lastBuyAt = at
		}
		// Force a REST reconstruction soon so cancellations and partial fills are
		// reflected in the reserved daily notional.
		g.riskStateUpdatedAt = time.Time{}
	case "sell":
		g.consumeInventoryLots(qty, price)
	}
}

func (g *GridStrategy) lotTargetPrice(entryPrice float64) float64 {
	if entryPrice <= 0 {
		return 0
	}
	// Never raise an old lot's target because of today's market price or a new
	// 4H center. A GTC order at this target can capture a next-day gap up.
	return normalizeLimitPrice(entryPrice*(1+g.cfg.MinProfitPct), "sell")
}

func (g *GridStrategy) syncInventoryLotsWithPosition(position positionState) {
	targetQty := math.Max(0, position.Qty)
	trackedQty := 0.0
	for _, lot := range g.inventoryLots {
		trackedQty += math.Max(0, lot.Qty)
	}
	if targetQty <= 1e-9 {
		g.inventoryLots = nil
		return
	}
	if trackedQty+1e-6 < targetQty {
		g.inventoryLots = append(g.inventoryLots, gridInventoryLot{
			Qty:        targetQty - trackedQty,
			EntryPrice: position.AvgEntryPrice,
			BoughtAt:   time.Now().UTC(),
		})
		return
	}
	if trackedQty <= targetQty+1e-6 {
		return
	}
	remaining := targetQty
	trimmed := make([]gridInventoryLot, 0, len(g.inventoryLots))
	for _, lot := range g.inventoryLots {
		if remaining <= 1e-9 || lot.Qty <= 0 {
			continue
		}
		keep := math.Min(lot.Qty, remaining)
		lot.Qty = keep
		trimmed = append(trimmed, lot)
		remaining -= keep
	}
	g.inventoryLots = trimmed
}

func (g *GridStrategy) consumeInventoryLots(qty, sellPrice float64) {
	remaining := qty
	for remaining > 1e-9 && len(g.inventoryLots) > 0 {
		best := -1
		bestEntry := math.MaxFloat64
		for i, lot := range g.inventoryLots {
			if lot.Qty <= 0 {
				continue
			}
			target := g.lotTargetPrice(lot.EntryPrice)
			if target > 0 && sellPrice+priceIncrement(sellPrice)/2 >= target && lot.EntryPrice < bestEntry {
				best = i
				bestEntry = lot.EntryPrice
			}
		}
		if best < 0 {
			best = 0
		}
		used := math.Min(remaining, g.inventoryLots[best].Qty)
		g.inventoryLots[best].Qty -= used
		remaining -= used
		if g.inventoryLots[best].Qty <= 1e-9 {
			g.inventoryLots = append(g.inventoryLots[:best], g.inventoryLots[best+1:]...)
		}
	}
}

func (g *GridStrategy) quantityForPrice(price, fallbackQty float64) float64 {
	if price <= 0 {
		return 0
	}
	if g.cfg.OrderNotional > 0 {
		// Whole shares keep limit-order behavior consistent across live accounts.
		return math.Floor(g.cfg.OrderNotional / price)
	}
	return fallbackQty
}

func (g *GridStrategy) canAddPosition(positionQty, pendingQty, addQty, orderPrice, currentPrice float64) bool {
	projectedQty := math.Max(0, positionQty) + math.Max(0, pendingQty) + math.Max(0, addQty)
	if g.cfg.MaxPositionQty > 0 && projectedQty > g.cfg.MaxPositionQty+1e-9 {
		return false
	}
	mark := math.Max(orderPrice, currentPrice)
	if g.cfg.MaxPositionNotional > 0 && mark > 0 && projectedQty*mark > g.cfg.MaxPositionNotional+1e-9 {
		return false
	}
	return true
}

func (g *GridStrategy) clientOrderIDFor(key, kind string, level int) string {
	if intent, ok := g.uncertainIntents[key]; ok &&
		time.Since(intent.CreatedAt) < 10*time.Minute {
		return intent.ClientOrderID
	}

	symbol := strings.ToLower(strings.TrimSpace(g.cfg.Symbol))
	prefix := fmt.Sprintf("grid-%s-%s", kind, symbol)

	if level > 0 {
		prefix = fmt.Sprintf("%s-%d", prefix, level)
	}

	id := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	g.uncertainIntents[key] = orderIntent{
		ClientOrderID: id,
		CreatedAt:     time.Now(),
	}
	return id
}

func (g *GridStrategy) clearOrderIntent(key string) {
	delete(g.uncertainIntents, key)
}

func (g *GridStrategy) rebuildGrid(ctx context.Context, center, spacing float64, position positionState, pendingBuyQty float64, openBuyOrders int, buyingPower, currentPrice float64) error {
	now := time.Now()
	if spacing <= 0 {
		spacing = g.cfg.SpacingPct
	}
	if spacing <= 0 {
		spacing = 0.01
	}

	g.centerPrice = center
	g.currentSpacing = spacing
	g.initialized = true
	g.lastBuildAt = now
	g.lastRebuildAt = now
	g.pendingOrders = make(map[string]time.Time)
	g.persistGridStateLocked()

	return g.maintainGrid(ctx, center, spacing, position, pendingBuyQty, openBuyOrders, nil, buyingPower, currentPrice)
}

func (g *GridStrategy) activeBuyLevelLimit() int {
	switch g.regime {
	case gridRegimeBear:
		return 0
	case gridRegimeBull:
		return minInt(g.cfg.BullBuyLevels, g.cfg.Levels)
	default:
		return g.cfg.Levels
	}
}

type exitTarget struct {
	Price float64
	Qty   float64
}

func (g *GridStrategy) maintainExitOrders(ctx context.Context, position positionState, openOrders []Order) error {
	g.syncInventoryLotsWithPosition(position)
	if position.Qty <= 1e-9 {
		return nil
	}

	desired := make(map[string]exitTarget)
	for _, lot := range g.inventoryLots {
		price := g.lotTargetPrice(lot.EntryPrice)
		// MinPrice/MaxPrice constrain new entries only. Never suppress the exit of
		// an already-owned lot because its profitable target is outside that band.
		if price <= 0 || lot.Qty <= 0 {
			continue
		}
		key := formatLimitPrice(price, "sell")
		target := desired[key]
		target.Price = price
		target.Qty += lot.Qty
		desired[key] = target
	}

	openByPrice := make(map[string]float64)
	reservedSellQty := 0.0
	for _, order := range openOrders {
		if !strings.EqualFold(order.Side, "sell") || !isActiveOrderStatus(order.Status) {
			continue
		}
		remaining := remainingOrderQty(order)
		price := parseFloatString(order.LimitPrice)
		if remaining <= 0 || price <= 0 {
			continue
		}
		key := formatLimitPrice(price, "sell")
		openByPrice[key] += remaining
		reservedSellQty += remaining
		delete(g.pendingOrders, "sell-"+key)
	}
	if reservedSellQty > position.Qty+1e-6 {
		return fmt.Errorf("grid %s refuses to add exits: open sell qty %.6f exceeds position %.6f", g.cfg.Symbol, reservedSellQty, position.Qty)
	}

	available := math.Max(0, position.Qty-reservedSellQty)
	targets := make([]exitTarget, 0, len(desired))
	for key, target := range desired {
		target.Qty = math.Max(0, target.Qty-openByPrice[key])
		if target.Qty > 1e-9 {
			targets = append(targets, target)
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].Price < targets[j].Price })
	for level, target := range targets {
		qty := math.Min(target.Qty, available)
		if qty <= 1e-9 {
			break
		}
		if !g.placeSellIfSafe(ctx, level+1, target.Price, qty) {
			continue
		}
		available -= qty
	}
	return nil
}

func (g *GridStrategy) maintainGrid(ctx context.Context, center, spacing float64, position positionState, pendingBuyQty float64, openBuyOrders int, openOrders []Order, buyingPower, currentPrice float64) error {
	now := time.Now()
	if spacing <= 0 {
		spacing = g.cfg.SpacingPct
	}
	if spacing <= 0 {
		spacing = 0.01
	}
	g.currentSpacing = spacing

	if err := g.maintainExitOrders(ctx, position, openOrders); err != nil {
		return err
	}

	levelLimit := g.activeBuyLevelLimit()
	if levelLimit <= 0 {
		g.setEntryFilterState("4h_bear_breakdown_buy_side_disabled")
		return nil
	}

	openBuyPrices := map[string]bool{}
	for _, order := range openOrders {
		if !strings.EqualFold(order.Side, "buy") || !isActiveOrderStatus(order.Status) {
			continue
		}
		price := parseFloatString(order.LimitPrice)
		if price <= 0 {
			continue
		}
		priceKey := formatLimitPrice(price, "buy")
		openBuyPrices[priceKey] = true
		delete(g.pendingOrders, "buy-"+priceKey)
	}

	seedQty := g.quantityForPrice(currentPrice, g.cfg.SeedQty)
	if position.Qty <= 0 && openBuyOrders == 0 && seedQty > 0 && g.allowBuyAtLevel(ctx, 0, currentPrice) {
		maxSeedPrice := center * (1 + g.cfg.MaxSeedPremiumPct)
		if currentPrice > maxSeedPrice {
			g.logBuyBlocked(now, fmt.Sprintf("seed chase protection; market=%.4f max_seed=%.4f center=%.4f", currentPrice, maxSeedPrice, center))
		} else {
			seedPrice := normalizeLimitPrice(currentPrice, "buy")
			notional := seedPrice * seedQty
			if g.canAddPosition(position.Qty, pendingBuyQty, seedQty, seedPrice, currentPrice) && buyingPower >= notional && g.canBuy(now, notional, openBuyOrders) {
				newBP, placed := g.placeSeedBuyIfSafe(ctx, seedPrice, seedQty, buyingPower, openBuyOrders)
				if placed {
					buyingPower = newBP
					return nil
				}
			}
		}
	}

	usedBuyPrices := map[string]bool{}
	for level := 1; level <= levelLimit; level++ {
		off := float64(level) * spacing
		buyPrice := center * (1 - off)
		if currentPrice > 0 {
			buyPrice = math.Min(buyPrice, currentPrice*(1-g.cfg.BuyMarketBufferPct))
		}
		buyKey := formatLimitPrice(buyPrice, "buy")

		if g.allowBuyAtLevel(ctx, level, currentPrice) && !usedBuyPrices[buyKey] && !openBuyPrices[buyKey] && buyPrice > 0 && (g.cfg.MinPrice <= 0 || buyPrice >= g.cfg.MinPrice) {
			buyQty := g.quantityForPrice(buyPrice, g.cfg.QtyPerOrder)
			if buyQty > 0 && g.canAddPosition(position.Qty, pendingBuyQty, buyQty, buyPrice, currentPrice) {
				newBP, placed := g.placeBuyIfSafe(ctx, level, buyPrice, buyQty, buyingPower, openBuyOrders)
				if placed {
					buyingPower = newBP
					pendingBuyQty += buyQty
					openBuyOrders++
					usedBuyPrices[buyKey] = true
				}
			}
		}
	}
	return nil
}

func (g *GridStrategy) placeSeedBuyIfSafe(ctx context.Context, price, qty, buyingPower float64, openBuyOrders int) (float64, bool) {
	now := time.Now()
	priceKey := formatLimitPrice(price, "buy")
	key := "buy-" + priceKey
	if g.isPending(key) {
		return buyingPower, false
	}
	notional := normalizeLimitPrice(price, "buy") * qty
	if buyingPower < notional {
		g.logBuyBlocked(now, fmt.Sprintf("insufficient buying power; available=%.2f required=%.2f", buyingPower, notional))
		return buyingPower, false
	}
	if !g.canBuy(now, notional, openBuyOrders) {
		return buyingPower, false
	}

	clientOrderID := g.clientOrderIDFor(key, "seed", 0)
	_, err := g.client.PlaceOrderIdempotent(ctx, OrderRequest{
		Symbol:        g.cfg.Symbol,
		Qty:           fmt.Sprintf("%.6f", qty),
		Side:          "buy",
		Type:          "limit",
		TimeInForce:   g.gridTimeInForce(),
		LimitPrice:    priceKey,
		ClientOrderID: clientOrderID,
	})
	if err != nil {
		if isDefinitiveSubmitError(err) {
			g.clearOrderIntent(key)
		}
		log.Printf("grid %s seed buy failed: price=%s qty=%.6f error=%v", g.cfg.Symbol, priceKey, qty, err)
		return buyingPower, false
	}
	g.clearOrderIntent(key)
	g.pendingOrders[key] = now
	g.reserveBuyOrder(now, notional)
	return buyingPower - notional, true
}

func (g *GridStrategy) placeBuyIfSafe(ctx context.Context, level int, price, qty, buyingPower float64, openBuyOrders int) (float64, bool) {
	now := time.Now()
	priceKey := formatLimitPrice(price, "buy")
	key := "buy-" + priceKey
	if g.isPending(key) {
		return buyingPower, false
	}
	notional := normalizeLimitPrice(price, "buy") * qty
	if buyingPower < notional {
		g.logBuyBlocked(now, fmt.Sprintf("insufficient buying power; available=%.2f required=%.2f", buyingPower, notional))
		return buyingPower, false
	}
	if !g.canBuy(now, notional, openBuyOrders) {
		return buyingPower, false
	}

	clientOrderID := g.clientOrderIDFor(key, "buy", level)
	_, err := g.client.PlaceOrderIdempotent(ctx, OrderRequest{
		Symbol:        g.cfg.Symbol,
		Qty:           fmt.Sprintf("%.6f", qty),
		Side:          "buy",
		Type:          "limit",
		TimeInForce:   g.gridTimeInForce(),
		LimitPrice:    priceKey,
		ClientOrderID: clientOrderID,
	})
	if err != nil {
		if isDefinitiveSubmitError(err) {
			g.clearOrderIntent(key)
		}
		log.Printf("grid %s buy order failed: level=%d price=%s qty=%.6f error=%v", g.cfg.Symbol, level, priceKey, qty, err)
		return buyingPower, false
	}
	g.clearOrderIntent(key)
	g.pendingOrders[key] = now
	g.reserveBuyOrder(now, notional)
	return buyingPower - notional, true
}

func (g *GridStrategy) placeSellIfSafe(ctx context.Context, level int, price, qty float64) bool {
	now := time.Now()
	priceKey := formatLimitPrice(price, "sell")
	key := "sell-" + priceKey
	if g.isPending(key) {
		return false
	}

	clientOrderID := g.clientOrderIDFor(key, "exit", level)
	_, err := g.client.PlaceOrderIdempotent(ctx, OrderRequest{
		Symbol:        g.cfg.Symbol,
		Qty:           fmt.Sprintf("%.6f", qty),
		Side:          "sell",
		Type:          "limit",
		TimeInForce:   g.gridTimeInForce(),
		LimitPrice:    priceKey,
		ClientOrderID: clientOrderID,
	})
	if err != nil {
		if isDefinitiveSubmitError(err) {
			g.clearOrderIntent(key)
		}
		log.Printf("grid %s sell order failed: level=%d price=%s qty=%.6f error=%v", g.cfg.Symbol, level, priceKey, qty, err)
		return false
	}
	g.clearOrderIntent(key)
	g.pendingOrders[key] = now
	return true
}

func isDefinitiveSubmitError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, status := range []string{"status=400", "status=401", "status=403", "status=404", "status=422"} {
		if strings.Contains(message, status) {
			return true
		}
	}
	return false
}

func (g *GridStrategy) isPending(key string) bool {
	if timestamp, ok := g.pendingOrders[key]; ok {
		if time.Since(timestamp) < 60*time.Second {
			return true
		}
		delete(g.pendingOrders, key)
	}
	return false
}

func (g *GridStrategy) levelIndex(center, price float64) int {
	spacing := g.currentSpacing
	if spacing <= 0 {
		spacing = g.cfg.SpacingPct
	}
	if center <= 0 || price <= 0 || spacing <= 0 {
		return 0
	}
	ratio := price / center
	diff := math.Abs(1 - ratio)
	idx := int(math.Round(diff / spacing))
	if idx < 1 {
		idx = 1
	}
	return idx
}

func (g *GridStrategy) refreshIndicatorSnapshot(ctx context.Context) error {
	needsATR := g.cfg.ATRPeriod > 0 && g.cfg.ATRMultiplier > 0
	needsEMA := strings.EqualFold(g.cfg.CenterMode, "ema")
	needsVWAP := strings.EqualFold(g.cfg.CenterMode, "vwap")
	needsADX := g.cfg.ADXPeriod > 0 && (g.cfg.ADXTrendThreshold > 0 || g.cfg.ADXRangeThreshold > 0)
	needsMACD := g.cfg.UseTrendFilter && g.cfg.EntryFilterMode != "off"

	if !needsATR && !needsEMA && !needsVWAP && !needsADX && !needsMACD {
		return nil
	}

	now := time.Now()
	g.indicatorMu.Lock()
	if !g.indicatorExpiresAt.IsZero() && now.Before(g.indicatorExpiresAt) {
		g.indicatorMu.Unlock()
		return nil
	}
	g.indicatorMu.Unlock()

	// Reuse the same completed 4H candles as the regime engine. This prevents a
	// partial intraday bar from loosening/tightening entry filters mid-candle.
	bars, err := g.completedFourHourBars(ctx, time.Now().UTC())
	ttl := indicatorCacheTTL
	if g.cfg.RebuildCooldown > 0 && g.cfg.RebuildCooldown < ttl {
		ttl = g.cfg.RebuildCooldown
	}
	if ttl <= 0 {
		ttl = time.Minute
	}

	if err != nil {
		g.indicatorMu.Lock()
		g.indicatorExpiresAt = time.Now().Add(ttl)
		g.indicatorMu.Unlock()
		return err
	}
	if len(bars) < 2 {
		g.indicatorMu.Lock()
		g.indicatorExpiresAt = time.Now().Add(ttl)
		g.indicatorMu.Unlock()
		return errors.New("insufficient bars for indicators")
	}

	var atr, ema, vwap, adx, plusDI, minusDI, macd, macdSignal, macdHist float64
	if needsATR {
		atr = computeATR(bars, g.cfg.ATRPeriod)
	}
	if needsEMA {
		ema = computeEMA(bars, g.cfg.CenterEMAPeriod)
	}
	if needsVWAP {
		vwap = computeVWAP(bars, g.cfg.CenterVWAPLookback)
	}
	if needsADX {
		adx, plusDI, minusDI = computeADX(bars, g.cfg.ADXPeriod)
	}
	if needsMACD {
		macd, macdSignal, macdHist = computeMACD(bars, g.cfg.MACDFastPeriod, g.cfg.MACDSlowPeriod, g.cfg.MACDSignalPeriod)
	}

	g.indicatorMu.Lock()
	if needsATR {
		g.cachedATR = atr
	}
	if needsEMA {
		g.cachedEMA = ema
	}
	if needsVWAP {
		g.cachedVWAP = vwap
	}
	if needsADX {
		g.cachedADX = adx
		g.cachedPlusDI = plusDI
		g.cachedMinusDI = minusDI
	}
	if needsMACD {
		g.cachedMACD = macd
		g.cachedMACDSignal = macdSignal
		g.cachedMACDHist = macdHist
	}
	g.indicatorUpdatedAt = time.Now().UTC()
	g.indicatorExpiresAt = time.Now().Add(ttl)
	g.indicatorMu.Unlock()
	return nil
}

func (g *GridStrategy) calculateSpacing(price float64) float64 {
	spacing := g.cfg.SpacingPct
	g.indicatorMu.Lock()
	atr := g.cachedATR
	g.indicatorMu.Unlock()

	if price > 0 && atr > 0 && g.cfg.ATRMultiplier > 0 {
		spacing = (atr / price) * g.cfg.ATRMultiplier
		if g.cfg.MinSpacingPct > 0 && spacing < g.cfg.MinSpacingPct {
			spacing = g.cfg.MinSpacingPct
		}
		if g.cfg.MaxSpacingPct > 0 && spacing > g.cfg.MaxSpacingPct {
			spacing = g.cfg.MaxSpacingPct
		}
	}
	if spacing <= 0 {
		spacing = g.cfg.SpacingPct
	}
	if spacing <= 0 {
		spacing = 0.01
	}
	return spacing
}

func (g *GridStrategy) determineCenter(fallback float64) float64 {
	mode := strings.ToLower(strings.TrimSpace(g.cfg.CenterMode))

	g.indicatorMu.Lock()
	ema := g.cachedEMA
	vwap := g.cachedVWAP
	g.indicatorMu.Unlock()

	switch mode {
	case "ema":
		if ema > 0 {
			return ema
		}
	case "vwap":
		if vwap > 0 {
			return vwap
		}
	}
	if g.centerPrice > 0 {
		return g.centerPrice
	}
	return fallback
}

func (g *GridStrategy) updateADXMode() string {
	g.indicatorMu.Lock()
	defer g.indicatorMu.Unlock()

	if g.cfg.ADXPeriod <= 0 || (g.cfg.ADXTrendThreshold <= 0 && g.cfg.ADXRangeThreshold <= 0) {
		g.adxMode = "range"
		return g.adxMode
	}

	value := g.cachedADX
	trendThreshold := g.cfg.ADXTrendThreshold
	rangeThreshold := g.cfg.ADXRangeThreshold
	if rangeThreshold <= 0 || rangeThreshold >= trendThreshold {
		rangeThreshold = trendThreshold * 0.8
	}

	mode := g.adxMode
	if mode == "" {
		if value >= trendThreshold && trendThreshold > 0 {
			mode = "trend"
		} else {
			mode = "range"
		}
	} else if mode == "trend" && rangeThreshold > 0 && value <= rangeThreshold {
		mode = "range"
	} else if mode == "range" && trendThreshold > 0 && value >= trendThreshold {
		mode = "trend"
	}
	g.adxMode = mode
	return mode
}

func (g *GridStrategy) allowBuyAtLevel(ctx context.Context, level int, currentPrice float64) bool {
	if g.regime == gridRegimeBear {
		g.setEntryFilterState("4h_bear_breakdown_buy_side_disabled")
		return false
	}
	if g.regime == gridRegimeBull && level > g.activeBuyLevelLimit() {
		g.setEntryFilterState("4h_bull_breakout_shallow_entries_only")
		return false
	}
	if !g.cfg.UseTrendFilter || g.cfg.EntryFilterMode == "off" {
		return true
	}

	// MA20 failure is fail-open: unavailable market data must not freeze entries.
	maAllowed := g.checkTrendMA20(ctx, currentPrice)
	g.ma20Mu.Lock()
	maAvailable := g.ma20Value > 0 && !g.ma20ValueUpdatedAt.IsZero()
	g.ma20Mu.Unlock()

	g.indicatorMu.Lock()
	macdHist := g.cachedMACDHist
	adx := g.cachedADX
	plusDI := g.cachedPlusDI
	minusDI := g.cachedMinusDI
	indicatorAvailable := !g.indicatorUpdatedAt.IsZero()
	g.indicatorMu.Unlock()

	if !maAvailable || !indicatorAvailable || currentPrice <= 0 {
		g.setEntryFilterState("data_unavailable_fail_open")
		return true
	}

	macdBearish := macdHist < -currentPrice*g.cfg.MACDBearishPct
	strongDowntrend := adx >= g.cfg.ADXTrendThreshold && minusDI > plusDI
	strongBearishConsensus := !maAllowed && macdBearish && strongDowntrend

	if g.cfg.EntryFilterMode == "strict" {
		allowed := maAllowed && !macdBearish && !strongDowntrend
		if allowed {
			g.setEntryFilterState("strict_allowed")
		} else {
			g.setEntryFilterState("strict_blocked")
		}
		return allowed
	}

	// Soft mode: MACD never has veto power by itself. During a confirmed strong
	// downtrend only skip the seed/shallow levels; deeper mean-reversion orders
	// remain live, avoiding the previous all-or-nothing missed-market behavior.
	if strongBearishConsensus && level < g.cfg.MinBearishBuyLevel {
		g.setEntryFilterState("soft_skip_shallow_strong_bearish")
		return false
	}
	if strongBearishConsensus {
		g.setEntryFilterState("soft_deep_levels_only")
	} else {
		g.setEntryFilterState("soft_allowed")
	}
	return true
}

func (g *GridStrategy) setEntryFilterState(state string) {
	g.indicatorMu.Lock()
	g.entryFilterState = state
	g.indicatorMu.Unlock()
}

func trueRange(current, previous Bar) float64 {
	hl := current.High - current.Low
	hp := math.Abs(current.High - previous.Close)
	lp := math.Abs(current.Low - previous.Close)

	tr := hl
	if hp > tr {
		tr = hp
	}
	if lp > tr {
		tr = lp
	}
	if tr < 0 {
		return 0
	}
	return tr
}

func computeATR(bars []Bar, period int) float64 {
	if period <= 0 || len(bars) < period+1 {
		return 0
	}
	start := len(bars) - period
	if start < 1 {
		start = 1
	}
	sum := 0.0
	for i := start; i < len(bars); i++ {
		sum += trueRange(bars[i], bars[i-1])
	}
	return sum / float64(len(bars)-start)
}

func computeEMA(bars []Bar, period int) float64 {
	if period <= 0 || len(bars) == 0 {
		return 0
	}
	k := 2.0 / (float64(period) + 1)
	ema := bars[0].Close
	for i := 1; i < len(bars); i++ {
		ema = (bars[i].Close-ema)*k + ema
	}
	return ema
}

func computeVWAP(bars []Bar, lookback int) float64 {
	if len(bars) == 0 {
		return 0
	}
	if lookback <= 0 || lookback > len(bars) {
		lookback = len(bars)
	}
	start := len(bars) - lookback
	pv := 0.0
	vol := 0.0
	for i := start; i < len(bars); i++ {
		typical := (bars[i].High + bars[i].Low + bars[i].Close) / 3.0
		pv += typical * bars[i].Volume
		vol += bars[i].Volume
	}
	if vol == 0 {
		return 0
	}
	return pv / vol
}

func computeMACD(bars []Bar, fastPeriod, slowPeriod, signalPeriod int) (macd, signal, histogram float64) {
	if fastPeriod <= 0 || slowPeriod <= fastPeriod || signalPeriod <= 0 || len(bars) < slowPeriod+signalPeriod {
		return 0, 0, 0
	}
	fastK := 2.0 / (float64(fastPeriod) + 1)
	slowK := 2.0 / (float64(slowPeriod) + 1)
	signalK := 2.0 / (float64(signalPeriod) + 1)
	fastEMA, slowEMA := bars[0].Close, bars[0].Close
	for i := 1; i < len(bars); i++ {
		fastEMA += (bars[i].Close - fastEMA) * fastK
		slowEMA += (bars[i].Close - slowEMA) * slowK
		macd = fastEMA - slowEMA
		if i == 1 {
			signal = macd
		} else {
			signal += (macd - signal) * signalK
		}
	}
	return macd, signal, macd - signal
}

func computeADX(bars []Bar, period int) (adx, plusDI, minusDI float64) {
	if period <= 0 || len(bars) < period+2 {
		return 0, 0, 0
	}

	n := len(bars)
	tr := make([]float64, n)
	plusDM := make([]float64, n)
	minusDM := make([]float64, n)

	for i := 1; i < n; i++ {
		highDiff := bars[i].High - bars[i-1].High
		lowDiff := bars[i-1].Low - bars[i].Low

		tr[i] = trueRange(bars[i], bars[i-1])
		if highDiff > lowDiff && highDiff > 0 {
			plusDM[i] = highDiff
		}
		if lowDiff > highDiff && lowDiff > 0 {
			minusDM[i] = lowDiff
		}
	}

	var tr14, plus14, minus14 float64
	for i := 1; i <= period && i < n; i++ {
		tr14 += tr[i]
		plus14 += plusDM[i]
		minus14 += minusDM[i]
	}
	if tr14 == 0 {
		return 0, 0, 0
	}

	dxs := make([]float64, 0, n-period)
	plusDI = 100 * (plus14 / tr14)
	minusDI = 100 * (minus14 / tr14)
	denom := plusDI + minusDI
	if denom != 0 {
		dxs = append(dxs, 100*math.Abs(plusDI-minusDI)/denom)
	}

	for i := period + 1; i < n; i++ {
		tr14 = tr14 - (tr14 / float64(period)) + tr[i]
		plus14 = plus14 - (plus14 / float64(period)) + plusDM[i]
		minus14 = minus14 - (minus14 / float64(period)) + minusDM[i]

		if tr14 == 0 {
			continue
		}
		plusDI = 100 * (plus14 / tr14)
		minusDI = 100 * (minus14 / tr14)
		denom = plusDI + minusDI
		if denom == 0 {
			continue
		}
		dx := 100 * math.Abs(plusDI-minusDI) / denom
		dxs = append(dxs, dx)
	}

	if len(dxs) == 0 {
		return 0, plusDI, minusDI
	}

	window := period
	if window > len(dxs) {
		window = len(dxs)
	}
	sum := 0.0
	for i := len(dxs) - window; i < len(dxs); i++ {
		sum += dxs[i]
	}
	return sum / float64(window), plusDI, minusDI
}

// -----------------------
// Open/Close Strategy
// -----------------------

type OpenCloseConfig struct {
	Symbol                string
	Qty                   float64
	SellMinutesBeforeOpen int
	BuyMinutesBeforeClose int
}

type OpenCloseStrategy struct {
	BaseStrategy
	cfg                 OpenCloseConfig
	lastBuyDate         string
	lastSellDate        string
	pendingBuyID        string
	pendingSellID       string
	pendingBuyClientID  string
	pendingSellClientID string
}

func NewOpenCloseStrategy(client *AlpacaClient, cfg OpenCloseConfig) *OpenCloseStrategy {
	if cfg.Qty <= 0 {
		cfg.Qty = 1
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

func (s *OpenCloseStrategy) Name() string {
	return "open-close-" + strings.ToUpper(strings.TrimSpace(s.cfg.Symbol))
}
func (s *OpenCloseStrategy) Symbol() string { return s.cfg.Symbol }
func (s *OpenCloseStrategy) Config() map[string]interface{} {
	return map[string]interface{}{
		"symbol":                   s.cfg.Symbol,
		"qty":                      s.cfg.Qty,
		"sell_minutes_before_open": s.cfg.SellMinutesBeforeOpen,
		"buy_minutes_before_close": s.cfg.BuyMinutesBeforeClose,
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
	active, err := hasActiveOrder(ctx, s.client, s.cfg.Symbol)
	if err != nil {
		return fmt.Errorf("open-close active-order check failed: %w", err)
	}
	if active {
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

	if s.pendingSellClientID == "" {
		s.pendingSellClientID = fmt.Sprintf("open-sell-%s-%d", strings.ToLower(s.cfg.Symbol), time.Now().UnixNano())
	}
	out, err := s.client.PlaceOrderIdempotent(ctx, OrderRequest{
		Symbol:        s.cfg.Symbol,
		Qty:           fmt.Sprintf("%.6f", sellQty),
		Side:          "sell",
		Type:          "market",
		TimeInForce:   "day",
		ExtendedHours: false,
		ClientOrderID: s.pendingSellClientID,
	})
	if err == nil {
		s.pendingSellID = out.ID
		s.pendingSellClientID = ""
	} else if isDefinitiveSubmitError(err) {
		s.pendingSellClientID = ""
	}
	return err
}

func (s *OpenCloseStrategy) executeBuyBeforeClose(ctx context.Context, acct *Account, dateStr string) error {
	active, err := hasActiveOrder(ctx, s.client, s.cfg.Symbol)
	if err != nil {
		return fmt.Errorf("open-close active-order check failed: %w", err)
	}
	if active {
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

	if s.pendingBuyClientID == "" {
		s.pendingBuyClientID = fmt.Sprintf("close-buy-%s-%d", strings.ToLower(s.cfg.Symbol), time.Now().UnixNano())
	}
	out, err := s.client.PlaceOrderIdempotent(ctx, OrderRequest{
		Symbol:        s.cfg.Symbol,
		Qty:           fmt.Sprintf("%.6f", s.cfg.Qty),
		Side:          "buy",
		Type:          "market",
		TimeInForce:   "day",
		ExtendedHours: false,
		ClientOrderID: s.pendingBuyClientID,
	})
	if err == nil {
		s.pendingBuyID = out.ID
		s.pendingBuyClientID = ""
	} else if isDefinitiveSubmitError(err) {
		s.pendingBuyClientID = ""
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

func isGridOrderForSymbol(order Order, symbol string) bool {
	if !strings.EqualFold(order.Symbol, symbol) {
		return false
	}
	id := strings.ToLower(strings.TrimSpace(order.ClientOrderID))
	sym := strings.ToLower(strings.TrimSpace(symbol))

	if strings.HasPrefix(id, "grid-buy-"+sym+"-") ||
		strings.HasPrefix(id, "grid-sell-"+sym+"-") ||
		strings.HasPrefix(id, "grid-exit-"+sym+"-") ||
		strings.HasPrefix(id, "grid-seed-"+sym+"-") {
		return true
	}

	parts := strings.Split(id, "-")
	return len(parts) >= 5 &&
		parts[0] == "grid" &&
		(parts[1] == "buy" || parts[1] == "sell" || parts[1] == "exit") &&
		parts[3] == sym
}

func filterGridOrdersBySymbol(orders []Order, symbol string) []Order {
	out := make([]Order, 0, len(orders))
	for _, order := range orders {
		if isGridOrderForSymbol(order, symbol) {
			out = append(out, order)
		}
	}
	return out
}

func isActiveOrderStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "open", "new", "accepted", "pending_new", "accepted_for_bidding", "partially_filled", "held", "calculated":
		return true
	default:
		return false
	}
}

func remainingOrderQty(order Order) float64 {
	qty := parseFloatString(order.Qty)
	filled := parseFloatString(order.FilledQty)
	return math.Max(0, qty-filled)
}

func hasActiveOrder(ctx context.Context, client *AlpacaClient, symbol string) (bool, error) {
	orders, err := client.ListOrders(ctx, "open")
	if err != nil {
		return false, err
	}
	for _, order := range filterOrdersBySymbol(orders, symbol) {
		if isActiveOrderStatus(order.Status) {
			return true, nil
		}
	}
	return false, nil
}

type positionState struct {
	Qty           float64
	AvgEntryPrice float64
	CurrentPrice  float64
	Side          string
}

func currentPositionState(ctx context.Context, client *AlpacaClient, symbol string) (positionState, error) {
	pos, err := client.GetPosition(ctx, symbol)
	if err != nil {
		if isHTTPStatusError(err, http.StatusNotFound) {
			return positionState{}, nil
		}
		return positionState{}, err
	}
	state := positionState{
		Qty:           parseFloatString(pos.Qty),
		AvgEntryPrice: parseFloatString(pos.AvgEntryPrice),
		CurrentPrice:  parseFloatString(pos.CurrentPrice),
		Side:          strings.ToLower(strings.TrimSpace(pos.Side)),
	}
	if state.CurrentPrice <= 0 && math.Abs(state.Qty) > 0 {
		mv := parseFloatString(pos.MarketValue)
		if math.Abs(mv) > 0 {
			state.CurrentPrice = math.Abs(mv / state.Qty)
		}
	}
	return state, nil
}

func currentPositionQty(ctx context.Context, client *AlpacaClient, symbol string) (float64, error) {
	state, err := currentPositionState(ctx, client, symbol)
	return state.Qty, err
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
	Name            string
	Symbol          string
	TradeCount      int
	RealizedPnL     float64
	UnrealizedPnL   float64
	TotalPnL        float64
	ReturnPct       float64
	InvestedCapital float64
	PositionQty     float64
	AvgCost         float64
	LastPrice       float64
}

type strategyLedger struct {
	lots             map[string][]PositionLot
	realizedPnL      float64
	grossBuyNotional float64
	tradeCount       int
	seenOrders       map[string]struct{}
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
	seenFillNotional  map[string]float64
	seenFillEvents    map[string]struct{}
	globalLots        map[string][]PositionLot
	globalRealizedPnL float64
	lastPrices        map[string]float64
	livePositions     map[string]HoldingSummary
	positionsSynced   bool
	strategyStats     map[string]*StrategyStats
	strategyLedgers   map[string]*strategyLedger
	isRunning         bool
	stopMu            sync.Mutex
	stopFunc          context.CancelFunc
	runCtx            context.Context
	useWebSockets     bool
	riskConfig        RiskConfig
	riskState         RiskState
}

func NewBot(client *AlpacaClient, interval time.Duration) *Bot {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Bot{
		client:           client,
		strategies:       map[string]Strategy{},
		interval:         interval,
		seenFillQty:      map[string]float64{},
		seenFillNotional: map[string]float64{},
		seenFillEvents:   map[string]struct{}{},
		globalLots:       map[string][]PositionLot{},
		lastPrices:       map[string]float64{},
		livePositions:    map[string]HoldingSummary{},
		strategyStats:    map[string]*StrategyStats{},
		strategyLedgers:  map[string]*strategyLedger{},
		useWebSockets:    true,
		riskConfig:       LoadRiskConfig(),
	}
}

func (b *Bot) SetRiskConfig(cfg RiskConfig) {
	b.mu.Lock()
	b.riskConfig = cfg
	b.mu.Unlock()
}

func (b *Bot) loadRiskState() {
	b.mu.Lock()
	defer b.mu.Unlock()
	path := strings.TrimSpace(b.riskConfig.StateFile)
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("risk state read failed: %v", err)
		}
		return
	}
	var state RiskState
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("risk state decode failed: %v", err)
		return
	}
	b.riskState = state
}

func (b *Bot) persistRiskStateLocked() {
	path := strings.TrimSpace(b.riskConfig.StateFile)
	if path == "" {
		return
	}
	data, err := json.MarshalIndent(b.riskState, "", "  ")
	if err != nil {
		log.Printf("risk state encode failed: %v", err)
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		log.Printf("risk state write failed: %v", err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		log.Printf("risk state replace failed: %v", err)
	}
}

func (b *Bot) RegisterStrategy(s Strategy) {
	if err := b.RegisterStrategySafe(s); err != nil {
		log.Printf("strategy registration rejected: %v", err)
	}
}

func (b *Bot) RegisterStrategySafe(s Strategy) error {
	if s == nil {
		return errors.New("strategy is nil")
	}
	name := strings.TrimSpace(s.Name())
	symbol := strings.ToUpper(strings.TrimSpace(s.Symbol()))
	if name == "" || symbol == "" {
		return errors.New("strategy name and symbol are required")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if existing, exists := b.strategies[name]; exists && !strings.EqualFold(existing.Symbol(), symbol) {
		return fmt.Errorf("strategy name %s is already registered for symbol %s", name, existing.Symbol())
	}
	for existingName, existing := range b.strategies {
		if existingName != name && strings.EqualFold(existing.Symbol(), symbol) {
			return fmt.Errorf("symbol %s is already controlled by strategy %s; multiple strategies cannot safely share one account position", symbol, existingName)
		}
	}
	b.strategies[name] = s
	if _, ok := b.strategyStats[name]; !ok {
		b.strategyStats[name] = &StrategyStats{Name: name, Symbol: symbol}
	}
	if _, ok := b.strategyLedgers[name]; !ok {
		b.strategyLedgers[name] = &strategyLedger{lots: map[string][]PositionLot{}, seenOrders: map[string]struct{}{}}
	}
	return nil
}

func (b *Bot) runOnce(ctx context.Context) {
	b.mu.RLock()
	symbols := make([]string, 0, len(b.strategies))
	for _, s := range b.strategies {
		symbols = append(symbols, s.Symbol())
	}
	b.mu.RUnlock()

	livePrices := make(map[string]float64)
	for _, sym := range symbols {
		normalized := strings.ToUpper(strings.TrimSpace(sym))
		if normalized == "" {
			continue
		}
		price, err := b.client.GetReferencePrice(ctx, normalized)
		if err != nil {
			b.logError("price-"+normalized, err.Error())
			continue
		}
		livePrices[normalized] = price
	}
	positionsFetched := false
	rawPositions := make([]Position, 0)
	livePositions := make(map[string]HoldingSummary)
	if positions, err := b.client.GetPositions(ctx); err == nil {
		positionsFetched = true
		rawPositions = positions
		for _, p := range positions {
			sum := positionToHoldingSummary(p)
			key := strings.ToUpper(strings.TrimSpace(sum.Symbol))
			livePositions[key] = sum
		}
	} else {
		b.logError("system", "fetch positions failed: "+err.Error())
	}
	b.mu.Lock()
	for sym, price := range livePrices {
		b.lastPrices[sym] = price
	}
	if positionsFetched {
		b.livePositions = livePositions
		b.positionsSynced = true
	}
	b.recalcStrategyStatsLocked()
	b.mu.Unlock()

	acct, err := b.client.GetAccount(ctx)
	if err != nil {
		b.logError("system", "fetch account failed: "+err.Error())
		return
	}
	if !positionsFetched {
		return
	}
	openOrders, err := b.client.ListOrders(ctx, "open")
	if err != nil {
		b.logError("risk", "fetch open orders failed; refusing new orders: "+err.Error())
		return
	}
	allowed, newlyHalted, riskReason := b.evaluateAccountRisk(acct, rawPositions, openOrders)
	if !allowed {
		if newlyHalted {
			b.logError("risk", riskReason)
			b.enforceRiskHalt(ctx, riskReason)
		}
		b.recordSnapshotFromAccount(acct)
		return
	}
	b.recordSnapshotFromAccount(acct)
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
		spendable := b.accountForStrategy(acct, s.Symbol(), rawPositions, openOrders)
		if err := s.Tick(ctx, spendable, clock); err != nil {
			b.logError(s.Name(), err.Error())
		}
		// Refresh after every strategy so later strategies cannot all spend the
		// same stale cash/buying-power snapshot in one cycle.
		if refreshed, refreshErr := b.client.GetAccount(ctx); refreshErr == nil {
			acct = refreshed
		} else if ctx.Err() == nil {
			b.logError("system", "post-strategy account refresh failed: "+refreshErr.Error())
			return
		}
		refreshedPositions, positionsErr := b.client.GetPositions(ctx)
		refreshedOrders, ordersErr := b.client.ListOrders(ctx, "open")
		if positionsErr != nil || ordersErr != nil {
			b.logError("risk", fmt.Sprintf("post-strategy exposure refresh failed; refusing further orders: positions=%v orders=%v", positionsErr, ordersErr))
			return
		}
		rawPositions = refreshedPositions
		openOrders = refreshedOrders
		allowed, newlyHalted, riskReason = b.evaluateAccountRisk(acct, rawPositions, openOrders)
		if !allowed {
			if newlyHalted {
				b.logError("risk", riskReason)
				b.enforceRiskHalt(ctx, riskReason)
			}
			return
		}
	}
}

func (b *Bot) accountForStrategy(acct *Account, symbol string, positions []Position, openOrders []Order) *Account {
	if acct == nil {
		return &Account{}
	}
	copyAccount := *acct
	b.mu.RLock()
	cashOnly := b.riskConfig.CashOnly
	riskConfig := b.riskConfig
	b.mu.RUnlock()
	available := math.Max(0, parseFloatString(acct.BuyingPower))
	if cashOnly {
		available = math.Min(available, math.Max(0, parseFloatString(acct.Cash)))
		if strings.TrimSpace(acct.NonMarginableBuyingPower) != "" {
			available = math.Min(available, math.Max(0, parseFloatString(acct.NonMarginableBuyingPower)))
		}
	}

	// Reserve both filled exposure and live buy orders before handing a budget to
	// a strategy. This prevents multiple strategies from all spending the same
	// account-level capacity during one scheduler cycle.
	equity := parseFloatString(acct.Equity)
	grossExposure, symbolExposure := calculateRiskExposure(positions, openOrders)
	if equity > 0 && riskConfig.MaxGrossExposurePct > 0 {
		grossRemaining := math.Max(0, equity*riskConfig.MaxGrossExposurePct-grossExposure)
		available = math.Min(available, grossRemaining)
	}
	if equity > 0 && riskConfig.MaxSymbolExposurePct > 0 {
		symbolKey := strings.ToUpper(strings.TrimSpace(symbol))
		symbolRemaining := math.Max(0, equity*riskConfig.MaxSymbolExposurePct-symbolExposure[symbolKey])
		available = math.Min(available, symbolRemaining)
	}
	copyAccount.BuyingPower = strconv.FormatFloat(available, 'f', 2, 64)
	return &copyAccount
}

func calculateRiskExposure(positions []Position, openOrders []Order) (float64, map[string]float64) {
	bySymbol := make(map[string]float64)
	for _, position := range positions {
		symbol := strings.ToUpper(strings.TrimSpace(position.Symbol))
		marketValue := math.Abs(parseFloatString(position.MarketValue))
		bySymbol[symbol] += marketValue
	}
	for _, order := range openOrders {
		if !strings.EqualFold(order.Side, "buy") || !isActiveOrderStatus(order.Status) {
			continue
		}
		price := parseFloatString(order.LimitPrice)
		if price <= 0 {
			price = parseFloatString(order.FilledAvgPrice)
		}
		reserved := remainingOrderQty(order) * price
		if reserved <= 0 {
			continue
		}
		symbol := strings.ToUpper(strings.TrimSpace(order.Symbol))
		bySymbol[symbol] += reserved
	}
	gross := 0.0
	for _, exposure := range bySymbol {
		gross += exposure
	}
	return gross, bySymbol
}

func (b *Bot) evaluateAccountRisk(acct *Account, positions []Position, openOrders []Order) (allowed, newlyHalted bool, reason string) {
	if acct == nil {
		return false, false, "account unavailable"
	}
	now := time.Now().UTC()
	day, _, _ := tradingDayBounds(now)
	equity := parseFloatString(acct.Equity)
	if equity <= 0 {
		return false, false, "account equity is not positive"
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.riskState.Halted {
		return false, false, b.riskState.Reason
	}
	if b.riskState.SessionDay != day || b.riskState.SessionEquity <= 0 {
		b.riskState.SessionDay = day
		b.riskState.SessionEquity = equity
	}
	if b.riskState.HighWaterEquity <= 0 || equity > b.riskState.HighWaterEquity {
		b.riskState.HighWaterEquity = equity
	}

	dailyLossPct := math.Max(0, (b.riskState.SessionEquity-equity)/b.riskState.SessionEquity)
	drawdownPct := math.Max(0, (b.riskState.HighWaterEquity-equity)/b.riskState.HighWaterEquity)
	grossExposure, exposureBySymbol := calculateRiskExposure(positions, openOrders)
	maxSymbolExposure := 0.0
	maxSymbol := ""
	for symbol, exposure := range exposureBySymbol {
		if exposure > maxSymbolExposure {
			maxSymbolExposure = exposure
			maxSymbol = symbol
		}
	}
	grossExposurePct := grossExposure / equity
	maxSymbolExposurePct := maxSymbolExposure / equity

	b.riskState.DailyLossPct = dailyLossPct
	b.riskState.DrawdownPct = drawdownPct
	b.riskState.GrossExposurePct = grossExposurePct
	b.riskState.UpdatedAt = now.Format(time.RFC3339)

	switch {
	case acct.AccountBlocked:
		reason = "broker account is blocked"
	case acct.TradingBlocked:
		reason = "broker trading is blocked"
	case acct.TradeSuspendedByUser:
		reason = "trading is suspended by user"
	case b.riskConfig.MaxDailyLossPct > 0 && dailyLossPct >= b.riskConfig.MaxDailyLossPct:
		reason = fmt.Sprintf("daily loss %.2f%% reached limit %.2f%%", dailyLossPct*100, b.riskConfig.MaxDailyLossPct*100)
	case b.riskConfig.MaxDrawdownPct > 0 && drawdownPct >= b.riskConfig.MaxDrawdownPct:
		reason = fmt.Sprintf("drawdown %.2f%% reached limit %.2f%%", drawdownPct*100, b.riskConfig.MaxDrawdownPct*100)
	case b.riskConfig.MaxGrossExposurePct > 0 && grossExposurePct > b.riskConfig.MaxGrossExposurePct:
		reason = fmt.Sprintf("gross exposure %.2f%% exceeds limit %.2f%%", grossExposurePct*100, b.riskConfig.MaxGrossExposurePct*100)
	case b.riskConfig.MaxSymbolExposurePct > 0 && maxSymbolExposurePct > b.riskConfig.MaxSymbolExposurePct:
		reason = fmt.Sprintf("%s exposure %.2f%% exceeds limit %.2f%%", maxSymbol, maxSymbolExposurePct*100, b.riskConfig.MaxSymbolExposurePct*100)
	}
	if reason == "" {
		b.persistRiskStateLocked()
		return true, false, ""
	}
	b.riskState.Halted = true
	b.riskState.Reason = reason
	b.persistRiskStateLocked()
	return false, true, reason
}

func (b *Bot) enforceRiskHalt(ctx context.Context, reason string) {
	log.Printf("RISK HALT: %s", reason)
	if _, err := b.client.CancelAllOrders(ctx); err != nil {
		b.logError("risk", "cancel all orders failed: "+err.Error())
	}
	b.mu.RLock()
	liquidate := b.riskConfig.LiquidateOnRiskHalt
	b.mu.RUnlock()
	if liquidate {
		if _, err := b.client.CloseAllPositions(ctx); err != nil {
			b.logError("risk", "close all positions failed: "+err.Error())
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
	b.recordSnapshotFromAccount(acct)
}

func (b *Bot) recordSnapshotFromAccount(acct *Account) {
	if acct == nil {
		return
	}
	b.snapshotMu.Lock()
	defer b.snapshotMu.Unlock()
	b.snapshots = append(b.snapshots, DailySnapshot{
		Time:   time.Now().UTC(),
		Equity: parseFloatString(acct.Equity),
	})
	if len(b.snapshots) > maxSnapshots {
		b.snapshots = append([]DailySnapshot(nil), b.snapshots[len(b.snapshots)-maxSnapshots:]...)
	}
}

func (b *Bot) logError(strategy, msg string) {
	log.Printf("[%s] %s", strategy, msg)
	b.errorLogMu.Lock()
	defer b.errorLogMu.Unlock()
	b.errorLog = append(b.errorLog, ErrorRecord{Time: time.Now().UTC(), Strategy: strategy, Error: msg})
	if len(b.errorLog) > maxErrorLogLen {
		b.errorLog = b.errorLog[len(b.errorLog)-maxErrorLogLen:]
	}
}

func effectiveOrderTime(order Order) time.Time {
	if order.FilledAt != nil {
		return *order.FilledAt
	}
	if order.UpdatedAt != nil {
		return *order.UpdatedAt
	}
	if order.CreatedAt != nil {
		return *order.CreatedAt
	}
	return time.Time{}
}

func (b *Bot) appendTradeRecordLocked(record TradeRecord) {
	b.tradeRecords = append(b.tradeRecords, record)
	if len(b.tradeRecords) > maxTradeRecords {
		b.tradeRecords = append([]TradeRecord(nil), b.tradeRecords[len(b.tradeRecords)-maxTradeRecords:]...)
	}
}

func (b *Bot) syncOrderFills(ctx context.Context) error {
	orders, err := b.client.ListOrders(ctx, "all")
	if err != nil {
		return err
	}

	// The API normally returns newest first. Replaying fills in reverse order
	// corrupts FIFO lots and realized PnL, so always process oldest first.
	sort.SliceStable(orders, func(i, j int) bool {
		left := effectiveOrderTime(orders[i])
		right := effectiveOrderTime(orders[j])
		if left.Equal(right) {
			return orders[i].ID < orders[j].ID
		}
		if left.IsZero() {
			return true
		}
		if right.IsZero() {
			return false
		}
		return left.Before(right)
	})

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, order := range orders {
		filledQty := parseFloatString(order.FilledQty)
		previousFilled := b.seenFillQty[order.ID]
		if filledQty <= previousFilled+1e-9 {
			continue
		}
		delta := filledQty - previousFilled
		cumulativeAvg := parseFloatString(order.FilledAvgPrice)
		cumulativeNotional := filledQty * cumulativeAvg
		previousNotional := b.seenFillNotional[order.ID]
		price := cumulativeAvg
		if delta > 0 && cumulativeNotional > previousNotional {
			price = (cumulativeNotional - previousNotional) / delta
		}
		if delta <= 0 || price <= 0 {
			continue
		}

		fillTime := effectiveOrderTime(order)
		if fillTime.IsZero() {
			fillTime = time.Now().UTC()
		}
		strategyName := detectStrategyName(order.ClientOrderID, order.Symbol)
		record := TradeRecord{
			Time:          fillTime,
			Symbol:        strings.ToUpper(strings.TrimSpace(order.Symbol)),
			Side:          strings.ToLower(strings.TrimSpace(order.Side)),
			Qty:           delta,
			Price:         price,
			OrderID:       order.ID,
			ClientOrderID: order.ClientOrderID,
			Strategy:      strategyName,
		}
		b.appendTradeRecordLocked(record)
		b.applyGlobalFill(record.Symbol, record.Side, delta, price)
		b.applyStrategyFill(strategyName, record.Symbol, record.Side, order.ID, delta, price, fillTime)
		b.seenFillQty[order.ID] = filledQty
		if cumulativeNotional > 0 {
			b.seenFillNotional[order.ID] = cumulativeNotional
		} else {
			b.seenFillNotional[order.ID] += delta * price
		}
		b.lastPrices[record.Symbol] = price
		b.notifyStrategyFill(strategyName, order.ID, record.Side, delta, price, fillTime)
	}

	// Do not delete deduplication state just because an order falls outside the
	// latest 500-order response. A websocket replay after reconnect must not be
	// counted a second time.
	b.recalcStrategyStatsLocked()
	return nil
}

func detectStrategyName(clientOrderID, symbol string) string {
	id := strings.ToLower(strings.TrimSpace(clientOrderID))
	sym := strings.ToUpper(strings.TrimSpace(symbol))
	switch {
	case strings.HasPrefix(id, "grid-buy-"),
		strings.HasPrefix(id, "grid-sell-"),
		strings.HasPrefix(id, "grid-exit-"),
		strings.HasPrefix(id, "grid-seed-"):
		return "grid-" + sym
	case strings.HasPrefix(id, "open-sell-"),
		strings.HasPrefix(id, "close-buy-"),
		strings.HasPrefix(id, "open-buy-"),
		strings.HasPrefix(id, "close-sell-"),
		strings.HasPrefix(id, "open-close-"),
		strings.HasPrefix(id, "overnight-"):
		return "open-close-" + sym
	default:
		return "unknown"
	}
}

func (b *Bot) applyGlobalFill(symbol, side string, qty, price float64) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	side = strings.ToLower(strings.TrimSpace(side))
	if qty <= 0 || price <= 0 || symbol == "" {
		return
	}
	if side == "buy" {
		b.globalLots[symbol] = append(b.globalLots[symbol], PositionLot{Qty: qty, Price: price, Time: time.Now().UTC()})
	} else if side == "sell" {
		b.globalLots[symbol] = consumeLots(b.globalLots[symbol], qty, price, &b.globalRealizedPnL)
	}
}

func (b *Bot) applyStrategyFill(stratName, symbol, side, orderID string, qty, price float64, fillTime time.Time) {
	ledger := b.strategyLedgers[stratName]
	if ledger == nil {
		return
	}
	if ledger.seenOrders == nil {
		ledger.seenOrders = map[string]struct{}{}
	}
	if orderID != "" {
		if _, exists := ledger.seenOrders[orderID]; !exists {
			ledger.seenOrders[orderID] = struct{}{}
			ledger.tradeCount++
		}
	} else {
		ledger.tradeCount++
	}

	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	side = strings.ToLower(strings.TrimSpace(side))
	if fillTime.IsZero() {
		fillTime = time.Now().UTC()
	}
	if side == "buy" {
		ledger.grossBuyNotional += qty * price
		ledger.lots[symbol] = append(ledger.lots[symbol], PositionLot{Qty: qty, Price: price, Time: fillTime})
	} else if side == "sell" {
		ledger.lots[symbol] = consumeLots(ledger.lots[symbol], qty, price, &ledger.realizedPnL)
	}
}

func (b *Bot) notifyStrategyFill(stratName, orderID, side string, qty, price float64, fillTime time.Time) {
	strategy := b.strategies[stratName]
	aware, ok := strategy.(FillAwareStrategy)
	if !ok || aware == nil {
		return
	}
	aware.OnFill(orderID, side, qty, price, fillTime)
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
		stat.InvestedCapital = ledger.grossBuyNotional

		symbol := strings.ToUpper(strings.TrimSpace(stat.Symbol))
		live, ok := b.livePositions[symbol]
		if ok {
			actualQty := math.Max(0, live.Qty)
			lots := ledger.lots[symbol]
			ledgerQty := 0.0
			for _, lot := range lots {
				ledgerQty += lot.Qty
			}

			switch {
			case actualQty <= 1e-6:
				ledger.lots[symbol] = nil
			case ledgerQty > actualQty+1e-6:
				// Reconciliation may miss the exact partial-fill sequence; align the
				// remaining quantity with the broker's authoritative average cost.
				ledger.lots[symbol] = []PositionLot{{Qty: actualQty, Price: live.AvgEntryPrice, Time: time.Now().UTC()}}
			case ledgerQty+1e-6 < actualQty:
				// Position existed before the retained order history or before the bot
				// started. Seed the missing quantity at Alpaca's average entry price.
				ledger.lots[symbol] = append(ledger.lots[symbol], PositionLot{
					Qty:   actualQty - ledgerQty,
					Price: live.AvgEntryPrice,
					Time:  time.Now().UTC(),
				})
			}

			stat.PositionQty = actualQty
			stat.AvgCost = live.AvgEntryPrice
			last := live.CurrentPrice
			if last <= 0 {
				last = b.lastPrices[symbol]
			}
			stat.LastPrice = last
			if live.UnrealizedPnL != 0 || actualQty == 0 {
				stat.UnrealizedPnL = live.UnrealizedPnL
			} else if actualQty > 0 && stat.AvgCost > 0 && last > 0 {
				if strings.EqualFold(live.Side, "short") {
					stat.UnrealizedPnL = actualQty * (stat.AvgCost - last)
				} else {
					stat.UnrealizedPnL = actualQty * (last - stat.AvgCost)
				}
			} else {
				stat.UnrealizedPnL = 0
			}
			stat.TotalPnL = stat.RealizedPnL + stat.UnrealizedPnL
			updateStrategyReturn(stat)
			continue
		}

		if b.positionsSynced {
			// A successful positions response omitted the symbol, so the broker says
			// the position is flat. Do not keep phantom lots in the dashboard.
			ledger.lots[symbol] = nil
			stat.PositionQty = 0
			stat.AvgCost = 0
			stat.LastPrice = b.lastPrices[symbol]
			stat.UnrealizedPnL = 0
			stat.TotalPnL = stat.RealizedPnL
			updateStrategyReturn(stat)
			continue
		}

		qty, cost, unrealized := 0.0, 0.0, 0.0
		mark := b.lastPrices[symbol]
		for _, lot := range ledger.lots[symbol] {
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
		updateStrategyReturn(stat)
	}
}

func updateStrategyReturn(stat *StrategyStats) {
	if stat == nil || stat.InvestedCapital <= 0 {
		if stat != nil {
			stat.ReturnPct = 0
		}
		return
	}
	stat.ReturnPct = stat.TotalPnL / stat.InvestedCapital * 100
}

// -----------------------
// WebSocket Streaming
// -----------------------

type priceEntry struct {
	price      float64
	receivedAt time.Time
	marketTime time.Time
	source     string
}

type PriceCache struct {
	mu     sync.RWMutex
	prices map[string]priceEntry
}

func NewPriceCache() *PriceCache {
	return &PriceCache{prices: map[string]priceEntry{}}
}

func (pc *PriceCache) Set(symbol string, price float64) {
	pc.SetFromSource(symbol, price, "unknown", time.Time{})
}

func (pc *PriceCache) SetFromSource(symbol string, price float64, source string, marketTime time.Time) {
	if pc == nil || price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return
	}
	source = strings.ToLower(strings.TrimSpace(source))
	now := time.Now()

	pc.mu.Lock()
	defer pc.mu.Unlock()

	previous, exists := pc.prices[symbol]
	if !marketTime.IsZero() && marketTime.After(now.Add(5*time.Minute)) {
		return
	}
	if exists && !marketTime.IsZero() && !previous.marketTime.IsZero() && marketTime.Before(previous.marketTime.Add(-time.Second)) {
		// Ignore delayed/out-of-order websocket packets.
		return
	}
	if exists && strings.Contains(source, "quote") && strings.Contains(previous.source, "trade") && now.Sub(previous.receivedAt) <= tradePreferenceWindow {
		return
	}
	pc.prices[symbol] = priceEntry{
		price:      price,
		receivedAt: now,
		marketTime: marketTime,
		source:     source,
	}
}

func (pc *PriceCache) Get(symbol string) (float64, bool) {
	if pc == nil {
		return 0, false
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	pc.mu.RLock()
	entry, ok := pc.prices[symbol]
	pc.mu.RUnlock()
	if !ok || entry.price <= 0 {
		return 0, false
	}
	if priceStaleThreshold > 0 && time.Since(entry.receivedAt) > priceStaleThreshold {
		return 0, false
	}
	return entry.price, true
}

func (pc *PriceCache) Snapshot() map[string]float64 {
	if pc == nil {
		return map[string]float64{}
	}
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	out := make(map[string]float64, len(pc.prices))
	for symbol, entry := range pc.prices {
		if entry.price > 0 && (priceStaleThreshold <= 0 || time.Since(entry.receivedAt) <= priceStaleThreshold) {
			out[symbol] = entry.price
		}
	}
	return out
}

func (c *AlpacaClient) AttachPriceCache(cache *PriceCache) {
	c.priceCache = cache
}

func (c *AlpacaClient) CachePrice(symbol string, price float64) {
	c.CacheMarketPrice(symbol, price, "unknown", time.Time{})
}

func (c *AlpacaClient) CacheMarketPrice(symbol string, price float64, source string, marketTime time.Time) {
	if c.priceCache != nil {
		c.priceCache.SetFromSource(symbol, price, source, marketTime)
	}
}

func (c *AlpacaClient) LatestPrice(symbol string) (float64, bool) {
	if c.priceCache == nil {
		return 0, false
	}
	return c.priceCache.Get(symbol)
}

func (c *AlpacaClient) marketStreamURL() string {
	feed := strings.TrimSpace(c.feed)
	if feed == "" {
		feed = "iex"
	}
	if strings.Contains(strings.ToLower(c.dataURL), "sandbox") {
		return fmt.Sprintf("wss://stream.data.sandbox.alpaca.markets/v2/%s", feed)
	}
	return fmt.Sprintf("wss://stream.data.alpaca.markets/v2/%s", feed)
}

func (c *AlpacaClient) tradingStreamURL() string {
	if strings.Contains(strings.ToLower(c.baseURL), "sandbox") {
		return "wss://paper-api.sandbox.alpaca.markets/stream"
	}
	if strings.Contains(strings.ToLower(c.baseURL), "paper") {
		return "wss://paper-api.alpaca.markets/stream"
	}
	return "wss://api.alpaca.markets/stream"
}

func (c *AlpacaClient) CloseStreams() {
	c.wsMu.Lock()
	if c.marketCancel != nil {
		c.marketCancel()
		c.marketCancel = nil
	}
	if c.tradingCancel != nil {
		c.tradingCancel()
		c.tradingCancel = nil
	}
	marketConn := c.marketConn
	tradingConn := c.tradingConn
	c.marketConn = nil
	c.tradingConn = nil
	c.wsMu.Unlock()
	// Closing the sockets is required: canceling context alone does not unblock
	// gorilla/websocket ReadMessage while the connection is idle.
	if marketConn != nil {
		_ = marketConn.Close()
	}
	if tradingConn != nil {
		_ = tradingConn.Close()
	}
}

func (c *AlpacaClient) registerMarketConn(ctx context.Context, conn *websocket.Conn) bool {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	if ctx.Err() != nil || c.marketCancel == nil {
		return false
	}
	c.marketConn = conn
	return true
}

func (c *AlpacaClient) releaseMarketConn(conn *websocket.Conn) {
	c.wsMu.Lock()
	if c.marketConn == conn {
		c.marketConn = nil
	}
	c.wsMu.Unlock()
	_ = conn.Close()
}

func (c *AlpacaClient) registerTradingConn(ctx context.Context, conn *websocket.Conn) bool {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	if ctx.Err() != nil || c.tradingCancel == nil {
		return false
	}
	c.tradingConn = conn
	return true
}

func (c *AlpacaClient) releaseTradingConn(conn *websocket.Conn) {
	c.wsMu.Lock()
	if c.tradingConn == conn {
		c.tradingConn = nil
	}
	c.wsMu.Unlock()
	_ = conn.Close()
}

func (c *AlpacaClient) StartMarketDataStream(ctx context.Context, symbols []string) {
	c.wsMu.Lock()
	if c.marketCancel != nil {
		c.wsMu.Unlock()
		return
	}
	streamCtx, cancel := context.WithCancel(ctx)
	c.marketCancel = cancel
	c.wsMu.Unlock()

	go c.runMarketStream(streamCtx, symbols)
}

func (c *AlpacaClient) StartTradingStream(ctx context.Context, onUpdate func(TradeUpdateEnvelope)) {
	c.wsMu.Lock()
	if c.tradingCancel != nil {
		c.wsMu.Unlock()
		return
	}
	streamCtx, cancel := context.WithCancel(ctx)
	c.tradingCancel = cancel
	c.wsMu.Unlock()

	go c.runTradingStream(streamCtx, onUpdate)
}

func (c *AlpacaClient) RunStreams(ctx context.Context, symbols []string, onUpdate func(TradeUpdateEnvelope)) {
	marketCtx, marketCancel := context.WithCancel(ctx)
	tradingCtx, tradingCancel := context.WithCancel(ctx)

	c.wsMu.Lock()
	if c.marketCancel != nil {
		c.marketCancel()
	}
	if c.tradingCancel != nil {
		c.tradingCancel()
	}
	c.marketCancel = marketCancel
	c.tradingCancel = tradingCancel
	c.wsMu.Unlock()

	var streams sync.WaitGroup
	streams.Add(2)
	go func() {
		defer streams.Done()
		c.runMarketStream(marketCtx, symbols)
	}()
	go func() {
		defer streams.Done()
		c.runTradingStream(tradingCtx, onUpdate)
	}()
	streams.Wait()

	c.wsMu.Lock()
	c.marketCancel = nil
	c.tradingCancel = nil
	c.wsMu.Unlock()
}

// Alpaca market-data payloads deliberately use both uppercase and lowercase
// keys with different meanings. Examples:
//
//	"T" = event type, while "t" = timestamp
//	"S" = symbol, while "s" = trade size
//	"C" and "c" may also have unrelated meanings in other event types
//
// encoding/json matches struct fields case-insensitively, so decoding these
// events into structs with fields tagged only as "T" or "S" can make the
// lowercase keys overwrite or fail against the uppercase fields. Decode each
// event into a raw-key map instead, then read the exact case-sensitive key.
type marketRawEvent map[string]json.RawMessage

type marketEventHeader struct {
	Type   string
	Symbol string
	Code   int
	Msg    string
}

func decodeMarketEventHeader(raw json.RawMessage) (marketRawEvent, marketEventHeader, error) {
	var fields marketRawEvent
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, marketEventHeader{}, err
	}

	var header marketEventHeader
	if value, ok := fields["T"]; ok {
		if err := json.Unmarshal(value, &header.Type); err != nil {
			return nil, marketEventHeader{}, fmt.Errorf("decode exact key T: %w", err)
		}
	}
	if value, ok := fields["S"]; ok {
		if err := json.Unmarshal(value, &header.Symbol); err != nil {
			return nil, marketEventHeader{}, fmt.Errorf("decode exact key S: %w", err)
		}
	}
	if value, ok := fields["code"]; ok {
		if err := json.Unmarshal(value, &header.Code); err != nil {
			return nil, marketEventHeader{}, fmt.Errorf("decode exact key code: %w", err)
		}
	}
	if value, ok := fields["msg"]; ok {
		if err := json.Unmarshal(value, &header.Msg); err != nil {
			return nil, marketEventHeader{}, fmt.Errorf("decode exact key msg: %w", err)
		}
	}
	return fields, header, nil
}

func rawFloat64(fields marketRawEvent, key string) (float64, error) {
	value, ok := fields[key]
	if !ok || len(value) == 0 || string(value) == "null" {
		return 0, nil
	}
	var out float64
	if err := json.Unmarshal(value, &out); err != nil {
		return 0, fmt.Errorf("decode exact key %s: %w", key, err)
	}
	return out, nil
}

func rawTime(fields marketRawEvent, key string) (time.Time, error) {
	value, ok := fields[key]
	if !ok || len(value) == 0 || string(value) == "null" {
		return time.Time{}, nil
	}
	var out time.Time
	if err := json.Unmarshal(value, &out); err != nil {
		return time.Time{}, fmt.Errorf("decode exact key %s: %w", key, err)
	}
	return out, nil
}

func (c *AlpacaClient) runMarketStream(ctx context.Context, symbols []string) {
	backoff := 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}

		conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.marketStreamURL(), nil)
		if err != nil {
			log.Printf("market websocket connect failed: %v", err)
			if !sleepWithContext(ctx, backoff) {
				return
			}
			if backoff < 15*time.Second {
				backoff *= 2
			}
			continue
		}
		if !c.registerMarketConn(ctx, conn) {
			_ = conn.Close()
			return
		}

		log.Printf("market websocket connected: url=%s", c.marketStreamURL())
		backoff = 2 * time.Second

		if err := conn.WriteJSON(map[string]any{
			"action": "auth",
			"key":    c.apiKey,
			"secret": c.secret,
		}); err != nil {
			c.releaseMarketConn(conn)
			log.Printf("market websocket auth write failed: %v", err)
			if !sleepWithContext(ctx, backoff) {
				return
			}
			continue
		}

		subs := normalizeSymbols(symbols)
		if len(subs) == 0 {
			c.releaseMarketConn(conn)
			log.Printf("market websocket has no valid symbols; stream stopped")
			return
		}

		// Only quote and trade events are needed for a live reference price.
		// Not subscribing to bars also avoids the overloaded JSON field "c",
		// which is numeric for bars but an array of conditions for trades.
		sub := map[string]any{
			"action": "subscribe",
			"quotes": subs,
			"trades": subs,
		}
		log.Printf("market websocket subscribe: symbols=%v", subs)
		if err := conn.WriteJSON(sub); err != nil {
			c.releaseMarketConn(conn)
			log.Printf("market websocket subscribe write failed: %v", err)
			if !sleepWithContext(ctx, backoff) {
				return
			}
			continue
		}

		readErr := c.readMarketLoop(ctx, conn)
		c.releaseMarketConn(conn)
		if ctx.Err() != nil {
			return
		}
		if readErr != nil {
			log.Printf("market websocket reconnecting after error: %v", readErr)
			if !sleepWithContext(ctx, backoff) {
				return
			}
		}
	}
}

func (c *AlpacaClient) readMarketLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		events, err := splitMarketEvents(message)
		if err != nil {
			log.Printf("market websocket frame decode failed: %v message=%s", err, truncateLog(string(message), 500))
			continue
		}

		for _, raw := range events {
			fields, header, err := decodeMarketEventHeader(raw)
			if err != nil {
				log.Printf("market websocket event header decode failed: %v event=%s", err, truncateLog(string(raw), 300))
				continue
			}

			symbol := strings.ToUpper(strings.TrimSpace(header.Symbol))
			switch strings.ToLower(strings.TrimSpace(header.Type)) {
			case "q":
				if symbol == "" {
					log.Printf("market quote missing exact key S: event=%s", truncateLog(string(raw), 300))
					continue
				}
				bid, bidErr := rawFloat64(fields, "bp")
				ask, askErr := rawFloat64(fields, "ap")
				marketTime, timeErr := rawTime(fields, "t")
				if bidErr != nil || askErr != nil || timeErr != nil {
					log.Printf("market quote decode failed: bid=%v ask=%v time=%v event=%s", bidErr, askErr, timeErr, truncateLog(string(raw), 300))
					continue
				}
				price, reason := quoteReferencePrice(bid, ask)
				if price > 0 {
					c.CacheMarketPrice(symbol, price, "ws-quote", marketTime)
					if marketDebugEnabled() {
						log.Printf("market quote: symbol=%s bid=%.6f ask=%.6f accepted=%.6f", symbol, bid, ask, price)
					}
				} else if marketDebugEnabled() {
					log.Printf("market quote rejected: symbol=%s bid=%.6f ask=%.6f reason=%s", symbol, bid, ask, reason)
				}

			case "t":
				if symbol == "" {
					log.Printf("market trade missing exact key S: event=%s", truncateLog(string(raw), 300))
					continue
				}
				price, priceErr := rawFloat64(fields, "p")
				marketTime, timeErr := rawTime(fields, "t")
				if priceErr != nil || timeErr != nil {
					log.Printf("market trade decode failed: price=%v time=%v event=%s", priceErr, timeErr, truncateLog(string(raw), 300))
					continue
				}
				if price > 0 {
					c.CacheMarketPrice(symbol, price, "ws-trade", marketTime)
					if marketDebugEnabled() {
						log.Printf("market trade: symbol=%s price=%.6f", symbol, price)
					}
				}

			case "success":
				log.Printf("market websocket success: %s", header.Msg)
			case "subscription":
				log.Printf("market websocket subscription confirmed")
			case "error":
				return fmt.Errorf("server error code=%d message=%s", header.Code, header.Msg)
			case "":
				log.Printf("market websocket event missing exact key T: event=%s", truncateLog(string(raw), 300))
			default:
				if marketDebugEnabled() {
					log.Printf("market websocket ignored event type=%q event=%s", header.Type, truncateLog(string(raw), 300))
				}
			}
		}
	}
}

func splitMarketEvents(message []byte) ([]json.RawMessage, error) {
	var batch []json.RawMessage
	if err := json.Unmarshal(message, &batch); err == nil {
		return batch, nil
	}

	var single json.RawMessage
	if err := json.Unmarshal(message, &single); err != nil {
		return nil, err
	}
	return []json.RawMessage{single}, nil
}

func normalizeSymbols(symbols []string) []string {
	seen := make(map[string]struct{}, len(symbols))
	out := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		symbol = strings.ToUpper(strings.TrimSpace(symbol))
		if symbol == "" {
			continue
		}
		if _, exists := seen[symbol]; exists {
			continue
		}
		seen[symbol] = struct{}{}
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

func quoteReferencePrice(bid, ask float64) (float64, string) {
	if bid <= 0 || ask <= 0 {
		return 0, "missing bid or ask"
	}
	if ask < bid {
		return 0, "crossed quote"
	}
	mid := (bid + ask) / 2
	if mid <= 0 {
		return 0, "invalid midpoint"
	}
	spreadPct := (ask - bid) / mid
	if spreadPct > maxQuoteSpreadPct {
		return 0, fmt.Sprintf("spread %.2f%% exceeds %.2f%%", spreadPct*100, maxQuoteSpreadPct*100)
	}
	return mid, ""
}

func marketDebugEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("MARKET_DEBUG")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func sleepWithContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func truncateLog(value string, maxLen int) string {
	if maxLen <= 0 || len(value) <= maxLen {
		return value
	}
	return value[:maxLen] + "..."
}

type TradeUpdateEnvelope struct {
	Stream string          `json:"stream"`
	Data   TradeUpdateData `json:"data"`
}

type TradeUpdateData struct {
	Event       string    `json:"event"`
	Timestamp   time.Time `json:"timestamp"`
	Price       string    `json:"price"`
	Qty         string    `json:"qty"`
	PositionQty string    `json:"position_qty"`
	Order       Order     `json:"order"`
}

func (c *AlpacaClient) runTradingStream(ctx context.Context, onUpdate func(TradeUpdateEnvelope)) {
	backoff := 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.tradingStreamURL(), nil)
		if err != nil {
			log.Printf("trading websocket connect failed: %v", err)
			if !sleepWithContext(ctx, backoff) {
				return
			}
			if backoff < 15*time.Second {
				backoff *= 2
			}
			continue
		}
		if !c.registerTradingConn(ctx, conn) {
			_ = conn.Close()
			return
		}
		backoff = 2 * time.Second

		if err := conn.WriteJSON(map[string]any{
			"action": "auth",
			"key":    c.apiKey,
			"secret": c.secret,
		}); err != nil {
			c.releaseTradingConn(conn)
			log.Printf("trading websocket auth write failed: %v", err)
			if !sleepWithContext(ctx, backoff) {
				return
			}
			continue
		}
		if err := conn.WriteJSON(map[string]any{
			"action": "listen",
			"data": map[string]any{
				"streams": []string{"trade_updates"},
			},
		}); err != nil {
			c.releaseTradingConn(conn)
			log.Printf("trading websocket listen write failed: %v", err)
			if !sleepWithContext(ctx, backoff) {
				return
			}
			continue
		}

		readErr := c.readTradingLoop(ctx, conn, onUpdate)
		c.releaseTradingConn(conn)
		if ctx.Err() != nil {
			return
		}
		if readErr != nil {
			log.Printf("trading websocket reconnecting after error: %v", readErr)
			if !sleepWithContext(ctx, backoff) {
				return
			}
		}
	}
}

func (c *AlpacaClient) readTradingLoop(ctx context.Context, conn *websocket.Conn, onUpdate func(TradeUpdateEnvelope)) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		var env TradeUpdateEnvelope
		if err := json.Unmarshal(msg, &env); err != nil {
			log.Printf("trading websocket decode failed: %v message=%s", err, truncateLog(string(msg), 500))
			continue
		}
		if env.Stream != "trade_updates" {
			continue
		}
		if onUpdate != nil {
			onUpdate(env)
		}
	}
}

func (b *Bot) startWebSockets(ctx context.Context) {
	b.mu.RLock()
	rawSymbols := make([]string, 0, len(b.strategies))
	for _, strategy := range b.strategies {
		rawSymbols = append(rawSymbols, strategy.Symbol())
	}
	b.mu.RUnlock()

	symbols := normalizeSymbols(rawSymbols)
	if len(symbols) == 0 {
		log.Printf("websocket startup skipped: no strategy symbols")
		return
	}
	if b.client.priceCache == nil {
		b.client.AttachPriceCache(NewPriceCache())
	}
	// This call blocks until both streams exit, so Bot.wg really waits for all
	// websocket goroutines during Stop/Restart.
	b.client.RunStreams(ctx, symbols, b.handleTradeUpdate)
}

func (b *Bot) handleTradeUpdate(update TradeUpdateEnvelope) {
	data := update.Data
	event := strings.ToLower(strings.TrimSpace(data.Event))
	if event != "fill" && event != "partial_fill" {
		return
	}

	orderID := strings.TrimSpace(data.Order.ID)
	symbol := strings.ToUpper(strings.TrimSpace(data.Order.Symbol))
	if orderID == "" || symbol == "" {
		return
	}
	side := strings.ToLower(strings.TrimSpace(data.Order.Side))
	price := parseFloatString(data.Price)
	cumulativeAvg := parseFloatString(data.Order.FilledAvgPrice)
	if price <= 0 {
		price = cumulativeAvg
	}
	filledQty := parseFloatString(data.Order.FilledQty)
	eventQty := parseFloatString(data.Qty)

	b.mu.Lock()
	defer b.mu.Unlock()

	eventKey := fmt.Sprintf("%s|%s|%.9f|%.9f", orderID, data.Timestamp.UTC().Format(time.RFC3339Nano), eventQty, price)
	if _, duplicate := b.seenFillEvents[eventKey]; duplicate {
		return
	}
	b.seenFillEvents[eventKey] = struct{}{}

	previousFilled := b.seenFillQty[orderID]
	// Prefer the cumulative filled quantity difference. This makes duplicate or
	// replayed websocket events idempotent. eventQty is only a fallback for
	// payloads that omit cumulative filled_qty.
	delta := filledQty - previousFilled
	if filledQty <= 0 && eventQty > 0 {
		delta = eventQty
	}
	if delta <= 1e-9 || price <= 0 {
		return
	}
	if filledQty > previousFilled {
		b.seenFillQty[orderID] = filledQty
	} else {
		b.seenFillQty[orderID] = previousFilled + delta
	}
	if filledQty > 0 && cumulativeAvg > 0 {
		b.seenFillNotional[orderID] = filledQty * cumulativeAvg
	} else {
		b.seenFillNotional[orderID] += delta * price
	}

	fillTime := data.Timestamp
	if fillTime.IsZero() {
		fillTime = effectiveOrderTime(data.Order)
	}
	if fillTime.IsZero() {
		fillTime = time.Now().UTC()
	}

	strategyName := detectStrategyName(data.Order.ClientOrderID, symbol)
	record := TradeRecord{
		Time:          fillTime,
		Symbol:        symbol,
		Side:          side,
		Qty:           delta,
		Price:         price,
		OrderID:       orderID,
		ClientOrderID: data.Order.ClientOrderID,
		Strategy:      strategyName,
	}
	b.appendTradeRecordLocked(record)
	b.applyGlobalFill(symbol, side, delta, price)
	b.applyStrategyFill(strategyName, symbol, side, orderID, delta, price, fillTime)
	b.lastPrices[symbol] = price
	b.notifyStrategyFill(strategyName, orderID, side, delta, price, fillTime)
	b.recalcStrategyStatsLocked()
}

func (b *Bot) startReconcileLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := b.syncOrderFills(ctx); err != nil {
				b.logError("system", "order reconcile failed: "+err.Error())
			}
		}
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
		filledAt := ""
		if o.FilledAt != nil {
			filledAt = o.FilledAt.Format(time.RFC3339)
		}
		updatedAt := ""
		if o.UpdatedAt != nil {
			updatedAt = o.UpdatedAt.Format(time.RFC3339)
		}
		strategy := detectStrategyName(o.ClientOrderID, o.Symbol)
		out = append(out, OrderSummary{
			ID:             o.ID,
			ClientOrderID:  o.ClientOrderID,
			Symbol:         o.Symbol,
			Side:           o.Side,
			Type:           o.Type,
			Qty:            parseFloatString(o.Qty),
			FilledQty:      parseFloatString(o.FilledQty),
			LimitPrice:     parseFloatString(o.LimitPrice),
			FilledAvgPrice: parseFloatString(o.FilledAvgPrice),
			Status:         o.Status,
			CreatedAt:      createdAt,
			FilledAt:       filledAt,
			UpdatedAt:      updatedAt,
			Strategy:       strategy,
		})
	}
	return out, nil
}

func (b *Bot) LatestPrices() map[string]float64 {
	if b.client == nil || b.client.priceCache == nil {
		return map[string]float64{}
	}
	// Return only values that still satisfy the freshness policy. The previous
	// implementation mixed indefinitely stale lastPrices into this result, which
	// made the dashboard look live even when market data had stopped.
	return b.client.priceCache.Snapshot()
}

func (b *Bot) TotalAssets(ctx context.Context) (map[string]any, error) {
	acct, err := b.client.GetAccount(ctx)
	if err != nil {
		return nil, err
	}
	longMV := parseFloatString(acct.LongMarketValue)
	shortMV := parseFloatString(acct.ShortMarketValue)
	return map[string]any{
		"id":                          strings.TrimSpace(acct.ID),
		"status":                      strings.TrimSpace(acct.Status),
		"trading_mode":                b.client.TradingMode(),
		"equity":                      parseFloatString(acct.Equity),
		"cash":                        parseFloatString(acct.Cash),
		"buying_power":                parseFloatString(acct.BuyingPower),
		"non_marginable_buying_power": parseFloatString(acct.NonMarginableBuyingPower),
		"long_market_value":           longMV,
		"short_market_value":          shortMV,
		"maintenance_margin":          parseFloatString(acct.MaintenanceMargin),
		"trading_blocked":             acct.TradingBlocked,
		"account_blocked":             acct.AccountBlocked,
		"trade_suspended_by_user":     acct.TradeSuspendedByUser,
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

func aggregateTradeRecords(records []TradeRecord) []TradeRecord {
	type aggregateKey struct {
		OrderID  string
		Side     string
		Strategy string
	}
	out := make([]TradeRecord, 0, len(records))
	index := make(map[aggregateKey]int, len(records))
	for _, record := range records {
		if strings.TrimSpace(record.OrderID) == "" {
			out = append(out, record)
			continue
		}
		key := aggregateKey{OrderID: record.OrderID, Side: record.Side, Strategy: record.Strategy}
		if idx, exists := index[key]; exists {
			existing := &out[idx]
			totalQty := existing.Qty + record.Qty
			if totalQty > 0 {
				existing.Price = (existing.Price*existing.Qty + record.Price*record.Qty) / totalQty
			}
			existing.Qty = totalQty
			if record.Time.After(existing.Time) {
				existing.Time = record.Time
			}
			continue
		}
		index[key] = len(out)
		out = append(out, record)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Time.After(out[j].Time) })
	return out
}

func (b *Bot) Trades() []TradeRecord {
	b.mu.RLock()
	raw := make([]TradeRecord, len(b.tradeRecords))
	copy(raw, b.tradeRecords)
	b.mu.RUnlock()
	return aggregateTradeRecords(raw)
}

func (b *Bot) Performance(ctx context.Context) (PerformanceSummary, error) {
	acct, err := b.client.GetAccount(ctx)
	if err != nil {
		return PerformanceSummary{}, err
	}
	currentEquity := parseFloatString(acct.Equity)
	periodEquity := b.initialEquity
	totalPnL := currentEquity - periodEquity
	returnPct := 0.0
	if periodEquity > 0 {
		returnPct = totalPnL / periodEquity * 100
	}
	if period, historyErr := b.client.GetPortfolioHistory7D(ctx); historyErr == nil && period.StartEquity > 0 {
		periodEquity = period.StartEquity
		if period.HasPnL {
			totalPnL = period.TotalPnL
		} else {
			totalPnL = currentEquity - periodEquity
		}
		if period.HasReturn {
			returnPct = period.ReturnPct
		} else {
			returnPct = totalPnL / periodEquity * 100
		}
	}
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
		if math.Abs(qty) <= 0 || avg <= 0 || cur <= 0 {
			continue
		}
		if side == "short" {
			unrealized += math.Abs(qty) * (avg - cur)
		} else {
			unrealized += qty * (cur - avg)
		}
	}
	b.mu.RLock()
	realized := b.globalRealizedPnL
	b.mu.RUnlock()
	return PerformanceSummary{
		Period:        "7D",
		InitialEquity: periodEquity,
		CurrentEquity: currentEquity,
		RealizedPnL:   realized,
		UnrealizedPnL: unrealized,
		TotalPnL:      totalPnL,
		ReturnPct:     returnPct,
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
	strats := make([]string, 0, len(b.strategies))
	for k := range b.strategies {
		strats = append(strats, k)
	}
	startedAt := b.startAt
	raw := make([]TradeRecord, len(b.tradeRecords))
	copy(raw, b.tradeRecords)
	riskState := b.riskState
	riskConfig := b.riskConfig
	running := b.isRunning
	b.mu.RUnlock()
	sort.Strings(strats)
	return map[string]any{
		"started_at":   startedAt,
		"running":      running,
		"build":        processBuildVersion,
		"trading_mode": b.client.TradingMode(),
		"strategies":   strats,
		"trade_count":  len(aggregateTradeRecords(raw)),
		"risk":         riskState,
		"risk_config": map[string]any{
			"cash_only":               riskConfig.CashOnly,
			"max_daily_loss_pct":      riskConfig.MaxDailyLossPct,
			"max_drawdown_pct":        riskConfig.MaxDrawdownPct,
			"max_gross_exposure_pct":  riskConfig.MaxGrossExposurePct,
			"max_symbol_exposure_pct": riskConfig.MaxSymbolExposurePct,
			"liquidate_on_risk_halt":  riskConfig.LiquidateOnRiskHalt,
		},
	}
}

// -----------------------
// Safe Dashboard Print
// -----------------------

func (b *Bot) StartMonitor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
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
	rawTrades := make([]TradeRecord, len(bot.tradeRecords))
	copy(rawTrades, bot.tradeRecords)
	bot.mu.RUnlock()
	totalTrades := len(aggregateTradeRecords(rawTrades))
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
		// 修改了排版占位符以容纳更长的带有 Symbol 的策略名称
		fmt.Printf("  %-16s | 标的: %-5s | 持仓: %-6.2f | PnL: $%+.2f \n",
			stat.Name, stat.Symbol, stat.PositionQty, stat.TotalPnL)
	}
	if len(positions) > 0 {
		fmt.Printf("\n [ 实时仓位 ]\n")
		latestPrices := bot.LatestPrices()
		for _, p := range positions {
			symbol := strings.ToUpper(strings.TrimSpace(p.Symbol))
			// 默认使用持仓 REST 接口返回的价格
			currentPrice := parseFloatString(p.CurrentPrice)
			priceSource := "POSITION"
			// 优先显示 WebSocket 缓存价格
			if wsPrice, ok := latestPrices[symbol]; ok && wsPrice > 0 {
				currentPrice = wsPrice
				priceSource = "MARKET"
			}
			fmt.Printf("   %-5s | 数量: %-6s | 均价: $%-.2f | 现价: $%-.2f | 来源: %s\n",
				symbol,
				p.Qty,
				parseFloatString(p.AvgEntryPrice),
				currentPrice,
				priceSource)
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
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return nil, errors.New("symbol is required")
	}
	u, err := url.Parse(fmt.Sprintf("%s/v2/stocks/%s/bars", c.dataURL, url.PathEscape(symbol)))
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("timeframe", timeframe)
	if !start.IsZero() {
		q.Set("start", start.UTC().Format(time.RFC3339))
	}
	if !end.IsZero() {
		q.Set("end", end.UTC().Format(time.RFC3339))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if strings.TrimSpace(c.feed) != "" {
		q.Set("feed", strings.TrimSpace(c.feed))
	}
	u.RawQuery = q.Encode()
	var resp BarsResponse
	if err := c.doJSON(ctx, http.MethodGet, u.String(), nil, &resp); err != nil {
		return nil, err
	}
	sort.SliceStable(resp.Bars, func(i, j int) bool {
		return resp.Bars[i].Time < resp.Bars[j].Time
	})
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

func (b *Bot) prewarmReferencePrices(ctx context.Context) {
	b.mu.RLock()
	rawSymbols := make([]string, 0, len(b.strategies))
	for _, strategy := range b.strategies {
		rawSymbols = append(rawSymbols, strategy.Symbol())
	}
	b.mu.RUnlock()

	for _, symbol := range normalizeSymbols(rawSymbols) {
		price, err := b.client.GetReferencePrice(ctx, symbol)
		if err != nil {
			b.logError("price-"+symbol, "startup price prewarm failed: "+err.Error())
			continue
		}
		b.mu.Lock()
		b.lastPrices[symbol] = price
		b.mu.Unlock()
		log.Printf("startup reference price: symbol=%s price=%.6f", symbol, price)
	}
}

func (b *Bot) Start(ctx context.Context) error {
	log.Printf("process build version: %s", processBuildVersion)
	b.loadRiskState()
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

	// 先做一次 REST 回填，补上 WS 之前的成交和历史缺口
	if err := b.syncOrderFills(runCtx); err != nil {
		b.logError("system", "initial order backfill failed: "+err.Error())
	}

	if b.useWebSockets {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.startWebSockets(runCtx)
		}()
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.startReconcileLoop(runCtx, 5*time.Minute)
		}()
	}

	// Do not wait for the first WebSocket event. REST prewarming gives every
	// strategy a valid starting price before its first Tick.
	b.prewarmReferencePrices(runCtx)

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
	if b.stopFunc != nil {
		b.stopFunc()
		b.stopFunc = nil
	}
	b.isRunning = false
	b.mu.Unlock()
	b.client.CloseStreams()
}

func (b *Bot) IsRunning() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.isRunning
}

func (b *Bot) Restart(ctx context.Context) error {
	b.Stop()
	if !b.waitStopped(10 * time.Second) {
		return errors.New("bot restart aborted: previous workers did not stop within 10s")
	}
	return b.Start(ctx)
}

func (b *Bot) waitStopped(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()
	if timeout <= 0 {
		<-done
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func (b *Bot) LiquidateAll(ctx context.Context) error {
	log.Println("正在停止机器人策略轮询...")
	b.Stop()
	if !b.waitStopped(5 * time.Second) {
		log.Printf("workers did not fully stop within 5s; continuing emergency broker cancellation")
	}
	if _, err := b.client.CancelAllOrders(ctx); err != nil {
		log.Printf("emergency cancel-all warning: %v", err)
	}
	if _, err := b.client.CloseAllPositions(ctx); err != nil {
		log.Printf("emergency close-all warning: %v", err)
	}
	deadline := time.NewTimer(25 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	var lastPositionErr, lastOrderErr error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			positions, posErr := b.client.GetPositions(ctx)
			orders, orderErr := b.client.ListOrders(ctx, "open")
			if posErr != nil || orderErr != nil {
				return fmt.Errorf("liquidation verification failed: positions=%v orders=%v", posErr, orderErr)
			}
			positionSymbols := make([]string, 0, len(positions))
			for _, position := range positions {
				positionSymbols = append(positionSymbols, position.Symbol)
			}
			orderIDs := make([]string, 0, len(orders))
			for _, order := range orders {
				orderIDs = append(orderIDs, order.ID)
			}
			return fmt.Errorf("liquidation incomplete after 25s: positions=%v open_orders=%v last_errors=(%v, %v)", positionSymbols, orderIDs, lastPositionErr, lastOrderErr)
		case <-tick.C:
			positions, posErr := b.client.GetPositions(ctx)
			orders, orderErr := b.client.ListOrders(ctx, "open")
			lastPositionErr, lastOrderErr = posErr, orderErr
			if posErr == nil && orderErr == nil && len(positions) == 0 && len(orders) == 0 {
				log.Println("仓位和挂单均已归零，平仓完毕。")
				return nil
			}
			if orderErr == nil && len(orders) > 0 {
				_, _ = b.client.CancelAllOrders(ctx)
			}
			if posErr == nil && len(positions) > 0 {
				_, _ = b.client.CloseAllPositions(ctx)
			}
			log.Printf("waiting for liquidation: positions=%d open_orders=%d", len(positions), len(orders))
		}
	}
}

