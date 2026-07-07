package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"mytrading/logger"
	"mytrading/process"
)

var (
	globalBot *process.Bot
	botMu     sync.RWMutex
)

func setGlobalBot(bot *process.Bot) {
	botMu.Lock()
	defer botMu.Unlock()
	globalBot = bot
}

func getGlobalBot() *process.Bot {
	botMu.RLock()
	defer botMu.RUnlock()
	return globalBot
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode json failed: %v", err)
	}
}

type DashboardResponse struct {
	Account          AccountView           `json:"account"`
	Performance      PerformanceView       `json:"performance"`
	Positions        []PositionView        `json:"positions"`
	Strategies       []StrategyDetail      `json:"strategies"`
	Trades           []TradeView           `json:"trades"`
	TradesByStrategy map[string]TradeGroup `json:"trades_by_strategy"`
	Orders           []OrderView           `json:"orders"`
	OrdersByStrategy map[string]OrderGroup `json:"orders_by_strategy"`
	Snapshots        []SnapshotView        `json:"snapshots"`
	Errors           []ErrorView           `json:"errors"`
	StrategyConfigs  map[string]any        `json:"strategy_configs"`
	LatestPrices     map[string]float64    `json:"latest_prices"`
	Status           map[string]any        `json:"status"`
	UpdatedAt        string                `json:"updated_at"`
	Meta             map[string]any        `json:"meta"`
}

type AccountView struct {
	ID               string  `json:"id"`
	Status           string  `json:"status"`
	Equity           float64 `json:"equity"`
	Cash             float64 `json:"cash"`
	BuyingPower      float64 `json:"buying_power"`
	LongMarketValue  float64 `json:"long_market_value"`
	ShortMarketValue float64 `json:"short_market_value"`
}

type PerformanceView struct {
	InitialEquity float64 `json:"initial_equity"`
	CurrentEquity float64 `json:"current_equity"`
	RealizedPnL   float64 `json:"realized_pnl"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`
	TotalPnL      float64 `json:"total_pnl"`
	ReturnPct     float64 `json:"return_pct"`
}

type PositionView struct {
	Symbol        string  `json:"symbol"`
	Qty           float64 `json:"qty"`
	AvgEntryPrice float64 `json:"avg_entry_price"`
	MarketValue   float64 `json:"market_value"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`
	CurrentPrice  float64 `json:"current_price"`
	Side          string  `json:"side"`
}

type TradeView struct {
	ID            string  `json:"id"`
	Time          string  `json:"time"`
	Symbol        string  `json:"symbol"`
	Side          string  `json:"side"`
	Qty           float64 `json:"qty"`
	Price         float64 `json:"price"`
	OrderID       string  `json:"order_id"`
	ClientOrderID string  `json:"client_order_id"`
	Strategy      string  `json:"strategy"`
}

type OrderView struct {
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

type SnapshotView struct {
	Time   string  `json:"time"`
	Equity float64 `json:"equity"`
}

type ErrorView struct {
	Time     string `json:"time"`
	Strategy string `json:"strategy"`
	Error    string `json:"error"`
}

type TradeGroup struct {
	Count   int         `json:"count"`
	Symbols []string    `json:"symbols"`
	Trades  []TradeView `json:"trades"`
}

type OrderGroup struct {
	Count   int         `json:"count"`
	Symbols []string    `json:"symbols"`
	Orders  []OrderView `json:"orders"`
}

type StrategyDetail struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	Symbol        string         `json:"symbol"`
	Symbols       []string       `json:"symbols"`
	SymbolCount   int            `json:"symbol_count"`
	TradeCount    int            `json:"trade_count"`
	OrderCount    int            `json:"order_count"`
	PositionCount int            `json:"position_count"`
	RealizedPnL   float64        `json:"realized_pnl"`
	UnrealizedPnL float64        `json:"unrealized_pnl"`
	TotalPnL      float64        `json:"total_pnl"`
	ReturnPct     float64        `json:"return_pct"`
	PositionQty   float64        `json:"position_qty"`
	AvgCost       float64        `json:"avg_cost"`
	LastPrice     float64        `json:"last_price"`
	LatestPrice   float64        `json:"latest_price"`
	Config        any            `json:"config"`
	Trades        []TradeView    `json:"trades"`
	Orders        []OrderView    `json:"orders"`
	Positions     []PositionView `json:"positions"`
}

func GetDashboard(w http.ResponseWriter, r *http.Request) {
	logger.Info("enter GetDashboard")

	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	msk := r.Header.Get("X-MSK")
	if msk == "" {
		http.Error(w, "Missing MSK", http.StatusUnauthorized)
		return
	}
	if !globalCache.MskExists(msk) {
		http.Error(w, "MSK Expired", http.StatusUnauthorized)
		return
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	ctx := r.Context()
	bot := getGlobalBot()

	resp := DashboardResponse{
		Account:          AccountView{},
		Performance:      PerformanceView{},
		Positions:        []PositionView{},
		Strategies:       []StrategyDetail{},
		Trades:           []TradeView{},
		TradesByStrategy: map[string]TradeGroup{},
		Orders:           []OrderView{},
		OrdersByStrategy: map[string]OrderGroup{},
		Snapshots:        []SnapshotView{},
		Errors:           []ErrorView{},
		StrategyConfigs:  map[string]any{},
		LatestPrices:     map[string]float64{},
		Status:           map[string]any{},
		UpdatedAt:        time.Now().In(time.FixedZone("CST", 8*3600)).Format("2006/01/02 15:04:05"),
		Meta: map[string]any{
			"trade_limit": 300,
		},
	}

	if bot == nil {
		writeJSON(w, http.StatusOK, resp)
		return
	}

	botRunning := bot.IsRunning()

	assets, errAssets := bot.TotalAssets(ctx)
	if errAssets == nil {
		resp.Account = formatAccount(assets, botRunning)
	} else {
		resp.Meta["account_error"] = errAssets.Error()
	}

	perf, errPerf := bot.Performance(ctx)
	if errPerf == nil {
		resp.Performance = formatPerformance(perf)
	} else {
		resp.Meta["performance_error"] = errPerf.Error()
	}

	positions, errPos := bot.Positions(ctx)
	if errPos == nil {
		resp.Positions = formatPositions(positions)
	} else {
		resp.Meta["positions_error"] = errPos.Error()
	}

	stats, errStats := bot.StrategySummaries(ctx)
	if errStats != nil {
		resp.Meta["strategies_error"] = errStats.Error()
	}

	allTrades := bot.Trades()
	allOrders, errOrders := bot.AllOrders(ctx)
	if errOrders != nil {
		resp.Meta["orders_error"] = errOrders.Error()
	}

	latestTrades := latestTradeRecords(allTrades, 300)
	resp.Trades = formatTrades(latestTrades)
	resp.Meta["trade_count_total"] = len(allTrades)
	resp.Meta["trade_count_returned"] = len(latestTrades)

	resp.TradesByStrategy = formatTradesByStrategy(allTrades)
	if errOrders == nil {
		resp.OrdersByStrategy = formatOrdersByStrategy(allOrders)
		resp.Orders = formatOrders(allOrders)
	}

	resp.Snapshots = formatSnapshots(bot.AccountHistory())
	resp.Errors = formatErrors(bot.ErrorLog())

	cfg := bot.StrategyConfigs()
	if cfg != nil {
		resp.StrategyConfigs = cfg
	}

	resp.LatestPrices = formatLatestPrices(bot.LatestPrices())
	resp.Status = bot.Status()
	resp.UpdatedAt = time.Now().In(time.FixedZone("CST", 8*3600)).Format("2006/01/02 15:04:05")

	resp.Strategies = buildStrategyDetails(
		stats,
		allTrades,
		allOrders,
		resp.Positions,
		resp.StrategyConfigs,
		resp.LatestPrices,
	)

	resp.Meta["strategy_count"] = len(resp.Strategies)
	resp.Meta["order_count_total"] = len(resp.Orders)
	resp.Meta["snapshot_count"] = len(resp.Snapshots)
	resp.Meta["error_count"] = len(resp.Errors)

	writeJSON(w, http.StatusOK, resp)
}

func parseFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	case string:
		f, _ := json.Number(x).Float64()
		return f
	default:
		return 0
	}
}

func formatAccount(assets map[string]any, running bool) AccountView {
	status := "stopped"
	if running {
		status = "running"
	}
	return AccountView{
		ID:               "",
		Status:           status,
		Equity:           parseFloat(assets["equity"]),
		Cash:             parseFloat(assets["cash"]),
		BuyingPower:      parseFloat(assets["buying_power"]),
		LongMarketValue:  0,
		ShortMarketValue: 0,
	}
}

func formatPerformance(p process.PerformanceSummary) PerformanceView {
	return PerformanceView{
		InitialEquity: p.InitialEquity,
		CurrentEquity: p.CurrentEquity,
		RealizedPnL:   p.RealizedPnL,
		UnrealizedPnL: p.UnrealizedPnL,
		TotalPnL:      p.TotalPnL,
		ReturnPct:     p.ReturnPct,
	}
}

func formatPositions(positions []process.HoldingSummary) []PositionView {
	out := make([]PositionView, 0, len(positions))
	for _, h := range positions {
		out = append(out, PositionView{
			Symbol:        h.Symbol,
			Qty:           h.Qty,
			AvgEntryPrice: h.AvgEntryPrice,
			MarketValue:   h.MarketValue,
			UnrealizedPnL: h.UnrealizedPnL,
			CurrentPrice:  h.CurrentPrice,
			Side:          h.Side,
		})
	}
	return out
}

func formatTrade(t process.TradeRecord) TradeView {
	return TradeView{
		ID:            t.OrderID,
		Time:          t.Time.UTC().Format(time.RFC3339),
		Symbol:        t.Symbol,
		Side:          t.Side,
		Qty:           t.Qty,
		Price:         t.Price,
		OrderID:       t.OrderID,
		ClientOrderID: t.ClientOrderID,
		Strategy:      normalizeStrategyName(t.Strategy),
	}
}

func formatTrades(trades []process.TradeRecord) []TradeView {
	out := make([]TradeView, 0, len(trades))
	for _, t := range trades {
		out = append(out, formatTrade(t))
	}
	return out
}

func formatOrder(o process.OrderSummary) OrderView {
	return OrderView{
		ID:            o.ID,
		ClientOrderID: o.ClientOrderID,
		Symbol:        o.Symbol,
		Side:          o.Side,
		Type:          o.Type,
		Qty:           o.Qty,
		FilledQty:     o.FilledQty,
		LimitPrice:    o.LimitPrice,
		Status:        o.Status,
		CreatedAt:     o.CreatedAt,
		Strategy:      normalizeStrategyName(o.Strategy),
	}
}

func formatOrders(orders []process.OrderSummary) []OrderView {
	out := make([]OrderView, 0, len(orders))
	for _, o := range orders {
		out = append(out, formatOrder(o))
	}
	return out
}

func formatSnapshots(snaps []process.DailySnapshot) []SnapshotView {
	out := make([]SnapshotView, 0, len(snaps))
	for _, s := range snaps {
		out = append(out, SnapshotView{
			Time:   s.Time.UTC().Format(time.RFC3339),
			Equity: s.Equity,
		})
	}
	return out
}

func formatErrors(errs []process.ErrorRecord) []ErrorView {
	out := make([]ErrorView, 0, len(errs))
	for _, e := range errs {
		out = append(out, ErrorView{
			Time:     e.Time.UTC().Format(time.RFC3339),
			Strategy: e.Strategy,
			Error:    e.Error,
		})
	}
	return out
}

func formatLatestPrices(prices map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(prices))
	for k, v := range prices {
		out[k] = v
	}
	return out
}

func latestTradeRecords(trades []process.TradeRecord, limit int) []process.TradeRecord {
	if len(trades) == 0 {
		return []process.TradeRecord{}
	}

	cp := make([]process.TradeRecord, len(trades))
	copy(cp, trades)

	sort.Slice(cp, func(i, j int) bool {
		return cp[i].Time.After(cp[j].Time)
	})

	if limit > 0 && len(cp) > limit {
		cp = cp[:limit]
	}
	return cp
}

func normalizeStrategyName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "unknown"
	}
	return name
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func sortTradesDesc(trades []TradeView) {
	sort.Slice(trades, func(i, j int) bool {
		return trades[i].Time > trades[j].Time
	})
}

func sortOrdersDesc(orders []OrderView) {
	sort.Slice(orders, func(i, j int) bool {
		return orders[i].CreatedAt > orders[j].CreatedAt
	})
}

func formatTradesByStrategy(trades []process.TradeRecord) map[string]TradeGroup {
	type bucket struct {
		count   int
		symbols map[string]struct{}
		trades  []process.TradeRecord
	}

	grouped := make(map[string]*bucket)

	for _, t := range trades {
		key := normalizeStrategyName(t.Strategy)
		b := grouped[key]
		if b == nil {
			b = &bucket{
				symbols: make(map[string]struct{}),
				trades:  make([]process.TradeRecord, 0),
			}
			grouped[key] = b
		}
		b.count++
		b.symbols[t.Symbol] = struct{}{}
		b.trades = append(b.trades, t)
	}

	keys := make([]string, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make(map[string]TradeGroup, len(grouped))
	for _, k := range keys {
		b := grouped[k]
		sort.Slice(b.trades, func(i, j int) bool {
			return b.trades[i].Time.After(b.trades[j].Time)
		})

		views := make([]TradeView, 0, len(b.trades))
		for _, t := range b.trades {
			views = append(views, formatTrade(t))
		}

		symbols := make([]string, 0, len(b.symbols))
		for sym := range b.symbols {
			symbols = append(symbols, sym)
		}
		sort.Strings(symbols)

		out[k] = TradeGroup{
			Count:   b.count,
			Symbols: symbols,
			Trades:  views,
		}
	}

	return out
}

func formatOrdersByStrategy(orders []process.OrderSummary) map[string]OrderGroup {
	type bucket struct {
		count   int
		symbols map[string]struct{}
		orders  []process.OrderSummary
	}

	grouped := make(map[string]*bucket)

	for _, o := range orders {
		key := normalizeStrategyName(o.Strategy)
		b := grouped[key]
		if b == nil {
			b = &bucket{
				symbols: make(map[string]struct{}),
				orders:  make([]process.OrderSummary, 0),
			}
			grouped[key] = b
		}
		b.count++
		b.symbols[o.Symbol] = struct{}{}
		b.orders = append(b.orders, o)
	}

	keys := make([]string, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make(map[string]OrderGroup, len(grouped))
	for _, k := range keys {
		b := grouped[k]
		sort.Slice(b.orders, func(i, j int) bool {
			return b.orders[i].CreatedAt > b.orders[j].CreatedAt
		})

		views := make([]OrderView, 0, len(b.orders))
		for _, o := range b.orders {
			views = append(views, formatOrder(o))
		}

		symbols := make([]string, 0, len(b.symbols))
		for sym := range b.symbols {
			symbols = append(symbols, sym)
		}
		sort.Strings(symbols)

		out[k] = OrderGroup{
			Count:   b.count,
			Symbols: symbols,
			Orders:  views,
		}
	}

	return out
}

func filterPositionsBySymbols(positions []PositionView, symbols []string) []PositionView {
	if len(symbols) == 0 {
		return []PositionView{}
	}
	set := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		set[strings.ToUpper(strings.TrimSpace(s))] = struct{}{}
	}

	out := make([]PositionView, 0)
	for _, p := range positions {
		if _, ok := set[strings.ToUpper(strings.TrimSpace(p.Symbol))]; ok {
			out = append(out, p)
		}
	}
	return out
}

func buildStrategyDetails(
	stats []process.StrategyStats,
	allTrades []process.TradeRecord,
	allOrders []process.OrderSummary,
	allPositions []PositionView,
	configs map[string]any,
	latestPrices map[string]float64,
) []StrategyDetail {
	tradeGroups := formatTradesByStrategy(allTrades)
	orderGroups := formatOrdersByStrategy(allOrders)

	out := make([]StrategyDetail, 0, len(stats))
	for _, s := range stats {
		name := normalizeStrategyName(s.Name)
		tg := tradeGroups[name]
		og := orderGroups[name]

		symbols := []string{s.Symbol}
		symbols = append(symbols, tg.Symbols...)
		symbols = append(symbols, og.Symbols...)
		symbols = uniqueStrings(symbols)
		sort.Strings(symbols)

		positions := filterPositionsBySymbols(allPositions, symbols)

		var config any
		if configs != nil {
			if v, ok := configs[name]; ok {
				config = v
			}
		}

		out = append(out, StrategyDetail{
			ID:            name,
			Name:          name,
			Symbol:        s.Symbol,
			Symbols:       symbols,
			SymbolCount:   len(symbols),
			TradeCount:    s.TradeCount,
			OrderCount:    og.Count,
			PositionCount: len(positions),
			RealizedPnL:   s.RealizedPnL,
			UnrealizedPnL: s.UnrealizedPnL,
			TotalPnL:      s.TotalPnL,
			ReturnPct:     s.ReturnPct,
			PositionQty:   s.PositionQty,
			AvgCost:       s.AvgCost,
			LastPrice:     s.LastPrice,
			LatestPrice:   latestPrices[s.Symbol],
			Config:        config,
			Trades:        tg.Trades,
			Orders:        og.Orders,
			Positions:     positions,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})

	return out
}

func defaultGridConfig(symbol string, qtyPerOrder, maxQty float64) process.GridConfig {
	return process.GridConfig{
		Symbol:                symbol,
		Levels:                6,
		SpacingPct:            0.02,
		MinSpacingPct:         0.01,
		MaxSpacingPct:         0.06,
		QtyPerOrder:           qtyPerOrder,
		SeedQty:               qtyPerOrder,
		RecenterPct:           0.05,
		MaxPositionQty:        maxQty,
		UseTrendFilter:        true,
		ATRPeriod:             14,
		ATRMultiplier:         1.2,
		CenterMode:            "ema",
		CenterEMAPeriod:       34,
		CenterVWAPLookback:    48,
		ADXPeriod:             14,
		ADXTrendThreshold:     25,
		ADXRangeThreshold:     18,
		DailyBuyNotionalLimit: 1000,
		BuyCooldown:           10 * time.Minute,
		RebuildCooldown:       20 * time.Minute,
	}
}

// StartTrade 初始化并启动交易机器人，赋值给全局 globalBot。
func StartTrade() {
	cfg := process.LoadConfig()

	client := process.NewAlpacaClient(cfg)
	bot := process.NewBot(client, cfg.Interval)

	bot.RegisterStrategy(process.NewGridStrategy(client, defaultGridConfig("LEU", 5, 25)))
	bot.RegisterStrategy(process.NewGridStrategy(client, defaultGridConfig("MOD", 5, 25)))
	bot.RegisterStrategy(process.NewGridStrategy(client, defaultGridConfig("IONQ", 5, 25)))

	ctx := context.Background()
	if err := bot.Start(ctx); err != nil {
		log.Fatal(err)
	}

	bot.StartMonitor(ctx, time.Minute)
	setGlobalBot(bot)

	select {}
}

// BarsQuery 用于接收查询参数
type BarsQuery struct {
	Symbol string `json:"symbol"`
}

// BarsResponseView 返回给前端的K线结构
type BarView struct {
	Time   float64 `json:"t"`
	Open   float64 `json:"o"`
	High   float64 `json:"h"`
	Low    float64 `json:"l"`
	Close  float64 `json:"c"`
	Volume float64 `json:"v"`
}

func GetBarsHandler(w http.ResponseWriter, r *http.Request) {
	msk := r.Header.Get("X-MSK")

	if msk == "" {
		http.Error(w, "Missing MSK", http.StatusUnauthorized)
		return
	} else if !globalCache.MskExists(msk) {
		http.Error(w, "MSK Expired", http.StatusUnauthorized)
		return
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	symbol := r.URL.Query().Get("symbol")
	if symbol == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing symbol"})
		return
	}

	bot := getGlobalBot()
	if bot == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "bot not running"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	bars, err := bot.GetHistoricalBars(ctx, symbol)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("fetch bars for %s: %v", symbol, err),
		})
		return
	}

	writeJSON(w, http.StatusOK, bars)
}

func LiquidateHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	msk := r.Header.Get("X-MSK")

	if msk == "" {
		http.Error(w, "Missing MSK", http.StatusUnauthorized)
		return
	} else if !globalCache.MskExists(msk) {
		http.Error(w, "MSK Expired", http.StatusUnauthorized)
		return
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	bot := getGlobalBot()
	if bot == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "bot not running"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := bot.LiquidateAll(ctx); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "liquidated"})
}

func RestartHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	msk := r.Header.Get("X-MSK")

	if msk == "" {
		http.Error(w, "Missing MSK", http.StatusUnauthorized)
		return
	} else if !globalCache.MskExists(msk) {
		http.Error(w, "MSK Expired", http.StatusUnauthorized)
		return
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	bot := getGlobalBot()
	if bot == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "bot not initialized"})
		return
	}

	ctx := context.Background()

	if err := bot.Restart(ctx); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "restarted"})
}
