package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
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
	ID                       string  `json:"id"`
	Status                   string  `json:"status"`
	BrokerStatus             string  `json:"broker_status"`
	TradingMode              string  `json:"trading_mode"`
	Equity                   float64 `json:"equity"`
	ComputedEquity           float64 `json:"computed_equity"`
	Cash                     float64 `json:"cash"`
	BuyingPower              float64 `json:"buying_power"`
	NonMarginableBuyingPower float64 `json:"non_marginable_buying_power"`
	LongMarketValue          float64 `json:"long_market_value"`
	ShortMarketValue         float64 `json:"short_market_value"`
	MaintenanceMargin        float64 `json:"maintenance_margin"`
	TradingBlocked           bool    `json:"trading_blocked"`
	AccountBlocked           bool    `json:"account_blocked"`
	TradeSuspendedByUser     bool    `json:"trade_suspended_by_user"`
}

type PerformanceView struct {
	Period        string  `json:"period"`
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
	ID              string         `json:"id"`
	Name            string         `json:"name"`
	Symbol          string         `json:"symbol"`
	Symbols         []string       `json:"symbols"`
	SymbolCount     int            `json:"symbol_count"`
	TradeCount      int            `json:"trade_count"`
	OrderCount      int            `json:"order_count"`
	PositionCount   int            `json:"position_count"`
	RealizedPnL     float64        `json:"realized_pnl"`
	UnrealizedPnL   float64        `json:"unrealized_pnl"`
	TotalPnL        float64        `json:"total_pnl"`
	ReturnPct       float64        `json:"return_pct"`
	InvestedCapital float64        `json:"invested_capital"`
	PositionQty     float64        `json:"position_qty"`
	AvgCost         float64        `json:"avg_cost"`
	LastPrice       float64        `json:"last_price"`
	LatestPrice     float64        `json:"latest_price"`
	Config          any            `json:"config"`
	Trades          []TradeView    `json:"trades"`
	Orders          []OrderView    `json:"orders"`
	Positions       []PositionView `json:"positions"`
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
			"trade_limit":              300,
			"performance_period":       "7D",
			"return_pct_unit":          "percentage_points",
			"realized_pnl_is_estimate": true,
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
		resp.Meta["account_equity_reported"] = parseFloat(assets["equity"])
		resp.Meta["account_equity_computed"] = resp.Account.ComputedEquity
		if rawStatus, ok := assets["status"]; ok {
			resp.Meta["broker_account_status"] = rawStatus
		}
		if id, ok := assets["id"]; ok {
			resp.Meta["account_id"] = id
		}
	} else {
		resp.Meta["account_error"] = errAssets.Error()
	}

	perf, errPerf := bot.Performance(ctx)
	if errPerf == nil {
		resp.Performance = formatPerformance(perf)
	} else {
		resp.Meta["performance_error"] = errPerf.Error()
	}

	if (resp.Account.Equity <= 0 || math.Abs(resp.Account.Equity) < 1e-6) && math.Abs(resp.Account.ComputedEquity) > 1e-6 {
		resp.Account.Equity = resp.Account.ComputedEquity
	}
	if (resp.Account.Equity <= 0 || math.Abs(resp.Account.Equity) < 1e-6) && resp.Performance.CurrentEquity > 0 {
		resp.Account.Equity = resp.Performance.CurrentEquity
	}
	if math.Abs(resp.Account.Equity) < 1e-6 {
		resp.Account.Equity = 0
	}
	resp.Meta["account_equity_final"] = resp.Account.Equity

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

func parseBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		parsed, _ := strconv.ParseBool(strings.TrimSpace(x))
		return parsed
	default:
		return false
	}
}

func formatAccount(assets map[string]any, running bool) AccountView {
	status := "stopped"
	if running {
		status = "running"
	}

	equity := parseFloat(assets["equity"])
	cash := parseFloat(assets["cash"])
	buyingPower := parseFloat(assets["buying_power"])
	nonMarginableBuyingPower := parseFloat(assets["non_marginable_buying_power"])
	longMarketValue := parseFloat(assets["long_market_value"])
	shortMarketValueRaw := parseFloat(assets["short_market_value"])
	maintenanceMargin := parseFloat(assets["maintenance_margin"])

	brokerStatus := ""
	if v, ok := assets["status"]; ok {
		brokerStatus = strings.TrimSpace(fmt.Sprintf("%v", v))
	}

	accountID := ""
	if v, ok := assets["id"]; ok {
		accountID = strings.TrimSpace(fmt.Sprintf("%v", v))
	}

	tradingMode := "paper"
	if v, ok := assets["trading_mode"]; ok {
		if parsed := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", v))); parsed == "live" || parsed == "paper" {
			tradingMode = parsed
		}
	}

	signedShort := shortMarketValueRaw
	if signedShort > 0 {
		signedShort = -signedShort
	}

	if math.Abs(longMarketValue) < 1e-6 {
		longMarketValue = 0
	}
	if math.Abs(signedShort) < 1e-6 {
		signedShort = 0
	}
	if math.Abs(cash) < 1e-6 {
		cash = 0
	}
	if math.Abs(buyingPower) < 1e-6 {
		buyingPower = 0
	}

	computedEquity := cash + longMarketValue + signedShort
	if math.Abs(computedEquity) < 1e-6 {
		computedEquity = 0
	}

	if math.Abs(equity) < 1e-6 {
		equity = 0
	}

	if (equity <= 0 || math.Abs(equity) < 1e-6) && computedEquity > 0 {
		equity = computedEquity
	}

	return AccountView{
		ID:                       accountID,
		Status:                   status,
		BrokerStatus:             brokerStatus,
		TradingMode:              tradingMode,
		Equity:                   equity,
		ComputedEquity:           computedEquity,
		Cash:                     cash,
		BuyingPower:              buyingPower,
		NonMarginableBuyingPower: nonMarginableBuyingPower,
		LongMarketValue:          longMarketValue,
		ShortMarketValue:         signedShort,
		MaintenanceMargin:        maintenanceMargin,
		TradingBlocked:           parseBool(assets["trading_blocked"]),
		AccountBlocked:           parseBool(assets["account_blocked"]),
		TradeSuspendedByUser:     parseBool(assets["trade_suspended_by_user"]),
	}
}

func formatPerformance(p process.PerformanceSummary) PerformanceView {
	return PerformanceView{
		Period:        p.Period,
		InitialEquity: p.InitialEquity,
		CurrentEquity: p.CurrentEquity,
		RealizedPnL:   p.RealizedPnL,
		UnrealizedPnL: p.UnrealizedPnL,
		TotalPnL:      p.TotalPnL,
		ReturnPct:     p.ReturnPct,
	}
}

func sanitizePositionSide(h process.HoldingSummary) string {
	side := strings.ToLower(strings.TrimSpace(h.Side))
	if side == "short" || side == "long" {
		return side
	}

	if h.Qty < 0 || h.MarketValue < 0 {
		return "short"
	}
	if h.Qty > 0 || h.MarketValue > 0 {
		return "long"
	}
	return "long"
}

func formatPositions(positions []process.HoldingSummary) []PositionView {
	out := make([]PositionView, 0, len(positions))
	for _, h := range positions {
		side := sanitizePositionSide(h)
		symbol := strings.ToUpper(strings.TrimSpace(h.Symbol))
		out = append(out, PositionView{
			Symbol:        symbol,
			Qty:           h.Qty,
			AvgEntryPrice: h.AvgEntryPrice,
			MarketValue:   h.MarketValue,
			UnrealizedPnL: h.UnrealizedPnL,
			CurrentPrice:  h.CurrentPrice,
			Side:          side,
		})
	}
	return out
}

func formatTrade(t process.TradeRecord) TradeView {
	side := strings.ToLower(strings.TrimSpace(t.Side))
	symbol := strings.ToUpper(strings.TrimSpace(t.Symbol))
	orderID := strings.TrimSpace(t.OrderID)
	clientOrderID := strings.TrimSpace(t.ClientOrderID)
	return TradeView{
		ID:            orderID,
		Time:          t.Time.UTC().Format(time.RFC3339),
		Symbol:        symbol,
		Side:          side,
		Qty:           t.Qty,
		Price:         t.Price,
		OrderID:       orderID,
		ClientOrderID: clientOrderID,
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
	side := strings.ToLower(strings.TrimSpace(o.Side))
	typ := strings.ToLower(strings.TrimSpace(o.Type))
	status := strings.ToLower(strings.TrimSpace(o.Status))
	symbol := strings.ToUpper(strings.TrimSpace(o.Symbol))
	return OrderView{
		ID:             strings.TrimSpace(o.ID),
		ClientOrderID:  strings.TrimSpace(o.ClientOrderID),
		Symbol:         symbol,
		Side:           side,
		Type:           typ,
		Qty:            o.Qty,
		FilledQty:      o.FilledQty,
		LimitPrice:     o.LimitPrice,
		FilledAvgPrice: o.FilledAvgPrice,
		Status:         status,
		CreatedAt:      strings.TrimSpace(o.CreatedAt),
		FilledAt:       strings.TrimSpace(o.FilledAt),
		UpdatedAt:      strings.TrimSpace(o.UpdatedAt),
		Strategy:       normalizeStrategyName(o.Strategy),
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

func strategyKey(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "unknown"
	}
	return strings.ToLower(name)
}

func groupHasSymbol(symbols []string, target string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return false
	}
	for _, sym := range symbols {
		if strings.EqualFold(strings.TrimSpace(sym), target) {
			return true
		}
	}
	return false
}

func pickTradeGroup(groups map[string]TradeGroup, key string, aliases ...string) TradeGroup {
	if g, ok := groups[key]; ok && (g.Count > 0 || len(g.Trades) > 0) {
		return g
	}
	for _, alias := range aliases {
		aliasKey := strategyKey(alias)
		if g, ok := groups[aliasKey]; ok && (g.Count > 0 || len(g.Trades) > 0) {
			return g
		}
	}
	for _, alias := range aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		for _, g := range groups {
			if groupHasSymbol(g.Symbols, alias) {
				return g
			}
		}
	}
	return TradeGroup{}
}

func pickOrderGroup(groups map[string]OrderGroup, key string, aliases ...string) OrderGroup {
	if g, ok := groups[key]; ok && (g.Count > 0 || len(g.Orders) > 0) {
		return g
	}
	for _, alias := range aliases {
		aliasKey := strategyKey(alias)
		if g, ok := groups[aliasKey]; ok && (g.Count > 0 || len(g.Orders) > 0) {
			return g
		}
	}
	for _, alias := range aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		for _, g := range groups {
			if groupHasSymbol(g.Symbols, alias) {
				return g
			}
		}
	}
	return OrderGroup{}
}

func pickConfig(configs map[string]any, keys ...string) any {
	if configs == nil || len(configs) == 0 {
		return nil
	}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if v, ok := configs[key]; ok {
			return v
		}
		lower := strings.ToLower(key)
		if v, ok := configs[lower]; ok {
			return v
		}
		upper := strings.ToUpper(key)
		if v, ok := configs[upper]; ok {
			return v
		}
	}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		for existKey, v := range configs {
			if strings.EqualFold(existKey, key) {
				return v
			}
		}
	}
	return nil
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
		key := strategyKey(t.Strategy)
		b := grouped[key]
		if b == nil {
			b = &bucket{
				symbols: make(map[string]struct{}),
				trades:  make([]process.TradeRecord, 0),
			}
			grouped[key] = b
		}
		b.count++
		b.symbols[strings.ToUpper(strings.TrimSpace(t.Symbol))] = struct{}{}
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
		key := strategyKey(o.Strategy)
		b := grouped[key]
		if b == nil {
			b = &bucket{
				symbols: make(map[string]struct{}),
				orders:  make([]process.OrderSummary, 0),
			}
			grouped[key] = b
		}
		b.count++
		b.symbols[strings.ToUpper(strings.TrimSpace(o.Symbol))] = struct{}{}
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

func mergeOrdersForStrategy(existing []OrderView, allOrders []process.OrderSummary, symbols []string, identifiers ...string) []OrderView {
	if len(existing) == 0 && len(allOrders) == 0 {
		return []OrderView{}
	}

	merged := make(map[string]OrderView, len(existing))
	for _, o := range existing {
		if o.ID == "" {
			continue
		}
		merged[o.ID] = o
	}

	symbolSet := make(map[string]struct{}, len(symbols))
	for _, sym := range symbols {
		upper := strings.ToUpper(strings.TrimSpace(sym))
		if upper == "" {
			continue
		}
		symbolSet[upper] = struct{}{}
	}

	identifierSet := make(map[string]struct{}, len(identifiers))
	for _, id := range identifiers {
		key := strategyKey(id)
		if key == "" {
			continue
		}
		identifierSet[key] = struct{}{}
	}

	for _, summary := range allOrders {
		strategyKeyLower := strategyKey(summary.Strategy)
		symbolUpper := strings.ToUpper(strings.TrimSpace(summary.Symbol))

		_, strategyMatch := identifierSet[strategyKeyLower]
		_, symbolMatch := symbolSet[symbolUpper]

		if !strategyMatch && !symbolMatch {
			continue
		}

		view := formatOrder(summary)
		if view.ID == "" {
			continue
		}
		merged[view.ID] = view
	}

	out := make([]OrderView, 0, len(merged))
	for _, v := range merged {
		out = append(out, v)
	}
	sortOrdersDesc(out)
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
		displayName := normalizeStrategyName(s.Name)
		key := strategyKey(s.Name)

		symbolOriginal := strings.TrimSpace(s.Symbol)
		symbolUpper := strings.ToUpper(symbolOriginal)

		aliases := []string{symbolOriginal, symbolUpper, displayName}
		tradeGroup := pickTradeGroup(tradeGroups, key, aliases...)
		orderGroup := pickOrderGroup(orderGroups, key, aliases...)

		combinedSymbols := make([]string, 0, 4)
		if symbolUpper != "" {
			combinedSymbols = append(combinedSymbols, symbolUpper)
		}
		for _, sym := range tradeGroup.Symbols {
			sym = strings.ToUpper(strings.TrimSpace(sym))
			if sym != "" {
				combinedSymbols = append(combinedSymbols, sym)
			}
		}
		for _, sym := range orderGroup.Symbols {
			sym = strings.ToUpper(strings.TrimSpace(sym))
			if sym != "" {
				combinedSymbols = append(combinedSymbols, sym)
			}
		}
		combinedSymbols = uniqueStrings(combinedSymbols)
		sort.Strings(combinedSymbols)

		positions := filterPositionsBySymbols(allPositions, combinedSymbols)
		if positions == nil {
			positions = []PositionView{}
		}

		latestPrice := 0.0
		if price, ok := latestPrices[symbolUpper]; ok {
			latestPrice = price
		} else if price, ok := latestPrices[symbolOriginal]; ok {
			latestPrice = price
		} else {
			for _, sym := range combinedSymbols {
				if price, ok := latestPrices[sym]; ok {
					latestPrice = price
					break
				}
			}
		}

		config := pickConfig(configs, key, displayName, symbolOriginal, symbolUpper)

		trades := tradeGroup.Trades
		if trades == nil {
			trades = []TradeView{}
		}

		orders := mergeOrdersForStrategy(orderGroup.Orders, allOrders, combinedSymbols, key, displayName, symbolOriginal, symbolUpper)
		if orders == nil {
			orders = []OrderView{}
		}

		tradeCount := s.TradeCount
		if tradeGroup.Count > tradeCount {
			tradeCount = tradeGroup.Count
		}
		if len(trades) > tradeCount {
			tradeCount = len(trades)
		}

		orderCount := orderGroup.Count
		if len(orders) > orderCount {
			orderCount = len(orders)
		}

		out = append(out, StrategyDetail{
			ID:              displayName,
			Name:            displayName,
			Symbol:          symbolUpper,
			Symbols:         combinedSymbols,
			SymbolCount:     len(combinedSymbols),
			TradeCount:      tradeCount,
			OrderCount:      orderCount,
			PositionCount:   len(positions),
			RealizedPnL:     s.RealizedPnL,
			UnrealizedPnL:   s.UnrealizedPnL,
			TotalPnL:        s.TotalPnL,
			ReturnPct:       s.ReturnPct,
			InvestedCapital: s.InvestedCapital,
			PositionQty:     s.PositionQty,
			AvgCost:         s.AvgCost,
			LastPrice:       s.LastPrice,
			LatestPrice:     latestPrice,
			Config:          config,
			Trades:          trades,
			Orders:          orders,
			Positions:       positions,
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
		OrderNotional:         250,
		MaxPositionNotional:   0,
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
		EntryFilterMode:       "soft",
		MACDFastPeriod:        12,
		MACDSlowPeriod:        26,
		MACDSignalPeriod:      9,
		MACDBearishPct:        0.001,
		MinBearishBuyLevel:    3,
		DailyBuyNotionalLimit: 1500,
		BuyCooldown:           2 * time.Minute,
		RebuildCooldown:       20 * time.Minute,
		MaxOpenBuyOrders:      3,
	}
}

// StartTrade 初始化并启动交易机器人，赋值给全局 globalBot。
func StartTrade() {
	cfg := process.LoadConfig()

	client := process.NewAlpacaClient(cfg)
	bot := process.NewBot(client, cfg.Interval)

	//bot.RegisterStrategy(process.NewGridStrategy(client, defaultGridConfig("CW", 1, 0)))
	bot.RegisterStrategy(process.NewGridStrategy(client, defaultGridConfig("MOD", 5, 0)))
	bot.RegisterStrategy(process.NewGridStrategy(client, defaultGridConfig("FLNC", 50, 0)))

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

	// Once an authenticated emergency liquidation is accepted, keep it running
	// even if the browser disconnects. The broker-side verification has its own
	// bounded timeout and must not be aborted by a transient client connection.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
