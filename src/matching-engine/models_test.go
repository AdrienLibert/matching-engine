package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var folderPath string = "../../schemas/"

type Contract struct {
	Type       string                 `json:"type"`
	Version    string                 `json:"version"`
	Metadata   map[string]interface{} `json:"metadata"`
	Properties map[string]interface{} `json:"properties"`
	Required   []string               `json:"required"`
}

// LoadContracts reads all JSON schema files from the contracts directory
func LoadContracts(folder string) (map[string]Contract, error) {
	contracts := make(map[string]Contract)
	err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".json" {
			file, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}
			var contract Contract
			if err := json.Unmarshal(file, &contract); err != nil {
				return err
			}
			contracts[info.Name()] = contract
		}
		return nil
	})
	return contracts, err
}

func matchType(jsonType, goType string) bool {
	switch jsonType {
	case "integer":
		return goType == "int64"
	case "string":
		return goType == "string"
	case "boolean":
		return goType == "bool"
	case "array":
		return goType == "slice"
	case "object":
		return goType == "struct"
	case "number":
		return goType == "float64"
	default:
		return false
	}
}

func ValidateStructAgainstContract(schema Contract, obj interface{}) bool {
	objType := reflect.TypeOf(obj)
	//objValue := reflect.ValueOf(obj)

	for jsonFieldName, fieldSchema := range schema.Properties {
		var field reflect.StructField
		found := false
		for i := 0; i < objType.NumField(); i++ {
			if objType.Field(i).Tag.Get("json") == jsonFieldName {
				field = objType.Field(i)
				found = true
				break
			}
		}
		if !found {
			return false
		}
		// Simple type check (expand for complex types)
		jsonType := fieldSchema.(map[string]interface{})["type"].(string)
		goType := field.Type.Kind().String()
		if !matchType(jsonType, goType) {
			return false
		}
	}
	return true
}

func TestOrderAgainstContract(t *testing.T) {
	contractName := "order.json"
	contracts, err := LoadContracts(folderPath)
	if err != nil {
		t.Fatalf("Failed to load contracts: %v", err)
	}

	orderContract, ok := contracts[contractName]
	if !ok {
		t.Errorf("Contr load {%s}", contractName)
	}

	t.Run(contractName, func(t *testing.T) {
		valid := ValidateStructAgainstContract(orderContract, Order{})
		if !valid {
			t.Errorf("Struct does not match contract: %s", contractName)
		}
	})
}

func TestTradeAgainstContract(t *testing.T) {
	contractName := "trade.json"
	contracts, err := LoadContracts(folderPath)
	if err != nil {
		t.Fatalf("Failed to load contracts: %v", err)
	}
	orderContract, ok := contracts[contractName]
	if !ok {
		t.Errorf("Contr load {%s}", contractName)
	}

	t.Run(contractName, func(t *testing.T) {
		valid := ValidateStructAgainstContract(orderContract, Trade{})
		if !valid {
			t.Errorf("Struct does not match contract: %s", contractName)
		}
	})

}
func TestPricePointAgainstContract(t *testing.T) {
	contractName := "pricepoint.json"
	contracts, err := LoadContracts(folderPath)
	if err != nil {
		t.Fatalf("Failed to load contracts: %v", err)
	}
	orderContract, ok := contracts[contractName]
	if !ok {
		t.Errorf("Contr load {%s}", contractName)
	}

	t.Run(contractName, func(t *testing.T) {
		valid := ValidateStructAgainstContract(orderContract, PricePoint{})
		if !valid {
			t.Errorf("Struct does not match contract: %s", contractName)
		}
	})

}

func TestMessageToOrderValid(t *testing.T) {
	message := []byte(`{"order_id":"uuid-1","order_type":"limit","price":10.5,"quantity":7,"action":"BUY","timestamp":1700000000}`)
	order, err := messageToOrder(message)

	if err != nil {
		t.Fatalf("expected valid order message, got error: %v", err)
	}

	if order.OrderID != "uuid-1" || order.OrderType != "limit" || order.Price != 10.5 || order.Quantity != 7 || order.Timestamp != 1700000000 {
		t.Fatalf("unexpected parsed order: %+v", order)
	}
	if order.Action != "BUY" {
		t.Fatalf("expected BUY action, got %+v", order.Action)
	}
}

func TestMessageToOrderRejectsNegativeQuantity(t *testing.T) {
	message := []byte(`{"order_id":"uuid-3","order_type":"limit","price":11.0,"quantity":-5,"action":"SELL","timestamp":1700000002}`)
	_, err := messageToOrder(message)

	if err == nil {
		t.Fatalf("expected reject for negative quantity")
	}
}

func TestMessageToOrderRejectsMissingAction(t *testing.T) {
	message := []byte(`{"order_id":"uuid-5","order_type":"limit","price":12.0,"quantity":2,"timestamp":1700000004}`)
	_, err := messageToOrder(message)

	if err == nil {
		t.Fatalf("expected reject for missing action")
	}
}

func TestMessageToOrderRejectsZeroQuantity(t *testing.T) {
	message := []byte(`{"order_id":"uuid-4","order_type":"limit","price":11.0,"quantity":0,"action":"BUY","timestamp":1700000003}`)
	_, err := messageToOrder(message)

	if err == nil {
		t.Fatalf("expected reject for zero quantity")
	}
}

func TestMessageToOrderInvalidJSON(t *testing.T) {
	message := []byte(`{"order_id":"uuid-1","order_type":`)
	_, err := messageToOrder(message)

	if err == nil {
		t.Fatalf("expected parsing error for malformed message")
	}
}

func TestTradeToJSONRoundTrip(t *testing.T) {
	original := Trade{
		TradeId:   "trade-1",
		OrderId:   "order-1",
		Quantity:  3,
		Price:     11.25,
		Action:    "BUY",
		Status:    "partial",
		Timestamp: 1700000001,
	}

	message := original.toJSON()

	var decoded Trade
	if err := json.Unmarshal(message, &decoded); err != nil {
		t.Fatalf("expected valid trade JSON, got error: %v", err)
	}

	if !reflect.DeepEqual(original, decoded) {
		t.Fatalf("decoded trade differs: got %+v want %+v", decoded, original)
	}
}

func TestPricePointToJSONRoundTrip(t *testing.T) {
	original := PricePoint{Price: 12.75}
	message := original.toJSON()

	var decoded PricePoint
	if err := json.Unmarshal(message, &decoded); err != nil {
		t.Fatalf("expected valid price point JSON, got error: %v", err)
	}

	if !reflect.DeepEqual(original, decoded) {
		t.Fatalf("decoded pricepoint differs: got %+v want %+v", decoded, original)
	}
}

func TestCreateTradeStatusClosed(t *testing.T) {
	order := &Order{OrderID: "order-closed", Quantity: 0}
	trade := createTrade("trade-closed", order, 5, 10.0, "BUY", 1700000002)

	if trade.Status != "closed" {
		t.Fatalf("expected closed status, got %s", trade.Status)
	}
	if trade.TradeId != "trade-closed" || trade.OrderId != "order-closed" || trade.Quantity != 5 || trade.Price != 10.0 || trade.Action != "BUY" || trade.Timestamp != 1700000002 {
		t.Fatalf("unexpected trade payload: %+v", trade)
	}
}

func TestCreateTradeStatusPartial(t *testing.T) {
	order := &Order{OrderID: "order-partial", Quantity: 2}
	trade := createTrade("trade-partial", order, 3, 11.0, "SELL", 1700000003)

	if trade.Status != "partial" {
		t.Fatalf("expected partial status, got %s", trade.Status)
	}
	if trade.TradeId != "trade-partial" || trade.OrderId != "order-partial" || trade.Quantity != 3 || trade.Price != 11.0 || trade.Action != "SELL" || trade.Timestamp != 1700000003 {
		t.Fatalf("unexpected trade payload: %+v", trade)
	}
}

func TestLoadContractsFailFastMissingFolder(t *testing.T) {
	missingFolder := filepath.Join(t.TempDir(), "does-not-exist")
	contracts, err := LoadContracts(missingFolder)

	if err == nil {
		t.Fatalf("expected error when loading contracts from missing folder")
	}
	if len(contracts) != 0 {
		t.Fatalf("expected no contracts for missing folder, got %d", len(contracts))
	}
}

func TestLoadContractsFailFastMalformedJSON(t *testing.T) {
	tmpDir := t.TempDir()

	validSchema := []byte(`{"type":"object","version":"1","metadata":{},"properties":{"price":{"type":"number"}},"required":["price"]}`)
	if err := os.WriteFile(filepath.Join(tmpDir, "valid.json"), validSchema, 0o644); err != nil {
		t.Fatalf("failed to write valid schema: %v", err)
	}

	invalidSchema := []byte(`{"type":"object","version":`)
	if err := os.WriteFile(filepath.Join(tmpDir, "broken.json"), invalidSchema, 0o644); err != nil {
		t.Fatalf("failed to write invalid schema: %v", err)
	}

	_, err := LoadContracts(tmpDir)
	if err == nil {
		t.Fatalf("expected error when malformed schema exists")
	}
}

func TestRejectAndContinueMalformedInput(t *testing.T) {
	orderbook := NewOrderBook()
	matchingEngine := NewMatchingEngine(nil, orderbook)

	messages := [][]byte{
		[]byte(`{"order_id":"buy-1","order_type":"limit","price":10.0,"quantity":4,"action":"BUY","timestamp":1700000000}`),
		[]byte(`{"order_id":"broken","order_type":`),
		[]byte(`{"order_id":"sell-1","order_type":"limit","price":11.0,"quantity":3,"action":"SELL","timestamp":1700000001}`),
	}

	rejectedCount := 0
	processedCount := 0
	for _, message := range messages {
		order, err := messageToOrder(message)
		if err != nil {
			rejectedCount++
			continue
		}
		matchingEngine.Process(&order, nil, nil)
		processedCount++
	}

	if rejectedCount != 1 {
		t.Fatalf("expected 1 malformed message rejected, got %d", rejectedCount)
	}
	if processedCount != 2 {
		t.Fatalf("expected 2 valid messages processed, got %d", processedCount)
	}

	if orderbook.BestBid.Len() != 1 || orderbook.BestBid.Peak() != 10.0 {
		t.Fatalf("unexpected best bid state after reject-and-continue: len=%d peak=%v", orderbook.BestBid.Len(), orderbook.BestBid.Peak())
	}
	if orderbook.BestAsk.Len() != 1 || orderbook.BestAsk.Peak() != 11.0 {
		t.Fatalf("unexpected best ask state after reject-and-continue: len=%d peak=%v", orderbook.BestAsk.Len(), orderbook.BestAsk.Peak())
	}

	bidOrders := orderbook.PriceToBuyOrders[10.0]
	if bidOrders == nil || len(*bidOrders) != 1 || (*bidOrders)[0].OrderID != "buy-1" || (*bidOrders)[0].Quantity != 4 {
		t.Fatalf("unexpected bid queue after reject-and-continue: %+v", bidOrders)
	}

	askOrders := orderbook.PriceToSellOrders[11.0]
	if askOrders == nil || len(*askOrders) != 1 || (*askOrders)[0].OrderID != "sell-1" || (*askOrders)[0].Quantity != 3 {
		t.Fatalf("unexpected ask queue after reject-and-continue: %+v", askOrders)
	}
}

func TestRejectZeroQuantityOrderNoSideEffects(t *testing.T) {
	orderbook := NewOrderBook()
	matchingEngine := NewMatchingEngine(nil, orderbook)

	restingBuy := Order{OrderID: "resting-buy", OrderType: "limit", Price: 10.0, Quantity: 3, Action: "BUY", Timestamp: 1700000000}
	restingSell := Order{OrderID: "resting-sell", OrderType: "limit", Price: 11.0, Quantity: 4, Action: "SELL", Timestamp: 1700000001}
	matchingEngine.Process(&restingBuy, nil, nil)
	matchingEngine.Process(&restingSell, nil, nil)

	tradeChannel := make(chan Trade, 4)
	pricePointChannel := make(chan PricePoint, 4)
	zeroQtyOrder := Order{OrderID: "zero-order", OrderType: "limit", Price: 11.0, Quantity: 0, Action: "SELL", Timestamp: 1700000002}
	matchingEngine.Process(&zeroQtyOrder, tradeChannel, pricePointChannel)

	if len(tradeChannel) != 0 {
		t.Fatalf("expected no trade events for zero-quantity order, got %d", len(tradeChannel))
	}
	if len(pricePointChannel) != 0 {
		t.Fatalf("expected no pricepoint events for zero-quantity order, got %d", len(pricePointChannel))
	}

	if orderbook.BestBid.Len() != 1 || orderbook.BestBid.Peak() != 10.0 {
		t.Fatalf("unexpected best bid after zero-quantity order: len=%d peak=%v", orderbook.BestBid.Len(), orderbook.BestBid.Peak())
	}
	if orderbook.BestAsk.Len() != 1 || orderbook.BestAsk.Peak() != 11.0 {
		t.Fatalf("unexpected best ask after zero-quantity order: len=%d peak=%v", orderbook.BestAsk.Len(), orderbook.BestAsk.Peak())
	}

	bidOrders := orderbook.PriceToBuyOrders[10.0]
	if bidOrders == nil || len(*bidOrders) != 1 || (*bidOrders)[0].OrderID != "resting-buy" || (*bidOrders)[0].Quantity != 3 {
		t.Fatalf("unexpected bid queue after zero-quantity order: %+v", bidOrders)
	}

	askOrders := orderbook.PriceToSellOrders[11.0]
	if askOrders == nil || len(*askOrders) != 1 || (*askOrders)[0].OrderID != "resting-sell" || (*askOrders)[0].Quantity != 4 {
		t.Fatalf("unexpected ask queue after zero-quantity order: %+v", askOrders)
	}
}
