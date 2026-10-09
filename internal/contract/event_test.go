package contract

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func fixture() Event {
	b := Business{RunID: "fixture", TransactionID: ID("run", "0"), SourceSeq: 0,
		EventTimeMS: 1791417600000, CardToken: "card_" + ID("card", "1"),
		MerchantID: "merchant-001", MCC: "5411", IssuerBankID: "bank-0",
		AcquirerBankID: "bank-1", Currency: "USD", AmountMinor: 1234,
		AuthorizationDate: "2026-10-08", RuleVersion: "preview-rules-v1",
		ReferenceVersion: "synthetic-reference-v1"}
	e, err := New(b, 1)
	if err != nil {
		panic(err)
	}
	return e
}

func TestHashBoundaries(t *testing.T) {
	e := fixture()
	later, err := New(e.Business, 999)
	if err != nil || e.PayloadHash != later.PayloadHash || e.EventID != later.EventID {
		t.Fatal("transport time changed identity")
	}
	changed := e.Business
	changed.AmountMinor++
	different, err := New(changed, 1)
	if err != nil || different.PayloadHash == e.PayloadHash {
		t.Fatal("money not hashed")
	}
	if ID("ab", "c") == ID("a", "bc") {
		t.Fatal("ambiguous ID encoding")
	}
	if bytes.Contains(Canonical(e.Business), []byte("produced_at")) {
		t.Fatal("transport in hash")
	}
}

func TestStrictRequiredFieldsAndMoney(t *testing.T) {
	valid, _ := json.Marshal(fixture())
	if _, err := Decode(valid); err != nil {
		t.Fatal(err)
	}
	for _, replacement := range []string{"1.5", "9223372036854775808", "null", "-1"} {
		data := bytes.Replace(valid, []byte(`"amount_minor":1234`), []byte(`"amount_minor":`+replacement), 1)
		if _, err := Decode(data); err == nil {
			t.Fatalf("accepted money %s", replacement)
		}
	}
	for _, data := range [][]byte{
		bytes.Replace(valid, []byte(`"schema_version":1`), []byte(`"schema_version":2`), 1),
		bytes.Replace(valid, []byte(`"source_seq":0,`), nil, 1),
		bytes.Replace(valid, []byte(`"source_seq":0`), []byte(`"source_seq":null`), 1),
		bytes.Replace(valid, []byte(`"currency":"USD"`), []byte(`"currency":"EUR"`), 1),
		append(append([]byte{}, valid...), []byte(` {}`)...),
	} {
		if _, err := Decode(data); err == nil {
			t.Fatalf("accepted invalid event: %s", data)
		}
	}
	extra := strings.TrimSuffix(string(valid), "}") + `,"optional_note":"ok"}`
	if _, err := Decode([]byte(extra)); err != nil {
		t.Fatal(err)
	}
}
