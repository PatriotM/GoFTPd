package core

import "testing"

func TestNormalizeTransferMode(t *testing.T) {
	if mode, ok := normalizeTransferMode("s"); !ok || mode != "S" {
		t.Fatalf("expected stream mode to normalize, got %q %v", mode, ok)
	}
	if _, ok := normalizeTransferMode("B"); ok {
		t.Fatalf("expected unsupported mode to be rejected")
	}
}

func TestNormalizeTransferStructure(t *testing.T) {
	if structure, ok := normalizeTransferStructure("f"); !ok || structure != "F" {
		t.Fatalf("expected file structure to normalize, got %q %v", structure, ok)
	}
	if _, ok := normalizeTransferStructure("R"); ok {
		t.Fatalf("expected unsupported structure to be rejected")
	}
}

func TestNormalizeTransferType(t *testing.T) {
	if transferType, ok := normalizeTransferType("a"); !ok || transferType != "A" {
		t.Fatalf("expected ASCII type to normalize, got %q %v", transferType, ok)
	}
	if transferType, ok := normalizeTransferType("I"); !ok || transferType != "I" {
		t.Fatalf("expected binary type to normalize, got %q %v", transferType, ok)
	}
	if _, ok := normalizeTransferType("L 8"); ok {
		t.Fatalf("expected unsupported type to be rejected")
	}
}
