package main

import "testing"

func TestGetenv(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		fallback string
		want     string
	}{
		{name: "fallback when unset", envValue: "", fallback: "10", want: "10"},
		{name: "returns env value", envValue: "42", fallback: "10", want: "42"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NUM_TRADERS", tc.envValue)
			value := getenv("NUM_TRADERS", tc.fallback)
			if value != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, value)
			}
		})
	}
}
