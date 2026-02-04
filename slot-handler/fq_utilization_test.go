package main

import "testing"

func TestUtilWindowP90(t *testing.T) {
	u := newUtilWindow(10)
	for i := 0; i < 10; i++ {
		u.Record(9, 10)
	}
	if got := u.P90(); got < 0.9 {
		t.Fatalf("expected P90 >= 0.9, got %v", got)
	}
}

func TestUtilWindowP90IgnoresUncappedSamples(t *testing.T) {
	u := newUtilWindow(5)
	for i := 0; i < 5; i++ {
		u.Record(7, 0)
	}
	if got := u.P90(); got != 1 {
		t.Fatalf("expected P90 to treat uncapped as 1, got %v", got)
	}
}
