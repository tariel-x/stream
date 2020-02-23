package log

import (
	"context"
	"testing"
)

func TestLog_Set(t *testing.T) {
	l, _ := NewLog()
	ctx := context.Background()
	// 0 a; 1 b; 2 c; 3 d; 4 e
	l.Set(ctx, 0, "a")
	l.Set(ctx, 4, "e")
	l.Set(ctx, 3, "d")
	l.Set(ctx, 1, "b")
	l.Set(ctx, 2, "c")

	expected := []string{"b", "c", "d", "e"}
	var actual []string
	results, _ := l.Pull(ctx, 1)
	for letter := range results {
		t.Log(letter)
		actual = append(actual, letter)
	}
	for i := range expected {
		if expected[i] != actual[i] {
			t.Errorf("%s != %s", expected[i], actual[i])
		}
	}
}
