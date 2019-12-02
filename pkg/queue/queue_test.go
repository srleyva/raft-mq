package queue

import (
	"testing"
)

func TestQueue(t *testing.T) {
	t.Run("test queue init fails with NoName", func(t *testing.T) {
		_, err := NewQueue("", nil)
		if err != ErrNoName {
			t.Errorf("wrong error returned: %s", err)
		}
	})
	t.Run("test queue initilization with no items", func(t *testing.T) {
		queue, err := NewQueue("test", nil)
		if err != nil {
			t.Errorf("error where not expected: %s", err)
		}
		if len(queue.ListItems()) != 0 {
			t.Errorf("wrong number of items: expected 0, actual: %d", len(queue.ListItems()))
		}
		if queue.String() != "test" {
			t.Errorf("string does not return correct name: Actual: test Expected: %s", queue.String())
		}
	})
	t.Run("test queue pop and empty bool", func(t *testing.T) {
		items := []interface{}{"hello", "these", "are", "messages"}
		queue, err := NewQueue("test", items)
		if err != nil {
			t.Errorf("error where not expected: %s", err)
		}

		if queue.IsEmpty() {
			t.Errorf("is empty returning true when false")
		}

		for _, item := range items {
			queueItem, err := queue.Pop()
			if err != nil {
				t.Errorf("error where not expected: %s", err)
			}
			if queueItem != item {
				t.Errorf("wrong item returned: actual: %s, expected: %s", queueItem, item)
			}
		}

		if _, err := queue.Pop(); err != ErrQueueIsEmpty {
			t.Errorf("wrong error returned: %s", err)
		}

		if !queue.IsEmpty() {
			t.Errorf("is empty returning false when true")
		}
	})
	t.Run("test queue insert", func(t *testing.T) {
		queue, err := NewQueue("test", nil)
		if err != nil {
			t.Errorf("error where not expected: %s", err)
		}
		queue.InsertItem("hello")
		item, err := queue.Pop()
		if err != nil {
			t.Errorf("error where not expected: %s", err)
		}
		if item != "hello" {
			t.Errorf("wrong string returned: Actual: %s, Expected: hello", item)
		}
	})
}
