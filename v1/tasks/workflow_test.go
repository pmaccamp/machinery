package tasks_test

import (
	"testing"

	"github.com/pmaccamp/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestNewChain(t *testing.T) {
	t.Parallel()

	task1 := tasks.Signature{
		Name: "foo",
		Args: []interface{}{
			{
				Type:  "float64",
				Value: interface{}(1),
			},
			{
				Type:  "float64",
				Value: interface{}(1),
			},
		},
	}

	task2 := tasks.Signature{
		Name: "bar",
		Args: []interface{}{
			{
				Type:  "float64",
				Value: interface{}(5),
			},
			{
				Type:  "float64",
				Value: interface{}(6),
			},
		},
	}

	task3 := tasks.Signature{
		Name: "qux",
		Args: []interface{}{
			{
				Type:  "float64",
				Value: interface{}(4),
			},
		},
	}

	chain, err := tasks.NewChain(&task1, &task2, &task3)
	if err != nil {
		t.Fatal(err)
	}

	firstTask := chain.Tasks[0]

	assert.Equal(t, "foo", firstTask.Id)
	assert.Equal(t, "bar", firstTask.OnSuccess[0].Id)
	assert.Equal(t, "qux", firstTask.OnSuccess[0].OnSuccess[0].Id)
}
