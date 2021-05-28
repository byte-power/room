package utility

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitSliceBySize(t *testing.T) {
	slice := []interface{}{"a", "b", "c", "d", "e", "f"}
	sizes := []int{-1, 0}
	for _, size := range sizes {
		result, err := SplitSliceBySize(slice, size)
		assert.Equal(t, ErrSizeNotPositive, err)
		assert.Nil(t, result)
	}

	cases := []struct {
		size   int
		result [][]interface{}
	}{
		{
			1,
			[][]interface{}{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}, {"f"}},
		}, {
			2,
			[][]interface{}{{"a", "b"}, {"c", "d"}, {"e", "f"}},
		}, {
			3,
			[][]interface{}{{"a", "b", "c"}, {"d", "e", "f"}},
		}, {
			4,
			[][]interface{}{{"a", "b", "c", "d"}, {"e", "f"}},
		}, {
			5,
			[][]interface{}{{"a", "b", "c", "d", "e"}, {"f"}},
		}, {
			6,
			[][]interface{}{{"a", "b", "c", "d", "e", "f"}},
		}, {
			7,
			[][]interface{}{{"a", "b", "c", "d", "e", "f"}},
		}, {
			100,
			[][]interface{}{{"a", "b", "c", "d", "e", "f"}},
		},
	}
	for _, c := range cases {
		result, err := SplitSliceBySize(slice, c.size)
		assert.Nil(t, err)
		assert.Equal(t, c.result, result)
	}
}

func TestConvertJSONArrayIntoSlices(t *testing.T) {
	slice := []interface{}{"a", "b", "c", "d", "e", "f"}
	jsonBinary, _ := json.Marshal(slice)
	sizes := []int{-1, 0}
	for _, size := range sizes {
		result, err := ConvertJSONArrayIntoSlices(string(jsonBinary), size)
		assert.Equal(t, ErrSizeNotPositive, err)
		assert.Nil(t, result)
	}
	cases := []struct {
		size   int
		result [][]interface{}
	}{
		{
			1,
			[][]interface{}{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}, {"f"}},
		}, {
			2,
			[][]interface{}{{"a", "b"}, {"c", "d"}, {"e", "f"}},
		}, {
			3,
			[][]interface{}{{"a", "b", "c"}, {"d", "e", "f"}},
		}, {
			4,
			[][]interface{}{{"a", "b", "c", "d"}, {"e", "f"}},
		}, {
			5,
			[][]interface{}{{"a", "b", "c", "d", "e"}, {"f"}},
		}, {
			6,
			[][]interface{}{{"a", "b", "c", "d", "e", "f"}},
		}, {
			7,
			[][]interface{}{{"a", "b", "c", "d", "e", "f"}},
		}, {
			100,
			[][]interface{}{{"a", "b", "c", "d", "e", "f"}},
		},
	}
	for _, c := range cases {
		result, err := ConvertJSONArrayIntoSlices(string(jsonBinary), c.size)
		assert.Nil(t, err)
		assert.Equal(t, c.result, result)
	}
}
