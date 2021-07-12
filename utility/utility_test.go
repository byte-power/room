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

func TestStringSet(t *testing.T) {
	items1 := []string{"a", "b", "c", "d"}
	set1 := NewStringSet(items1...)

	assert.Equal(t, len(items1), set1.Len())

	for _, item := range items1 {
		assert.True(t, set1.Contains(item))
	}
	set1.Remove("a")
	assert.False(t, set1.Contains("a"))

	set1.Add("e")
	assert.Equal(t, 4, set1.Len())
	assert.True(t, set1.Contains("e"))

	set1.AddItems("x", "y", "z")
	assert.Equal(t, 7, set1.Len())
	assert.ElementsMatch(t, []string{"b", "c", "d", "e", "x", "y", "z"}, set1.ToSlice())

	items2 := []string{"1", "2", "a", "b", "b", "1", "2", "x"}
	set2 := NewStringSet(items2...)

	assert.Equal(t, 5, set2.Len())

	for _, item := range items2 {
		assert.True(t, set2.Contains(item))
	}

	items3 := []string{"x", "y", "z"}
	set3 := NewStringSet(items3...)
	bytes, err := json.Marshal(set3)
	assert.Nil(t, err)
	_items3 := make([]string, 0)
	_ = json.Unmarshal(bytes, &_items3)
	assert.Equal(t, len(items3), len(_items3))
	assert.ElementsMatch(t, items3, _items3)

	items4 := []string{"1", "x", "y", "z"}
	set4 := NewStringSet(items4...)
	_items4 := []string{"a", "b"}
	bytes, _ = json.Marshal(&_items4)
	err = json.Unmarshal(bytes, set4)
	assert.Nil(t, err)
	assert.Equal(t, 2, set4.Len())
	for _, item := range _items4 {
		assert.True(t, set4.Contains(item))
	}
}

func TestMergeStringSliceAndRemoveDuplicateItems(t *testing.T) {
	slices := [][]string{{"a", "b", "c"}, {"a", "b", "d"}, {"x", "y", "x", "d"}}
	uniqueItems := []string{"a", "b", "c", "d", "x", "y"}
	result := MergeStringSliceAndRemoveDuplicateItems(slices...)
	assert.Equal(t, len(uniqueItems), len(result))
	assert.ElementsMatch(t, result, uniqueItems)

	slices = [][]string{{}, {"a", "b"}, {"a"}}
	uniqueItems = []string{"a", "b"}
	result = MergeStringSliceAndRemoveDuplicateItems(slices...)
	assert.Equal(t, len(uniqueItems), len(result))
	assert.ElementsMatch(t, result, uniqueItems)

	slice := []string{"a", "b", "a", "c"}
	uniqueItems = []string{"a", "b", "c"}
	result = MergeStringSliceAndRemoveDuplicateItems(slice)
	assert.Equal(t, len(uniqueItems), len(result))
	assert.ElementsMatch(t, result, uniqueItems)
}
