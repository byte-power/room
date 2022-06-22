package utility

import (
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func PanicIfNotNil(err error) {
	if err == nil {
		return
	}
	log.Println(err)
	debug.PrintStack()
	panic(err)
}

var envVars map[string]string

func EnvironmentVariables() map[string]string {
	if envVars != nil {
		return envVars
	}
	lines := os.Environ()
	envVars = make(map[string]string, len(lines))
	for _, line := range lines {
		comps := strings.Split(line, "=")
		if len(comps) > 1 {
			envVars[comps[0]] = comps[1]
		}
	}
	return envVars
}

type StrMap = map[string]interface{}
type AnyMap = map[interface{}]interface{}

func AnyToAnyMap(value interface{}) AnyMap {
	if value == nil {
		return nil
	}
	switch val := value.(type) {
	case AnyMap:
		return val
	case StrMap:
		count := len(val)
		if count == 0 {
			return nil
		}
		m := make(AnyMap, count)
		for k, v := range val {
			m[k] = v
		}
		return m
	default:
		return nil
	}
}

func AnyToStrMap(value interface{}) StrMap {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case StrMap:
		return v
	case AnyMap:
		l := len(v)
		if l == 0 {
			return nil
		}
		m := make(StrMap, l)
		for k, v := range v {
			m[AnyToString(k)] = v
		}
		return m
	default:
		return nil
	}
}

func AnyToString(value interface{}) string {
	if value == nil {
		return ""
	}
	switch val := value.(type) {
	case *string:
		if val == nil {
			return ""
		}
		return *val
	case string:
		return val
	case int:
		return strconv.Itoa(val)
	case error:
		return val.Error()
	case []byte:
		return string(val)
	default:
		return fmt.Sprint(value)
	}
}

func AnyToInt64(value interface{}) int64 {
	if value == nil {
		return 0
	}
	switch val := value.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	case *string:
		if val == nil {
			return 0
		}
		if i, err := StringToInt64(*val); err == nil {
			return i
		}
	case string:
		if i, err := StringToInt64(val); err == nil {
			return i
		}
	case float32:
		return int64(val)
	case float64:
		return int64(val)
	case bool:
		if val {
			return 1
		}
		return 0
	}
	return 0
}

func AnyToFloat64(value interface{}) float64 {
	if value == nil {
		return 0
	}
	switch val := value.(type) {
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	case uint:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case *string:
		if val == nil {
			return 0
		}
		if v, err := strconv.ParseFloat(*val, 64); err == nil {
			return v
		}
	case string:
		if v, err := strconv.ParseFloat(val, 64); err == nil {
			return v
		}
	case bool:
		if val {
			return 1
		}
		return 0
	}
	return 0
}

func AnyToBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch v := v.(type) {
	case bool:
		return v
	case int:
		return v != 0
	case int8:
		return v != 0
	case int16:
		return v != 0
	case int32:
		return v != 0
	case int64:
		return v != 0
	case uint:
		return v != 0
	case uint8:
		return v != 0
	case uint16:
		return v != 0
	case uint32:
		return v != 0
	case uint64:
		return v != 0
	case float32:
		return v != 0
	case float64:
		return v != 0
	case string:
		if len(v) == 0 {
			return false
		}
		c := strings.ToLower(v[0:1])
		return c == "y" || c == "t" || c == "1"
	case *string:
		return v != nil && AnyToBool(*v)
	default:
		return false
	}
}

func AnyArrayToStrMap(mapInterface []interface{}) StrMap {
	if len(mapInterface)/2 < 1 {
		return nil
	}
	elementMap := make(StrMap)
	for i := 0; i < len(mapInterface)/2; i += 1 {
		key := AnyToString(mapInterface[i*2])
		elementMap[key] = mapInterface[i*2+1]
	}
	return elementMap
}

func AnyToStringArray(any interface{}) []string {
	if any == nil {
		return nil
	}
	switch v := any.(type) {
	case []string:
		return v
	case []interface{}:
		return AnyArrayToStringArray(v)
	default:
		return nil
	}
}

func AnyArrayToStringArray(arrInterface []interface{}) []string {
	elementArray := make([]string, len(arrInterface))
	for i, v := range arrInterface {
		elementArray[i] = AnyToString(v)
	}
	return elementArray
}

func StringToInt64(value string) (int64, error) {
	if index := strings.Index(value, "."); index > 0 {
		value = value[:index]
	}
	return strconv.ParseInt(value, 10, 64)
}

// BytesToString 按string的底层结构，转换[]byte
func BytesToString(b []byte) string {
	if b == nil {
		return ""
	}
	return *(*string)(unsafe.Pointer(&b))
}

// StringToBytes 按[]byte的底层结构，转换字符串，len与cap皆为字符串的len
func StringToBytes(s string) []byte {
	return StringPToBytes(&s)
}

func StringPToBytes(s *string) []byte {
	if s == nil {
		return nil
	}
	// 获取s的起始地址开始后的两个 uintptr 指针
	x := (*[2]uintptr)(unsafe.Pointer(s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func FindInSyncMap(m *sync.Map, keys ...interface{}) interface{} {
	return FindInSyncMapWithKeys(m, keys)
}

func FindInSyncMapWithKeys(m *sync.Map, keys []interface{}) interface{} {
	if m == nil {
		return nil
	}
	l := len(keys)
	if l == 0 {
		return nil
	}
	v0, ok := m.Load(keys[0])
	if !ok || l == 1 {
		return v0
	}
	switch v := v0.(type) {
	case StrMap:
		return FindInStrMapWithKeys(v, keys[1:])
	case AnyMap:
		return FindInAnyMapWithKeys(v, keys[1:])
	default:
		return nil
	}
}

func FindInAnyMap(m AnyMap, keys ...interface{}) interface{} {
	return FindInAnyMapWithKeys(m, keys)
}

func FindInAnyMapWithKeys(m AnyMap, keys []interface{}) interface{} {
	if m == nil {
		return nil
	}
	l := len(keys)
	if l == 0 {
		return nil
	}
	value := m[keys[0]]
	if l == 1 {
		return value
	}
	switch v := value.(type) {
	case AnyMap:
		return FindInAnyMapWithKeys(v, keys[1:])
	case StrMap:
		return FindInStrMapWithKeys(v, keys[1:])
	default:
		return nil
	}
}

func FindInStrMap(m StrMap, keys ...interface{}) interface{} {
	return FindInStrMapWithKeys(m, keys)
}

func FindInStrMapWithKeys(m StrMap, keys []interface{}) interface{} {
	if m == nil {
		return nil
	}
	l := len(keys)
	if l == 0 {
		return nil
	}
	value := m[AnyToString(keys[0])]
	if l == 1 {
		return value
	}
	switch v := value.(type) {
	case AnyMap:
		return FindInAnyMapWithKeys(v, keys[1:])
	case StrMap:
		return FindInStrMapWithKeys(v, keys[1:])
	default:
		return nil
	}
}

//flatten map ,e.g
// map A
// {
// 	"foo":{
// 		"bar":1
// 	}
// }
// map B = FlattenMap("",".",A)
// {
// 	"foo.bar":1
// }

func FlattenMap(rootKey, delimiter string, originData StrMap) StrMap {
	result := make(StrMap)
	for key, value := range originData {
		var tmpKey string
		if rootKey == "" {
			tmpKey = key
		} else {
			tmpKey = rootKey + delimiter + key
		}
		if reflect.ValueOf(value).Kind() == reflect.Map {
			v := AnyToStrMap(value)
			tmpMap := FlattenMap(tmpKey, delimiter, v)
			for k, v := range tmpMap {
				result[k] = v
			}
		} else {
			result[tmpKey] = value
		}
	}
	return result
}

func CanConvertToFloat32Loselessly(v float64) bool {
	absV := math.Abs(v)
	if absV < math.MaxFloat32 && absV > math.SmallestNonzeroFloat32 {
		return true
	}
	return false
}

func CanConvertToInt64Loselessly(v float64) bool {
	return v == math.Trunc(v)
}

func CanConvertToInt32Loselessly(v float64) bool {
	return v == math.Trunc(v) && v < math.MaxInt32 && v > math.MinInt32
}

// StringToChunks split a string into string slices with element's size <= chunkSize
// Examples:
// StringToChunks("abcd", 1) => []string{"a", "b", "c", "d"}
// StringToChunks("abcd", 2) => []string{"ab", "cd"}
// StringToChunks("abcd", 3) => []string{"abc", "d"}
// stringToChunks("abcd", 4) => []string{"abcd"}
// stringToChunks("abcd", 5) => []string{"abcd"}
func StringToChunks(s string, chunkSize int) []string {
	var chunks []string
	strLength := len(s)
	index := 0
	for index < strLength {
		endIndex := IntMin(index+chunkSize, strLength)
		chunk := s[index:endIndex]
		chunks = append(chunks, chunk)
		index = endIndex
	}
	return chunks
}

func IntMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func StringSliceContains(slice []string, str string) bool {
	for _, item := range slice {
		if item == str {
			return true
		}
	}

	return false
}

func IntSliceContains(slice []int, i int) bool {
	for _, item := range slice {
		if item == i {
			return true
		}
	}
	return false
}

func StringSliceToInterfaceSlice(slice []string) []interface{} {
	result := make([]interface{}, len(slice))
	for index, item := range slice {
		result[index] = item
	}
	return result
}

type StringSet struct {
	m     map[string]bool
	mutex sync.Mutex
}

func NewStringSet(items ...string) *StringSet {
	m := make(map[string]bool)
	for _, item := range items {
		m[item] = true
	}
	return &StringSet{m: m, mutex: sync.Mutex{}}
}

func (set *StringSet) Add(item string) {
	set.mutex.Lock()
	defer set.mutex.Unlock()
	set.m[item] = true
}

func (set *StringSet) AddItems(items ...string) {
	set.mutex.Lock()
	defer set.mutex.Unlock()
	for _, item := range items {
		set.m[item] = true
	}
}

func (set *StringSet) Remove(item string) {
	set.mutex.Lock()
	defer set.mutex.Unlock()
	delete(set.m, item)
}

func (set *StringSet) Contains(item string) bool {
	set.mutex.Lock()
	defer set.mutex.Unlock()
	return set.m[item]
}

func (set *StringSet) ToSlice() []string {
	items := make([]string, 0, len(set.m))
	set.mutex.Lock()
	defer set.mutex.Unlock()
	for item := range set.m {
		items = append(items, item)
	}
	return items
}

func (set *StringSet) Len() int {
	set.mutex.Lock()
	defer set.mutex.Unlock()
	return len(set.m)
}

func (set *StringSet) MarshalJSON() ([]byte, error) {
	return json.Marshal(set.ToSlice())
}

func (set *StringSet) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	slice := make([]string, 0)
	if err := json.Unmarshal(data, &slice); err != nil {
		return err
	}
	*set = *NewStringSet(slice...)
	return nil
}

func (set *StringSet) Copy() *StringSet {
	set.mutex.Lock()
	defer set.mutex.Unlock()
	m := make(map[string]bool, len(set.m))
	for key, value := range set.m {
		m[key] = value
	}
	return &StringSet{m: m, mutex: sync.Mutex{}}
}

func (set *StringSet) Merge(s *StringSet) {
	slice := s.ToSlice()
	set.mutex.Lock()
	defer set.mutex.Unlock()
	for _, item := range slice {
		set.m[item] = true
	}
}

func MergeStringSet(sets ...*StringSet) *StringSet {
	set := NewStringSet([]string{}...)
	for _, s := range sets {
		set.AddItems(s.ToSlice()...)
	}
	return set
}

func IsTwoStringSliceEqual(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for index, value := range s1 {
		if s2[index] != value {
			return false
		}
	}
	return true
}

func IsTwoStringSliceContainsSameElement(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	m1 := make(map[string]bool)
	m2 := make(map[string]bool)
	for _, item := range s1 {
		m1[item] = true
	}
	for _, item := range s2 {
		m2[item] = true
	}
	for item := range m1 {
		if !m2[item] {
			return false
		}
	}
	return true
}

func IsTwoStringMapEqual(m1, m2 map[string]string) bool {
	if len(m1) != len(m2) {
		return false
	}
	for key, value := range m1 {
		v2, ok := m2[key]
		if !ok {
			return false
		}
		if v2 != value {
			return false
		}
	}
	return true
}

func TimestampInMS(t time.Time) int64 {
	return t.UnixNano() / 1000 / 1000
}

func GetSecondsAndNanoSecondsFromTsInMs(ts int64) (int64, int64) {
	seconds := ts / 1000
	ms := ts % 1000
	ns := ms * 1000 * 1000
	return seconds, ns
}

var ErrSizeNotPositive = errors.New("size should be greater than 0")

func ConvertJSONArrayIntoSlices(v string, size int) ([][]interface{}, error) {
	if size <= 0 {
		return nil, ErrSizeNotPositive
	}
	value := []interface{}{}
	if err := json.Unmarshal([]byte(v), &value); err != nil {
		return nil, err
	}
	return SplitSliceBySize(value, size)
}

func SplitSliceBySize(slice []interface{}, size int) ([][]interface{}, error) {
	if size <= 0 {
		return nil, ErrSizeNotPositive
	}
	slices := [][]interface{}{}
	length := len(slice)
	chunkCount := length / size
	if length%size != 0 {
		chunkCount++
	}
	for index := 0; index < chunkCount; index++ {
		var s []interface{}
		if (index+1)*size > length {
			s = slice[index*size:]
		} else {
			s = slice[index*size : (index+1)*size]
		}
		slices = append(slices, s)
	}
	return slices, nil
}

func GetLatestTime(times ...time.Time) time.Time {
	latestTime := time.Time{}
	for _, t := range times {
		if t.After(latestTime) {
			latestTime = t
		}
	}
	return latestTime
}

func MergeStringSliceAndRemoveDuplicateItems(slices ...[]string) []string {
	return MergeStringSlicesToStringSet(slices...).ToSlice()
}

func MergeStringSlicesToStringSet(slices ...[]string) *StringSet {
	if len(slices) == 0 {
		return NewStringSet([]string{}...)
	}
	set := NewStringSet(slices[0]...)
	for _, slice := range slices[1:] {
		set.AddItems(slice...)
	}
	return set
}

func FuncName() string {
	pc, _, _, _ := runtime.Caller(1)
	fn := runtime.FuncForPC(pc).Name()
	if ind := strings.LastIndexByte(fn, '/'); ind != -1 {
		fn = fn[ind+1:]
	}
	return fn
}
