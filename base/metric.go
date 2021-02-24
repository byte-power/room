package base

import (
	"time"

	"gopkg.in/alexcesaro/statsd.v2"
)

const (
	counterMetricPrefix   = "counter."
	timeMetricPrefix      = "time."
	gaugeMetricPrefix     = "gauge."
	histogramMetricPrefix = "histogram."
)

type MetricClient struct {
	*statsd.Client
}

// 初始化Metric (statsd)
//
// Returns:
//   - metric连接对象
//   - 测试连接可能出现的错误
func InitMetric(config MetricConfig) (MetricClient, error) {
	if err := config.check(); err != nil {
		return MetricClient{}, err
	}
	var opts []statsd.Option
	opts = append(opts, statsd.Address(config.Host))
	if config.Prefix != "" {
		opts = append(opts, statsd.Prefix(config.Prefix))
	}
	if config.MaxPacktSize > 0 {
		opts = append(opts, statsd.MaxPacketSize(config.MaxPacktSize))
	}
	if config.FlushPeriodSeconds > 0 {
		opts = append(opts, statsd.FlushPeriod(time.Second*time.Duration(config.FlushPeriodSeconds)))
	}
	if config.Network != "" {
		opts = append(opts, statsd.Network(config.Network))
	}
	if config.SampleRate > 0 {
		opts = append(opts, statsd.SampleRate(config.SampleRate))
	}
	if len(config.Tags) > 0 {
		opts = append(opts, statsd.Tags(config.Tags...))
	}
	c := MetricClient{}
	client, err := statsd.New(opts...)
	c.Client = client
	return c, err
}

// MetricCount would change count on <num> for key.
func (mc *MetricClient) MetricCount(key string, num interface{}) *MetricClient {
	mc.Count(counterMetricPrefix+key, num)
	return mc
}

// MetricIncrease would increase count on 1 for key with statsd count.
func (mc *MetricClient) MetricIncrease(key string) *MetricClient {
	mc.Count(counterMetricPrefix+key, 1)
	return mc
}

// MetricTimeDuration would record time duration for key with statsd timing.
//
// - Parameters:
//   - duration: e.g. time.Now().Sub(oldTime)
func (mc *MetricClient) MetricTimeDuration(key string, duration time.Duration) *MetricClient {
	ms := float64(duration) / float64(time.Millisecond)
	mc.Timing(timeMetricPrefix+key, ms)
	return mc
}

func (mc *MetricClient) MetricTiming(key string, value interface{}) *MetricClient {
	mc.Timing(timeMetricPrefix+key, value)
	return mc
}

func (mc *MetricClient) MetricGauge(bucket string, value interface{}) *MetricClient {
	mc.Gauge(gaugeMetricPrefix+bucket, value)
	return mc
}

func (mc *MetricClient) MetricHistogram(bucket string, value interface{}) *MetricClient {
	mc.Histogram(histogramMetricPrefix+bucket, value)
	return mc
}
