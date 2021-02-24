package utility

import (
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sync"
	"time"
)

const (
	defaultDSN                    = "127.0.0.1:24224"
	defaultNetwork                = "tcp"
	defaultUnixSocketPath         = ""
	defaultConnectTimeout         = 3 * time.Second
	defaultWriteTimeout           = time.Duration(0) // Write() will not time out
	defaultBufferLimit            = 8 * 1024
	defaultRetryWait              = 500
	defaultMaxRetryWait           = 60000
	defaultMaxRetry               = 5
	defaultReconnectWaitIncreRate = 2
)

type TCPWriterConfig struct {
	Network        string        `json:"network"`
	DSN            string        `json:"dsn"`
	UnixSocketPath string        `json:"unix_socket_path"`
	ConnectTimeout time.Duration `json:"connect_timeout"`
	WriteTimeout   time.Duration `json:"write_timeout"`

	BufferLimit        int    `json:""`
	RetryWait          int    `json:"retry_wait"`
	MaxRetry           int    `json:"max_retry"`
	MaxRetryWait       int    `json:"max_retry_wait"`
	TagPrefix          string `json:"tag_prefix"`
	Async              bool   `json:"async"`
	ForceStopAsyncSend bool   `json:"force_stop_async_send"`
	// RequestAck sends the chunk option with a unique ID. The server will
	// respond with an acknowledgement. This option improves the reliability
	// of the message transmission.
	RequestAck bool `json:"request_ack"`
}

type ErrUnknownNetwork struct {
	network string
}

func (e *ErrUnknownNetwork) Error() string {
	return "unknown network " + e.network
}

func NewErrUnknownNetwork(network string) error {
	return &ErrUnknownNetwork{network}
}

type tcpMessageToSend struct {
	data []byte
}

var _ io.Writer = (*TCPWriter)(nil)

type TCPWriter struct {
	Config TCPWriterConfig

	stopRunning chan bool
	pending     chan *tcpMessageToSend
	wg          sync.WaitGroup

	muconn sync.Mutex
	conn   net.Conn
}

func DefaultTCPWriterConfig() TCPWriterConfig {
	return TCPWriterConfig{
		DSN:                defaultDSN,
		Network:            defaultNetwork,
		UnixSocketPath:     defaultUnixSocketPath,
		ConnectTimeout:     defaultConnectTimeout,
		WriteTimeout:       defaultWriteTimeout,
		BufferLimit:        defaultBufferLimit,
		RetryWait:          defaultRetryWait,
		MaxRetry:           defaultMaxRetry,
		MaxRetryWait:       defaultMaxRetryWait,
		TagPrefix:          "",
		Async:              false,
		ForceStopAsyncSend: false,
		RequestAck:         false,
	}
}

func NewTCPWriter(config TCPWriterConfig) *TCPWriter {
	if config.DSN == "" {
		config.DSN = defaultDSN
	}
	if config.Network == "" {
		config.Network = defaultNetwork
	}
	if config.UnixSocketPath == "" {
		config.UnixSocketPath = defaultUnixSocketPath
	}
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = defaultConnectTimeout
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = defaultWriteTimeout
	}
	if config.BufferLimit == 0 {
		config.BufferLimit = defaultBufferLimit
	}
	if config.RetryWait == 0 {
		config.RetryWait = defaultRetryWait
	}
	if config.MaxRetry == 0 {
		config.MaxRetry = defaultMaxRetry
	}
	if config.MaxRetryWait == 0 {
		config.MaxRetryWait = defaultMaxRetryWait
	}

	if config.Async {
		inst := &TCPWriter{
			Config:  config,
			pending: make(chan *tcpMessageToSend, config.BufferLimit),
		}
		inst.wg.Add(1)
		go inst.run()
		return inst
	}
	inst := &TCPWriter{Config: config}
	return inst
}

func (w *TCPWriter) Write(p []byte) (n int, err error) {
	msg := &tcpMessageToSend{}
	if w.Config.Async {
		msg.data = make([]byte, len(p))
		copy(msg.data, p) // duplicate it to avoid data changing
		err = w.appendBuffer(msg)
	} else {
		msg.data = p
		err = w.write(msg)
	}
	if err == nil {
		n = len(p)
	}
	return
}

// appendBuffer appends data to buffer with lock.
func (f *TCPWriter) appendBuffer(msg *tcpMessageToSend) error {
	select {
	case f.pending <- msg:
	default:
		return fmt.Errorf("TCPWriter#appendBuffer: Buffer full, limit %v", f.Config.BufferLimit)
	}
	return nil
}

// close closes the connection.
func (f *TCPWriter) close(c net.Conn) {
	f.muconn.Lock()
	if f.conn != nil && f.conn == c {
		f.conn.Close()
		f.conn = nil
	}
	f.muconn.Unlock()
}

// connect establishes a new connection using the specified transport.
func (f *TCPWriter) connect() (err error) {
	switch f.Config.Network {
	case "tcp":
		f.conn, err = net.DialTimeout(f.Config.Network, f.Config.DSN, f.Config.ConnectTimeout)
	case "unix":
		f.conn, err = net.DialTimeout(f.Config.Network, f.Config.UnixSocketPath, f.Config.ConnectTimeout)
	default:
		err = NewErrUnknownNetwork(f.Config.Network)
	}
	return err
}

func (f *TCPWriter) run() {
	drainEvents := false
	var emitEventDrainMsg sync.Once
	for {
		select {
		case entry, ok := <-f.pending:
			if !ok {
				f.wg.Done()
				return
			}
			if drainEvents {
				emitEventDrainMsg.Do(func() { fmt.Fprintf(os.Stderr, "[%s] Discarding queued events...\n", time.Now().Format(time.RFC3339)) })
				continue
			}
			err := f.write(entry)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[%s] Unable to send logs to TCP Daemon, reconnecting...\n", time.Now().Format(time.RFC3339))
			}
		}
		select {
		case stopRunning, ok := <-f.stopRunning:
			if stopRunning || !ok {
				drainEvents = true
			}
		default:
		}
	}
}

func e(x, y float64) int {
	return int(math.Pow(x, y))
}

func (f *TCPWriter) write(msg *tcpMessageToSend) error {
	var c net.Conn
	connectRetryWaitDurationTotal := time.Duration(0)
	for i := 0; i < f.Config.MaxRetry; i++ {
		c = f.conn
		// Connect if needed
		if c == nil {
			f.muconn.Lock()
			if f.conn == nil {
				connectStartTime := time.Now()
				err := f.connect()
				connectTimeCost := time.Now().Sub(connectStartTime)
				if err != nil {
					f.muconn.Unlock()
					fmt.Printf("TCPWriter#write: failed to connect, connectTime: %v, connectErr: %v, retryTimes: %d\n", connectTimeCost, err, i)
					if _, ok := err.(*ErrUnknownNetwork); ok {
						// do not retry on unknown network error
						break
					}
					waitTime := f.Config.RetryWait * e(defaultReconnectWaitIncreRate, float64(i-1))
					if waitTime > f.Config.MaxRetryWait {
						waitTime = f.Config.MaxRetryWait
					}
					connectRetryWaitDuration := time.Duration(waitTime) * time.Millisecond
					connectRetryWaitDurationTotal += connectRetryWaitDuration
					fmt.Printf(
						"TCPWriter#write: failed to connect and wait to reconnect, connectRetryWaitDuration: %v, connectRetryWaitDurationTotal: %v, retryTimes: %d\n",
						connectRetryWaitDuration,
						connectRetryWaitDurationTotal,
						i)
					time.Sleep(connectRetryWaitDuration)
					continue
				}
				fmt.Printf("TCPWriter#write: successfully connect, connectTime: %v, retryTimes: %d\n", connectTimeCost, i)
			}
			c = f.conn
			f.muconn.Unlock()
		}

		// We're connected, write msg
		t := f.Config.WriteTimeout
		if time.Duration(0) < t {
			c.SetWriteDeadline(time.Now().Add(t))
		} else {
			c.SetWriteDeadline(time.Time{})
		}
		writeStartTime := time.Now()
		_, err := c.Write(msg.data)
		if err != nil {
			fmt.Printf(
				"TCPWriter#write: failed to write, writeTime: %v, writeErr: %v, retryTimes: %d\n",
				time.Now().Sub(writeStartTime),
				err,
				i)
			f.close(c)
		} else {
			return nil
		}
	}

	return fmt.Errorf("TCPWriter#write: failed to reconnect, connectMaxRetries: %d, connectTotalWaitDuration: %v, writeTimeout: %v", f.Config.MaxRetry, connectRetryWaitDurationTotal, f.Config.WriteTimeout)
}
