package bitcask

import "github.com/wwq1988/bitcask/internal/codec"

// Options Options
type Options struct {
	MaxFileSize  int64
	CodecFactory codec.Factory
}

func genOptions(opts ...Option) *Options {
	options := &Options{
		MaxFileSize:  4 << 20,
		CodecFactory: codec.New,
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// Option Option
type Option func(*Options) error

// WithMaxFileSize WithMaxFileSize
func WithMaxFileSize(size int64) Option {
	return func(options *Options) error {
		options.MaxFileSize = size
		return nil
	}
}
