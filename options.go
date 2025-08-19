package dynamorm

// Options contains configuration options for Storage.
type Options struct {
	Encoder    EncoderInterface
	Decoder    DecoderInterface
	NewBuilder CreateBuilder
}

// DefaultOptions creates default options for the storage, providing default encoder and decoder.
func DefaultOptions() *Options {
	return &Options{
		Encoder:    DefaultEncoder(),
		Decoder:    DefaultDecoder(),
		NewBuilder: NewBuilder,
	}
}

// Option is a function type that modifies Options for use with NewStorage().
type Option func(*Options)

// WithEncoder provides your own custom encoder when creating a Storage with NewStorage().
func WithEncoder(e EncoderInterface) Option {
	return func(cfg *Options) {
		if e != nil {
			cfg.Encoder = e
		}
	}
}

// WithDecoder provides your own custom decoder when creating a Storage with NewStorage().
func WithDecoder(d DecoderInterface) Option {
	return func(cfg *Options) {
		if d != nil {
			cfg.Decoder = d
		}
	}
}

// WithBuilder allows providing a custom builder factory used by Storage to
// create expression builders. Useful for testing or extending behavior.
func WithBuilder(nb CreateBuilder) Option {
	return func(cfg *Options) {
		if nb != nil {
			cfg.NewBuilder = nb
		}
	}
}
