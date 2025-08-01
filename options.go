package dynamorm

type Options struct {
	Encoder EncoderInterface
	Decoder DecoderInterface
}

func DefaultOptions() *Options {
	return &Options{
		Encoder: DefaultEncoder(),
		Decoder: DefaultDecoder(),
	}
}

type Option func(*Options)

// WithEncoder provides your own custom marshaling of Go structs to DynamoDB items.
func WithEncoder(e EncoderInterface) Option {
	return func(cfg *Options) {
		if e != nil {
			cfg.Encoder = e
		}
	}
}

// WithDecoder provides your own custom unmarshaling of Go structs from DynamoDB items.
func WithDecoder(d DecoderInterface) Option {
	return func(cfg *Options) {
		if d != nil {
			cfg.Decoder = d
		}
	}
}
