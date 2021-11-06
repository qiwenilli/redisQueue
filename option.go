package redisQueue

type queueOption struct {
	prefix  string
	name    string
	autoAck bool
}

type option func(opt *queueOption)

func WithPrefix(prefix string) option {
	return func(opt *queueOption) {
		opt.prefix = prefix
	}
}

func WithAck(auto bool) option {
	return func(opt *queueOption) {
		opt.autoAck = auto
	}
}

func WithName(name string) option {
	return func(opt *queueOption) {
		opt.name = name
	}
}
