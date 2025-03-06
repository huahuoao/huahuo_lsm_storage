package client

const (
	MB                         = 1 << 20
	GB                         = 1 << 30
	HTTP_BODY_DEFAULT_MAX_SIZE = 32 * MB
)

// command
const (
	GET_KEY = "get"
	SET_KEY = "set"
	DEL_KEY = "del"
)
const (
	SUCCESS = "200"
)
const (
	CONSISTENTHASH_VIRTUAL_NODE_NUM = 160
)
