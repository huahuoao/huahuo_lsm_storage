package protocol

import "github.com/huahuoao/lsm-core/internal/storage"

const (
	SuccessCode = "0"
	ErrorCode   = "1"
)

func newResponse(code string, result []byte) *BluebellResponse {
	return &BluebellResponse{
		Code:   code,
		Result: result,
	}
}

func HandleGet(request *BluebellRequest) *BluebellResponse {
	client := storage.GetClient()
	res, ok := client.Get([]byte(request.Key))
	if !ok {
		return newResponse(ErrorCode, nil)
	}
	return newResponse(SuccessCode, res)
}

func HandleSet(request *BluebellRequest) *BluebellResponse {
	client := storage.GetClient()
	err := client.Put([]byte(request.Key), request.Value)
	if err != nil {
		return newResponse(ErrorCode, []byte(err.Error()))
	}
	return newResponse(SuccessCode, nil)
}
