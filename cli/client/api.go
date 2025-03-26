package client

import (
	"errors"
	"time"
)

func (hc *HuaHuoLsmClient) Set(key string, value []byte) error {
	ip, err := GetRing().Get(key)
	if err != nil {
		return err
	}
	err = HuaHuoLsmCli.Clients[ip].set(key, value)
	return err
}

func (hc *HuaHuoLsmClient) Get(key string) ([]byte, error) {
	ip, err := GetRing().Get(key)
	if err != nil {
		return nil, err
	}
	value, err := HuaHuoLsmCli.Clients[ip].get(key)
	return value, err
}

func (c *Client) set(key string, value []byte) error {
	// Serialize key and value to calculate total size

	request := &Bluebell{
		Command: SET_KEY,
		Key:     key,
		Value:   value,
	}

	go c.sendRequestToServer(request)
	res, err := c.waitForResponseWithTimeout(5 * time.Second) // 等待响应，设置超时
	if err != nil {
		return err
	}
	if res.Code != SUCCESS {
		return errors.New(string("set failed"))
	}
	return nil
}

func (c *Client) get(key string) ([]byte, error) {
	request := &Bluebell{
		Command: GET_KEY,
		Key:     key,
		Value:   nil,
	}

	go c.sendRequestToServer(request)
	res, err := c.waitForResponseWithTimeout(5 * time.Second) // 等待响应，设置超时
	if err != nil {
		return nil, err
	}
	if res.Code != SUCCESS {
		return nil, errors.New(string(res.Result))
	}

	return res.Result, nil
}

func (c *Client) del(key string) error {
	request := &Bluebell{
		Command: DEL_KEY,
		Key:     key,
		Value:   nil,
	}

	go c.sendRequestToServer(request)
	res, err := c.waitForResponseWithTimeout(5 * time.Second) // 等待响应，设置超时
	if err != nil {
		return err
	}

	if res.Code != SUCCESS {
		return errors.New(string(res.Result))
	}
	return nil
}
