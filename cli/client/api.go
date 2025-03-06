package client

import (
	"errors"
	"time"
)

func (c *Client) SetString(key string, value string) error {
	// Serialize key and value to calculate total size
	b := []byte(value)

	request := &Bluebell{
		Command: SET_KEY,
		Key:     key,
		Value:   b,
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

func (c *Client) GetString(key string) (string, error) {
	request := &Bluebell{
		Command: GET_KEY,
		Key:     key,
		Value:   nil,
	}

	go c.sendRequestToServer(request)
	res, err := c.waitForResponseWithTimeout(5 * time.Second) // 等待响应，设置超时
	if err != nil {
		return "", err
	}

	if res.Code != SUCCESS {
		return "", errors.New(string(res.Result))
	}
	return string(res.Result), nil
}

func (c *Client) Del(key string) error {
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
