package redtable

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/garyburd/redigo/redis"
)

const (
	opMulti   = "MULTI"
	opHGet    = "HGET"
	opHMGet   = "HMGET"
	opHDel    = "HDEL"
	opHSet    = "HSET"
	opHKeys   = "HKEYS"
	opHExists = "HEXISTS"
	opExec    = "EXEC"

	EnvRedisServerURL = "REDIS_SERVER_URL"
)

var (
	ErrConnectionAlreadyClosed = errors.New("connection already closed")
)

type Client struct {
	shutdownMu    sync.Mutex
	alreadyClosed bool
	// TODO: (@odeke-em) decide if we should have a pool of connections
	// instead of using one connection or different dials per operation.
	conn redis.Conn
}

func New(redisServerURL string) (*Client, error) {
	conn, err := redis.DialURL(redisServerURL)

	if err != nil {
		return nil, err
	}

	client := &Client{conn: conn}
	return client, nil
}

func (c *Client) Close() error {
	c.shutdownMu.Lock()
	defer c.shutdownMu.Unlock()
	if c.alreadyClosed {
		return ErrConnectionAlreadyClosed
	}

	if err := c.conn.Close(); err != nil {
		return err
	}

	c.alreadyClosed = true
	return nil
}

func (c *Client) doHashOp(opName, hashTableName string, args ...interface{}) ([]interface{}, error) {
	if c.alreadyClosed {
		return nil, ErrConnectionAlreadyClosed
	}

	if err := c.conn.Send(opMulti); err != nil {
		return nil, err
	}

	allArgs := append([]interface{}{hashTableName}, args...)
	if sendErr := c.conn.Send(opName, allArgs...); sendErr != nil {
		return nil, sendErr
	}

	return redis.Values(c.conn.Do(opExec))
}

func (c *Client) HSet(hashTableName string, key, value interface{}) (interface{}, error) {
	replies, err := c.doHashOp(opHSet, hashTableName, key, value)
	if err != nil {
		return nil, err
	}
	return replies[0], nil
}

func multiKeysOp(c *Client, opName, hashTableName string, keys ...interface{}) ([]interface{}, error) {
	return c.doHashOp(opName, hashTableName, keys...)
}

func byKeyOp(c *Client, opName, hashTableName string, key interface{}) (interface{}, error) {
	replies, err := multiKeysOp(c, opName, hashTableName, key)
	if err != nil {
		return nil, err
	}
	return replies[0], nil
}

func (c *Client) HGet(hashTableName string, key interface{}) (interface{}, error) {
	return byKeyOp(c, opHGet, hashTableName, key)
}

func (c *Client) HMGet(hashTableName string, keys ...interface{}) ([]interface{}, error) {
	return c.doHashOp(opHMGet, hashTableName, keys...)
}

func (c *Client) HDel(hashTableName string, key interface{}) (interface{}, error) {
	return byKeyOp(c, opHDel, hashTableName, key)
}

func (c *Client) HKeys(hashTableName string) ([]interface{}, error) {
	return c.doHashOp(opHKeys, hashTableName)
}

func (c *Client) HLen(hashTableName string) (int64, error) {
	replies, err := c.doHashOp(opHDel, hashTableName)
	if err != nil {
		return 0, err
	}

	first := replies[0]
	if count, ok := first.(int64); ok {
		return count, nil
	}

	vStr := fmt.Sprintf("%v", first)
	return strconv.ParseInt(vStr, 10, 64)
}

func (c *Client) HExists(hashTableName string, key interface{}) (bool, error) {
	replies, err := c.doHashOp(opHExists, hashTableName, key)
	if err != nil {
		return false, err
	}

	first := replies[0]
	if existance, ok := first.(bool); ok {
		return existance, nil
	}

	return strconv.ParseBool(fmt.Sprintf("%v", first))
}
