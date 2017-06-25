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
	opDel     = "DEL"
	opLLen    = "LLEN"
	opLPush   = "LPUSH"
	opLPop    = "LPOP"
	opLIndex  = "LINDEX"

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

var errExpectingAURL = fmt.Errorf("expecting at least one URL")

func New(redisServerURLs ...string) (*Client, error) {
	var conn redis.Conn
	var err error = errExpectingAURL
	for _, srvURL := range redisServerURLs {
		conn, err = redis.DialURL(srvURL)

		if err != nil {
			continue
		}

		client := &Client{conn: conn}
		return client, nil
	}

	return nil, err
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

func byKeyOp(c *Client, opName, hashTableName string, keys ...interface{}) (interface{}, error) {
	replies, err := multiKeysOp(c, opName, hashTableName, keys...)
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

// HPop performs a pop which is a combination of `HGet` and `HDel` from a HashTable
func (c *Client) HPop(hashTableName string, key interface{}) (interface{}, error) {
	// HPop is a combination of HGet+HDel, in that order
	retrieved, err := c.HGet(hashTableName, key)
	if err != nil {
		return nil, err
	}
	if _, err := c.HDel(hashTableName, key); err != nil {
		return nil, err
	}
	return retrieved, nil
}

// HMove moves the contents keyed by a key from hashTableName1 to hashTableName2
func (c *Client) HMove(hashTableName1, hashTableName2 string, key interface{}) (interface{}, error) {
	table1Entry, err := c.HPop(hashTableName1, key)
	if err != nil {
		return nil, err
	}

	if _, err := c.HSet(hashTableName2, key, table1Entry); err != nil {
		return nil, err
	}
	return table1Entry, nil
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

// Del deletes one or more collections by the
// collection name, where any of the types are:
// hash, set, sorted set, list.
func (c *Client) Del(firstTable string, otherTables ...interface{}) (interface{}, error) {
	return byKeyOp(c, opDel, firstTable, otherTables...)
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

func (c *Client) LPush(tableName string, values ...interface{}) (interface{}, error) {
	return byKeyOp(c, opLPush, tableName, values...)
}

func (c *Client) LPop(tableName string) (interface{}, error) {
	return byKeyOp(c, opLPop, tableName)
}

func (c *Client) LLen(tableName string) (int64, error) {
	res, err := byKeyOp(c, opLLen, tableName)
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (c *Client) LIndex(tableName string, index int64) (interface{}, error) {
	return byKeyOp(c, opLIndex, tableName, index)
}
