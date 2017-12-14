package redtable

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	opMulti   = "MULTI"
	opHGet    = "HGET"
	opHMGet   = "HMGET"
	opHDel    = "HDEL"
	opHLen    = "HLEN"
	opHSet    = "HSET"
	opHKeys   = "HKEYS"
	opHExists = "HEXISTS"
	opExec    = "EXEC"
	opDel     = "DEL"
	opLLen    = "LLEN"
	opLPush   = "LPUSH"
	opLPop    = "LPOP"
	opLIndex  = "LINDEX"
	opSAdd    = "SADD"
	opSPop    = "SPOP"
	opSRem    = "SREM"
	opSLen    = "SLEN"

	opSMembers  = "SMEMBERS"
	opSIsMember = "SISMEMBER"

	EnvRedisServerURL = "REDIS_SERVER_URL"
)

var (
	ErrConnectionAlreadyClosed = errors.New("connection already closed")
)

type Client struct {
	shutdownMu sync.Mutex
	closeOnce  sync.Once

	connMu sync.Mutex

	// _curConn is the connection being currently used
	_curConn redis.Conn
	dialFn   func() (redis.Conn, error)
	pool     *redis.Pool
}

func redisDialer(url string) func() (redis.Conn, error) {
	return func() (redis.Conn, error) {
		conn, err := redis.DialURL(url, redis.DialConnectTimeout(550*time.Millisecond))
		return conn, err
	}
}

var defaultRedisConnIdleTime = 4 * time.Minute

func New(redisServerURLs ...string) (*Client, error) {
	var dialFn func() (redis.Conn, error)
	for _, srvURL := range redisServerURLs {
		fn := redisDialer(srvURL)
		conn, err := fn()
		if err != nil {
			continue
		}
		conn.Close()
		dialFn = fn
		break
	}

	if dialFn == nil {
		return nil, errInvalidRedisURLs
	}

	client := &Client{
		dialFn: dialFn,
		pool:   poolFromDialer(dialFn),
	}
	return client, nil
}

func poolFromDialer(dialFn func() (redis.Conn, error)) *redis.Pool {
	return &redis.Pool{
		Dial:        dialFn,
		MaxIdle:     4,    // Maximum number of idle connections in the pool
		MaxActive:   1000, // Maximum number of active connections at any instance
		IdleTimeout: 1 * time.Minute,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < defaultRedisConnIdleTime {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func (c *Client) poolConn() redis.Conn {
	if c.pool == nil {
		c.pool = poolFromDialer(c.dialFn)
	}
	return c.pool.Get()
}

func (c *Client) conn() redis.Conn {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c._curConn == nil {
		c._curConn = c.poolConn()
		return c._curConn
	}

	cn := c._curConn
	if cn.Err() == nil {
		// Go to go then
		return cn
	}

	// Otherwise time to refresh it
	cn.Close()
	c._curConn = nil
	return c.conn()
}

var errInvalidRedisURLs = errors.New("expecting at least one valid URL connection URL")

func (c *Client) Close() error {
	var err error = ErrConnectionAlreadyClosed
	c.closeOnce.Do(func() {
		err = c.pool.Close()
	})
	return err
}

func (c *Client) ConnErr() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if conn := c._curConn; conn != nil {
		return conn.Err()
	}
	return nil
}

func (c *Client) doHashOp(opName, hashTableName string, args ...interface{}) ([]interface{}, error) {
	if err := c.conn().Send(opMulti); err != nil {
		return nil, err
	}

	allArgs := append([]interface{}{hashTableName}, args...)
	if sendErr := c.conn().Send(opName, allArgs...); sendErr != nil {
		return nil, sendErr
	}
	return redis.Values(c.conn().Do(opExec))
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
	replies, err := c.doHashOp(opHLen, hashTableName)
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

func (c *Client) SAdd(tableName string, items ...interface{}) (interface{}, error) {
	return byKeyOp(c, opSAdd, tableName, items...)
}

func (c *Client) SMembers(tableName string) (interface{}, error) {
	return byKeyOp(c, opSMembers, tableName)
}

func (c *Client) SIsMember(tableName string, key interface{}) (bool, error) {
	retr, err := byKeyOp(c, opSIsMember, tableName, key)
	if err != nil {
		return false, err
	}
	value := retr.(int64)
	return value >= 1, nil
}

// SPop implements Redis command SPOP and it pops just 1 element from the set.
func (c *Client) SPop(tableName string) (interface{}, error) {
	return byKeyOp(c, opSPop, tableName)
}

// SRem implements Redis command SREM which removes elements from a set.
func (c *Client) SRem(tableName string, key interface{}, otherKeys ...interface{}) (interface{}, error) {
	keys := append([]interface{}{key}, otherKeys...)
	return byKeyOp(c, opSRem, tableName, keys...)
}
