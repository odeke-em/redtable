package redtable

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/odeke-em/go-uuid"
)

func envOrAlternates(envVar string, alternates ...string) string {
	if retr := os.Getenv(envVar); retr != "" {
		return retr
	}

	for _, alt := range alternates {
		if alt != "" {
			return alt
		}
	}
	return ""
}

func newTestClient() (*Client, error) {
	return New(envOrAlternates(EnvRedisServerURL, "redis://localhost:6379"))
}

func TestNew(t *testing.T) {
	client, err := newTestClient()
	if err != nil {
		t.Fatalf("failed to create a newClient, err %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("first close should be successful, got non-nil err %v", err)
	}

	for i := 0; i < 10; i++ {
		if err := client.Close(); err == nil {
			t.Errorf("client.Close: #%d succeeded, yet wanted failures", i)
		}
	}
}

func TestSettingAndGetting(t *testing.T) {
	client, err := newTestClient()
	if err != nil {
		t.Fatalf("failed to create a newClient, err %v", err)
	}

	kvps := []struct {
		key     interface{}
		value   interface{}
		comment string
	}{
		{12, "new", "mix of int and string"},
		{"Go1.6,7", 2016, "What a year to be alive"},
		{"@odeke_et", "7936d5b2-ca17-4635-ad70-ca477530faba237017917", "pure string KVP"},
	}

	tableName := "multi-consumer-test"
	defer func() {
		// cleanUp Phase
		for _, kvp := range kvps {
			_, err := client.HDel(tableName, kvp.key)
			if err != nil {
				t.Errorf("failed to delete key %v err %v; comment %s", kvp.key, err, kvp.comment)
			}
		}

		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	for i, kvp := range kvps {
		_, err := client.HSet(tableName, kvp.key, kvp.value)
		if err != nil {
			t.Errorf("HSet:: #%d key:%v value %v failed err %v; comment %s", i, kvp.key, kvp.value, err, kvp.comment)
		}
	}

	// First pass
	for i, kvp := range kvps {
		exists, err := client.HExists(tableName, kvp.key)
		if !exists {
			t.Errorf("#%d key:%v must exist", i, kvp.key)
		}
		if err != nil {
			t.Errorf("HExists:: #%d key:%v exist err failed err %v", i, kvp.key, err)
		}
	}

	for i, kvp := range kvps {
		got, err := client.HGet(tableName, kvp.key)
		if err != nil {
			t.Errorf("HGet:: #%d key:%v get err %v", i, kvp.key, err)
			continue
		}
		gotB := []byte(fmt.Sprintf("%s", got))
		wantB := []byte(fmt.Sprintf("%v", kvp.value))

		if !bytes.Equal(gotB, wantB) {
			t.Errorf("HGet:: #%d key:%v want %s got %s", i, kvp.key, wantB, gotB)
		}
	}
}

func TestHPop(t *testing.T) {
	client, err := newTestClient()
	if err != nil {
		t.Fatalf("creating client err=%v", err)
	}

	tableName := uuid.NewRandom().String()
	defer func() {
		clearTable(client, tableName)

		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	kvMap := map[interface{}]interface{}{
		"intrepid": "fluent",
		0x1927:     0x2719,
	}

	for key, value := range kvMap {
		_, err := client.HSet(tableName, key, value)
		if err != nil {
			t.Errorf("err=%v wantErr=nil, key=%v value=%v", err, key, value)
		}
	}

	for key, liveValue := range kvMap {
		poppedValue, err := client.HPop(tableName, key)
		if err != nil {
			t.Errorf("err=%v for HPop wantErr=nil;tableName=%v key=%v", err, tableName, key)
		}

		// We can only compare by string repr comparsions
		// since the retrieved value was a []byte retrieval
		strReprLive, strReprPopped := fmt.Sprintf("%v", liveValue), fmt.Sprintf("%s", poppedValue)
		if strReprLive != strReprPopped {
			t.Errorf("liveValue(%q) retrValue(%q) for key(%v)", strReprLive, strReprPopped, key)
		}
	}
}

func clearTable(client *Client, tableName string) (pass, fail uint64) {
	keys, _ := client.HKeys(tableName)
	for _, key := range keys {
		_, err := client.HDel(tableName, key)
		if err == nil {
			pass += 1
		} else {
			fail += 1
		}
	}

	return pass, fail
}

func TestDel(t *testing.T) {
	client, err := newTestClient()
	if err != nil {
		t.Fatalf("creating client err=%v", err)
	}
	defer client.Close()

	table := uuid.NewRandom().String()
	var otherTables []interface{}
	for i := 0; i < 3; i++ {
		otherTables = append(otherTables, uuid.NewRandom().String())
	}

	cleanup := func() error {
		_, err := client.Del(table, otherTables...)
		return err
	}
	defer cleanup()

	for i := 0; i < 10; i++ {
		strKey := fmt.Sprintf("%d", i)
		if _, err := client.HSet(table, strKey, strKey); err != nil {
			_ = cleanup()
			t.Fatalf("#%d: failed to HSet: %v", i, err)
		}
	}
}

func TestHMove(t *testing.T) {
	client, err := newTestClient()
	if err != nil {
		t.Fatalf("creating client err=%v", err)
	}

	table1, table2 := uuid.NewRandom().String(), uuid.NewRandom().String()
	defer func() {
		// cleanUp Phase
		for _, tableName := range []string{table1, table2} {
			passes, fails := clearTable(client, tableName)
			t.Logf("ClearTable: Passes=%v Fails=%v", passes, fails)
		}

		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	kvpMap := map[interface{}]interface{}{
		"abcABC123FoxTrot": 123.456,
		'f':                "f61bbc2e-a5df-4b49-8156-faef5fe6f3ba",
		1024:               1 << 20,
		2048:               2 << 20,
	}

	// Add all keys to the first table, test for presence in that table
	// then remove keys from that table, transfer them to the alternate table
	// rinse and repeat

	for i := 0; i < 16; i++ {
		primary, secondary := table1, table2
		if i%2 == 0 {
			primary, secondary = table2, table1
		}

		for key, value := range kvpMap {
			_, err := client.HSet(primary, key, []byte(fmt.Sprintf("%v", value)))
			if err != nil {
				t.Errorf("#%d: err=%v trying to insert <key=%v, value=%v> into primary %s", i, err, key, value, primary)
			}
		}

		for key, liveValue := range kvpMap {
			retrValue, err := client.HMove(primary, secondary, key)
			if err != nil {
				t.Errorf("#%d: err=%v for HMove wantErr=nil; primary=%v secondary=%v", i, err, primary, secondary)
			}

			// We can only compare by string repr comparsions
			// since the retrieved value was a []byte retrieval
			strReprLive, strReprRetr := fmt.Sprintf("%v", liveValue), fmt.Sprintf("%s", retrValue)
			if strReprLive != strReprRetr {
				t.Errorf("liveValue(%q) retrValue(%q) for key(%v)", strReprLive, strReprRetr, key)
			}
		}

		// Then the cleanup
		clearTable(client, primary)
	}
}

func TestLPopPushLenIndex(t *testing.T) {
	client, err := newTestClient()
	if err != nil {
		t.Fatalf("creating client err=%v", err)
	}
	defer client.Close()

	tableName := uuid.NewRandom().String()
	defer client.Del(tableName)

	tests := [...]struct {
		name string

		init func()
		do   func() error

		wantCount int64
		cleanup   func() error
	}{
		0: {
			name: "lpush test",
			init: func() {
				client.LPush(tableName, "a", 2, 3)
			},
			do: func() error {
				if count, err := client.LLen(tableName); err != nil || count != 3 {
					return fmt.Errorf("err:%v; count=%v", err, count)
				}
				return nil
			},
			cleanup: func() error {
				_, err := client.Del(tableName)
				return err
			},
		},
		1: {
			name: "lpop+llen+lindex test",
			init: func() {
				client.LPush(tableName, "a", 2, 3, "y")
			},
			do: func() error {
				if count, err := client.LLen(tableName); err != nil || count != 4 {
					return fmt.Errorf("err:%v; count=%v", err, count)
				}
				for i := 0; i < 3; i++ {
					if _, err := client.LPop(tableName); err != nil {
						return err
					}
				}
				if count, err := client.LLen(tableName); err != nil || count != 1 {
					return fmt.Errorf("err:%v; count=%v", err, count)
				}
				retr, err := client.LIndex(tableName, 0)
				if err != nil {
					return fmt.Errorf("lindex err: %v", err)
				}
				str := fmt.Sprintf("%s", retr)
				if str != "a" {
					return fmt.Errorf(`got=%q want="a"`, str)
				}
				return nil
			},
			cleanup: func() error {
				_, err := client.Del(tableName)
				return err
			},
		},
	}

	for i, tt := range tests {
		if tt.init != nil {
			tt.init()
		}
		if tt.do != nil {
			if err := tt.do(); err != nil {
				t.Errorf("#%d(%q): do err=%v", i, tt.name, err)
			}
		}
		if tt.cleanup != nil {
			if err := tt.cleanup(); err != nil {
				t.Errorf("#%d(%q): cleanup err=%v", i, tt.name, err)
			}
		}
	}
}

func TestSAddMembersPush(t *testing.T) {
	client, err := newTestClient()
	if err != nil {
		t.Fatalf("creating client err=%v", err)
	}
	defer client.Close()

	tableName := uuid.NewRandom().String()
	defer client.Del(tableName)

	tests := [...]struct {
		name string

		init func()
		do   func() error

		wantCount int64
		cleanup   func() error
	}{
		0: {
			name: "sadd test",
			init: func() {
				client.SAdd(tableName, `"a"`, `2`, `3`, `2`, `3`, `2`, `"a"`)
			},
			do: func() error {
				retr, err := client.SMembers(tableName)
				if err != nil {
					return err
				}
				wantMap := map[interface{}]bool{
					"a": true,
					2:   true,
					3:   true,
				}
				listing := retr.([]interface{})
				gotMap := make(map[interface{}]bool)
				for _, t := range listing {
					gotMap[fmt.Sprintf("%s", t)] = true
				}
				wantBlob, _ := json.Marshal(wantMap)
				gotBlob, _ := json.Marshal(gotMap)
				if !bytes.Equal(wantBlob, gotBlob) {
					return fmt.Errorf("got=%q want=%q", gotBlob, wantBlob)
				}
				return nil
			},
			cleanup: func() error {
				_, err := client.Del(tableName)
				return err
			},
		},
		1: {
			name: "spop+slen test",
			init: func() {
				client.SAdd(tableName, "a", 2, 3, "y")
			},
			do: func() error {
				for i := 0; i < 3; i++ {
					if _, err := client.SPop(tableName); err != nil {
						return err
					}
				}
				retr, err := client.SMembers(tableName)
				if err != nil {
					return err
				}
				members := retr.([]interface{})
				if count := len(members); count != 1 {
					return fmt.Errorf("err:%v; count=%v", err, count)
				}
				return nil
			},
			cleanup: func() error {
				_, err := client.Del(tableName)
				return err
			},
		},
	}

	for i, tt := range tests {
		if tt.init != nil {
			tt.init()
		}
		if tt.do != nil {
			if err := tt.do(); err != nil {
				t.Errorf("#%d(%q): do err=%v", i, tt.name, err)
			}
		}
		if tt.cleanup != nil {
			if err := tt.cleanup(); err != nil {
				t.Errorf("#%d(%q): cleanup err=%v", i, tt.name, err)
			}
		}
	}
}
