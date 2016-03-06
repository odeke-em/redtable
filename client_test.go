package redtable

import (
	"bytes"
	"fmt"
	"os"
	"testing"
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
