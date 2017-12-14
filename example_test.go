package redtable_test

import (
	"log"

	"github.com/odeke-em/redtable"
)

func ExampleMulticonsumer() {
	client, err := redtable.New("redis://localhost:6379")
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			log.Fatal(err)
		}
	}()

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
	for i, kvp := range kvps {
		_, err := client.HSet(tableName, kvp.key, kvp.value)
		if err != nil {
			log.Printf("#%d key:%v value %v failed err %v; comment %s", i, kvp.key, kvp.value, err, kvp.comment)
		}
	}
}
