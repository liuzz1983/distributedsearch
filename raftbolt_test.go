package main

import "testing"
import "io/ioutil"
import "os"

func Test(t *testing.T) {
	tmp, err := ioutil.TempFile("", "temp-file")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer os.Remove(tmp.Name())

	store, err := NewBoltStore(tmp.Name())
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	store.Set([]byte("key"), []byte("value"))

}
