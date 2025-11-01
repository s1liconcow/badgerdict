package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"

import (
	"errors"
	"os"
	"sync"
	"unsafe"

	"github.com/dgraph-io/badger/v4"
)

var (
	handleMu  sync.RWMutex
	handles           = make(map[uintptr]*badger.DB)
	nextID    uintptr = 1
	errorMu   sync.Mutex
	lastError string
)

func setError(err error) C.int {
	errorMu.Lock()
	defer errorMu.Unlock()
	if err != nil {
		lastError = err.Error()
		return -1
	}
	lastError = ""
	return 0
}

func storeHandle(db *badger.DB) uintptr {
	handleMu.Lock()
	defer handleMu.Unlock()
	id := nextID
	nextID++
	handles[id] = db
	return id
}

func getHandle(id uintptr) (*badger.DB, error) {
	handleMu.RLock()
	defer handleMu.RUnlock()
	db, ok := handles[id]
	if !ok {
		return nil, errors.New("invalid handle")
	}
	return db, nil
}

func deleteHandle(id uintptr) {
	handleMu.Lock()
	defer handleMu.Unlock()
	delete(handles, id)
}

//export Open
func Open(path *C.char, inMemory C.int) C.uintptr_t {
	goPath := C.GoString(path)
	if inMemory != 0 {
		goPath = ""
	}

	var opts badger.Options
	if goPath == "" {
		opts = badger.DefaultOptions("").WithInMemory(true)
	} else {
		_ = os.MkdirAll(goPath, 0o755)
		opts = badger.DefaultOptions(goPath)
	}

	db, err := badger.Open(opts)
	if err != nil {
		setError(err)
		return 0
	}

	setError(nil)
	return C.uintptr_t(storeHandle(db))
}

//export Close
func Close(handle C.uintptr_t) C.int {
	db, err := getHandle(uintptr(handle))
	if err != nil {
		return setError(err)
	}
	if err := db.Close(); err != nil {
		return setError(err)
	}
	deleteHandle(uintptr(handle))
	return setError(nil)
}

//export Set
func Set(handle C.uintptr_t, key *C.char, keyLen C.int, value *C.char, valueLen C.int) C.int {
	db, err := getHandle(uintptr(handle))
	if err != nil {
		return setError(err)
	}
	gotKey := C.GoBytes(unsafe.Pointer(key), keyLen)
	gotValue := C.GoBytes(unsafe.Pointer(value), valueLen)
	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set(gotKey, gotValue)
	})
	return setError(err)
}

//export Get
func Get(handle C.uintptr_t, key *C.char, keyLen C.int, valueLen *C.int) *C.char {
	db, err := getHandle(uintptr(handle))
	if err != nil {
		setError(err)
		return nil
	}
	gotKey := C.GoBytes(unsafe.Pointer(key), keyLen)

	var data []byte
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(gotKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			data = append([]byte(nil), val...)
			return nil
		})
	})
	if err != nil {
		setError(err)
		return nil
	}

	size := len(data)
	if size == 0 {
		buf := C.malloc(1)
		if buf == nil {
			setError(errors.New("malloc failed"))
			return nil
		}
		*valueLen = 0
		setError(nil)
		return (*C.char)(buf)
	}

	buf := C.malloc(C.size_t(size))
	if buf == nil {
		setError(errors.New("malloc failed"))
		return nil
	}

	copy(((*[1 << 30]byte)(unsafe.Pointer(buf)))[:size:size], data)
	*valueLen = C.int(size)
	setError(nil)
	return (*C.char)(buf)
}

//export Delete
func Delete(handle C.uintptr_t, key *C.char, keyLen C.int) C.int {
	db, err := getHandle(uintptr(handle))
	if err != nil {
		return setError(err)
	}
	gotKey := C.GoBytes(unsafe.Pointer(key), keyLen)
	err = db.Update(func(txn *badger.Txn) error {
		return txn.Delete(gotKey)
	})
	return setError(err)
}

//export Sync
func Sync(handle C.uintptr_t) C.int {
	db, err := getHandle(uintptr(handle))
	if err != nil {
		return setError(err)
	}
	return setError(db.Sync())
}

//export LastError
func LastError() *C.char {
	errorMu.Lock()
	defer errorMu.Unlock()
	if lastError == "" {
		return nil
	}
	return C.CString(lastError)
}

//export FreeCString
func FreeCString(str *C.char) {
	if str != nil {
		C.free(unsafe.Pointer(str))
	}
}

//export FreeBuffer
func FreeBuffer(buf *C.char) {
	if buf != nil {
		C.free(unsafe.Pointer(buf))
	}
}

func main() {}
