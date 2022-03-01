package main

import (
	"io"
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tidwall/uhaha"
)

func main() {
	// Choose where your leveldb will exist on the server.
	path := "data.db"

	// Open a blank representation of the data.
	db, err := loadData(path, nil)
	if err != nil {
		panic(err)
	}
	// Configure the uhaha server.
	var c uhaha.Config
	// Set the initial data to point to the blank database.
	c.InitialData = db

	// Assign the Snapshot and Restore functions so
	// the Raft log does not grow unbound.
	c.Snapshot = saveToSnapshot
	c.Restore = func(rd io.Reader) (data interface{}, err error) {
		return loadData(path, rd)
	}

	// Create your commands.
	c.AddWriteCommand("SET", cmdSET)
	c.AddReadCommand("GET", cmdGET)

	// Start the server.
	uhaha.Main(c)
}

func cmdSET(m uhaha.Machine, args []string) (interface{}, error) {
	db := m.Data().(*leveldb.DB)
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}
	err := db.Put([]byte(args[1]), []byte(args[2]), nil)
	if err != nil {
		return nil, err
	}
	return "OK", nil
}

func cmdGET(m uhaha.Machine, args []string) (interface{}, error) {
	db := m.Data().(*leveldb.DB)
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}
	val, err := db.Get([]byte(args[1]), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

type mySnapshot struct {
	snap *leveldb.Snapshot
}

func (s *mySnapshot) Persist(w io.Writer) error {
	// This happens in the background so there's no blocking the Uhaha
	// application. Incoming connections can use commands while persisting.

	// Create a new snapshot iterator.
	iter := s.snap.NewIterator(nil, nil)
	defer func() {
		iter.Release()
		s.snap.Release()
	}()

	for iter.Next() {
		if _, err := w.Write(iter.Key()); err != nil {
			return err
		}
		if _, err := w.Write(iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (s *mySnapshot) Done(string) {
	// This function does nothing.
}

func saveToSnapshot(data interface{}) (uhaha.Snapshot, error) {
	// Save the current leveldb database to a Uhaha snapshot.
	// Leveldb has a handy snapshot utility.
	db := data.(*leveldb.DB)
	snap, err := db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &mySnapshot{snap: snap}, nil
}

func loadData(path string, r io.Reader) (interface{}, error) {
	// Remove the old database. All data is restored
	// from the Raft snapshot.
	if err := os.RemoveAll(path); err != nil {
		return nil, err
	}
	var ok bool
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !ok {
			// There was a problem restoring the database.
			// Close the database to free resources.
			db.Close()
		}
	}()
	if r == nil {
		// A snapshot was not provided. Use a blank database.
		ok = true
		return db, nil
	}
	// Read from the snapshot reader.
	var b leveldb.Batch
	for {
		var key, value []byte
		if _, err := io.ReadFull(r, key); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Add to a batch of up to 256 key/value pairs.
		b.Put(key, value)
		if b.Len() == 256 {
			// Write the batch to disk.
			if err := db.Write(&b, nil); err != nil {
				return nil, err
			}
			// Reset the batch so we can add more pairs.
			b.Reset()
		}
	}
	// Write the remaining pairs.
	if err := db.Write(&b, nil); err != nil {
		return nil, err
	}
	// The database has now been restored.
	ok = true
	return db, err
}
