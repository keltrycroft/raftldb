package main

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tidwall/uhaha"
)

func main() {
	var conf uhaha.Config

	conf.Name = "leveldb"
	db, _ := leveldb.OpenFile("./tmp/leveldb", nil)
	conf.InitialData = db
	conf.UseJSONSnapshots = true

	conf.AddReadCommand("get", cmdGET)
	conf.AddWriteCommand("put", cmdPUT)
	conf.AddWriteCommand("del", cmdDEL)

	uhaha.Main(conf)
}

func cmdGET(m uhaha.Machine, args []string) (interface{}, error) {
	d := m.Data().(*leveldb.DB)
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}
	data, err := d.Get([]byte(args[1]), nil)
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func cmdPUT(m uhaha.Machine, args []string) (interface{}, error) {
	d := m.Data().(*leveldb.DB)
	if len(args) != 3 {
		return nil, uhaha.ErrWrongNumArgs
	}
	err := d.Put([]byte(args[1]), []byte(args[2]), nil)
	if err != nil {
		return nil, err
	}
	return "OK", nil
}

func cmdDEL(m uhaha.Machine, args []string) (interface{}, error) {
	d := m.Data().(*leveldb.DB)
	if len(args) != 2 {
		return nil, uhaha.ErrWrongNumArgs
	}
	err := d.Delete([]byte(args[1]), nil)
	if err != nil {
		return nil, err
	}
	return "OK", nil
}
