package pkg

import (
	"dbms/pkg/access/bp_tree"
	bpAdapter "dbms/pkg/storage/adapters/bp_tree"
	dataAdapter "dbms/pkg/storage/adapters/data"
	"dbms/pkg/transaction"
	"log"
)

type Executor struct {
	tx          *transaction.Tx
	index       *bp_tree.BPTree
	da          *dataAdapter.DataAdapter
	commandsMap map[int]func(args *Args) *Result
}

func NewExecutor(tx *transaction.Tx) *Executor {
	e := new(Executor)
	e.tx = tx
	e.index = bp_tree.NewBPTree(100, bpAdapter.NewBPTreeAdapter(tx))
	e.da = dataAdapter.NewDataAdapter(tx)
	e.commandsMap = map[int]func(args *Args) *Result{
		GetCmd: e.getCommand,
		SetCmd: e.setCommand,
		DelCmd: e.delCommand,
	}
	return e
}

func (e *Executor) Init() {
	e.index.Init()
}

// facade method
func (e *Executor) Get(key string) ([]byte, bool) {
	args := new(Args)
	args.key = key
	res := e.getCommand(args)
	return res.value, true
}

// facade method
func (e *Executor) Set(key string, value []byte) ([]byte, bool) {
	args := new(Args)
	args.key = key
	args.value = value
	e.setCommand(args)
	return nil, true
}

// facade method
func (e *Executor) Delete(key string) ([]byte, bool) {
	args := new(Args)
	args.key = key
	e.delCommand(args)
	return nil, true
}

func (e *Executor) ExecuteCmd(cmd *Cmd) *Result {
	return e.commandsMap[cmd.Type()](cmd.Args())
}

func (e *Executor) getCommand(args *Args) *Result {
	defer e.tx.DowngradeLocks()
	res := new(Result)
	pos, findErr := e.index.Find(args.key)
	if findErr == bp_tree.ErrKeyNotFound {
		res.err = bp_tree.ErrKeyNotFound
		return res
	} else if findErr != nil {
		log.Panic(findErr)
	}
	data, findErr := e.da.FindAtPos(args.key, pos)
	if findErr != nil {
		log.Panic(findErr)
	}
	res.value = data
	return res
}

func (e *Executor) setCommand(args *Args) *Result {
	defer e.tx.DowngradeLocks()
	res := new(Result)
	pos, findErr := e.index.Find(args.key)
	if findErr == nil {
		if writeErr := e.da.WriteAtPos(args.key, args.value, pos); writeErr != nil {
			log.Panic(writeErr)
		}
	} else if findErr == bp_tree.ErrKeyNotFound {
		writePos, writeErr := e.da.Write(args.key, args.value)
		if writeErr != nil {
			log.Panic(writeErr)
		}
		e.index.Insert(args.key, writePos)
	} else {
		log.Panic(findErr)
	}
	return res
}

func (e *Executor) delCommand(args *Args) *Result {
	defer e.tx.DowngradeLocks()
	res := new(Result)
	pos, err := e.index.Delete(args.key)
	if err == bp_tree.ErrKeyNotFound {
		res.err = bp_tree.ErrKeyNotFound
		return res
	}
	if delErr := e.da.DeleteAtPos(args.key, pos); delErr != nil {
		log.Panic(delErr)
	}
	return res
}
