package pkg

import (
	"dbms/pkg/access/bp_tree"
	bpAdapter "dbms/pkg/storage/adapters/bp_tree"
	dataAdapter "dbms/pkg/storage/adapters/data"
	"dbms/pkg/transaction"
	"log"
)

const (
	GetCommand = 0
	SetCommand = 1
	DelCommand = 2

	BeginCommand = 3
	CommitCommand = 4
	AbortCommand = 5
)

type Executor struct {
	tx    *transaction.Transaction
	index *bp_tree.BPTree
	da    *dataAdapter.DataAdapter
	commandsMap map[int]func(args *Args)*Result
}

type Args struct {
	key string
	value []byte
}

type Result struct {
	txId int
	value []byte
	ok bool
}

func NewExecutor(tx *transaction.Transaction) *Executor {
	e := new(Executor)
	e.tx = tx
	e.index = bp_tree.NewBPTree(100, bpAdapter.NewBPTreeAdapter(tx))
	e.da = dataAdapter.NewDataAdapter(tx)
	e.commandsMap = map[int]func(args *Args)*Result{
		GetCommand: e.getCommand,
		SetCommand: e.setCommand,
		DelCommand: e.delCommand,
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
	return res.value, res.ok
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
	res := e.delCommand(args)
	return nil, res.ok
}

func (e *Executor) ExecuteCommand(cmdType int, args *Args) *Result {
	return e.commandsMap[cmdType](args)
}

func (e *Executor) getCommand(args *Args) *Result {
	defer e.tx.DowngradeLocks()
	res := new(Result)
	pos, findErr := e.index.Find(args.key)
	if findErr == bp_tree.ErrKeyNotFound {
		res.ok = false
		return res
	} else if findErr != nil {
		log.Panic(findErr)
	}
	data, findErr := e.da.FindAtPos(args.key, pos)
	if findErr != nil {
		log.Panic(findErr)
	}
	res.value = data
	res.ok = true
	return res
}

func (e *Executor) setCommand(args *Args) *Result {
	defer e.tx.DowngradeLocks()
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
	return nil
}

func (e *Executor) delCommand(args *Args) *Result {
	defer e.tx.DowngradeLocks()
	res := new(Result)
	pos, err := e.index.Delete(args.key)
	if err == bp_tree.ErrKeyNotFound {
		res.ok = false
		return res
	}
	if delErr := e.da.DeleteAtPos(args.key, pos); delErr != nil {
		log.Panic(delErr)
	}
	res.ok = true
	return res
}
