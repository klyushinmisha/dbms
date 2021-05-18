package server

import (
	"dbms/pkg/core/access/bp_tree"
	"dbms/pkg/core/concurrency"
	bpAdapter "dbms/pkg/core/storage/adapters/bp_tree"
	dataAdapter "dbms/pkg/core/storage/adapters/data"
	"log"
)

type Command func() *Result

type CommandFactory struct {
	txProxy *TxProxy
}

func NewCommandFactory(txProxy *TxProxy) *CommandFactory {
	f := new(CommandFactory)
	f.txProxy = txProxy
	return f
}

func (f *CommandFactory) Create(cmd *Cmd) Command {
	switch cmd.Type() {
	case BegShCmd:
		return NewBeginCommand(f.txProxy, concurrency.SharedMode)
	case BegExCmd:
		return NewBeginCommand(f.txProxy, concurrency.ExclusiveMode)
	case CommitCmd:
		return NewCommitCommand(f.txProxy)
	case AbortCmd:
		return NewAbortCommand(f.txProxy)
	case HelpCmd:
		return NewHelpCommand()
	default:
		return NewDataManipulationCommand(f.txProxy, cmd)
	}
}

func NewBeginCommand(txProxy *TxProxy, mode int) Command {
	return func() *Result {
		res := new(Result)
		if err := txProxy.Init(mode); err != nil {
			res.err = err
		}
		return res
	}
}

func NewCommitCommand(txProxy *TxProxy) Command {
	return func() *Result {
		txProxy.Commit()
		return new(Result)
	}
}

func NewAbortCommand(txProxy *TxProxy) Command {
	return func() *Result {
		txProxy.Abort()
		return new(Result)
	}
}

func NewHelpCommand() Command {
	return func() *Result {
		res := new(Result)
		res.value = []byte(`Commands structure:
Data manipulation commands:
	GET key         - finds value associated with key
	SET key value   - sets value associated with key
	DEL key         - removes value associated with key
Transaction management commands:
	BEGIN SHARED    - starts new transaction with per-operation isolation
	BEGIN EXCLUSIVE - starts new transaction with per-transation isolation
	COMMIT          - commits active transaction
	ABORT           - aborts active transaction
`)
		return res
	}
}

type DataManipulationCommand struct {
	txProxy     *TxProxy
	cmd         *Cmd
	res         *Result
	index       *bp_tree.BPTree
	da          *dataAdapter.DataAdapter
	commandsMap map[int]func(args *Args)
}

func NewDataManipulationCommand(txProxy *TxProxy, cmd *Cmd) Command {
	c := new(DataManipulationCommand)
	c.txProxy = txProxy
	c.cmd = cmd
	c.res = new(Result)
	c.commandsMap = map[int]func(args *Args){
		GetCmd: c.getCommand,
		SetCmd: c.setCommand,
		DelCmd: c.delCommand,
	}
	return c.execute
}

func (c *DataManipulationCommand) execute() *Result {
	if c.txProxy.Tx() == nil {
		c.txProxy.Init(concurrency.SharedMode)
		defer c.txProxy.Commit()
	}
	c.index = bp_tree.NewBPTree(100, bpAdapter.NewBPTreeAdapter(c.txProxy.Tx()))
	c.da = dataAdapter.NewDataAdapter(c.txProxy.Tx())
	defer func() {
		if err := recover(); err == concurrency.ErrTxLockTimeout {
			c.txProxy.Abort()
			*c.res = Result{nil, concurrency.ErrTxLockTimeout}
		} else if err != nil {
			log.Panic(err)
		}
	}()
	c.commandsMap[c.cmd.Type()](c.cmd.Args())
	return c.res
}

func (c *DataManipulationCommand) getCommand(args *Args) {
	defer c.txProxy.Tx().DowngradeLocks()
	pos, findErr := c.index.Find(args.key)
	if findErr == bp_tree.ErrKeyNotFound {
		c.res.err = bp_tree.ErrKeyNotFound
		return
	} else if findErr != nil {
		log.Panic(findErr)
	}
	data, findErr := c.da.FindAtPos(args.key, pos)
	if findErr != nil {
		log.Panic(findErr)
	}
	c.res.value = data
}

func (c *DataManipulationCommand) setCommand(args *Args) {
	defer c.txProxy.Tx().DowngradeLocks()
	pos, findErr := c.index.Find(args.key)
	if findErr == nil {
		if writeErr := c.da.WriteAtPos(args.key, args.value, pos); writeErr != nil {
			log.Panic(writeErr)
		}
	} else if findErr == bp_tree.ErrKeyNotFound {
		writePos, writeErr := c.da.Write(args.key, args.value)
		if writeErr != nil {
			log.Panic(writeErr)
		}
		c.index.Insert(args.key, writePos)
	} else {
		log.Panic(findErr)
	}
}

func (c *DataManipulationCommand) delCommand(args *Args) {
	defer c.txProxy.Tx().DowngradeLocks()
	pos, err := c.index.Delete(args.key)
	if err == bp_tree.ErrKeyNotFound {
		c.res.err = bp_tree.ErrKeyNotFound
		return
	}
	if delErr := c.da.DeleteAtPos(args.key, pos); delErr != nil {
		log.Panic(delErr)
	}
}
