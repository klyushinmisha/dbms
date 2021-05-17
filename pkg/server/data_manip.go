package server

import (
	"dbms/pkg/core/access/bp_tree"
	"dbms/pkg/core/concurrency"
	bpAdapter "dbms/pkg/core/storage/adapters/bp_tree"
	dataAdapter "dbms/pkg/core/storage/adapters/data"
	"log"
)

type DataManipulationCommand struct {
	txProxy     *TxProxy
	cmd         *Cmd
	res         *Result
	index       *bp_tree.BPTree
	da          *dataAdapter.DataAdapter
	commandsMap map[int]func(args *Args)
}

func NewDataManipulationCommand(txProxy *TxProxy, cmd *Cmd) *DataManipulationCommand {
	c := new(DataManipulationCommand)
	c.txProxy = txProxy
	c.cmd = cmd
	c.res = new(Result)
	c.commandsMap = map[int]func(args *Args){
		GetCmd: c.getCommand,
		SetCmd: c.setCommand,
		DelCmd: c.delCommand,
	}
	return c
}

func (c *DataManipulationCommand) Execute() *Result {
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
