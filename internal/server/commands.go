package server

import (
	"dbms/internal/core/access/bp_tree"
	"dbms/internal/core/concurrency"
	bpAdapter "dbms/internal/core/storage/adapters/bp_tree"
	dataAdapter "dbms/internal/core/storage/adapters/data"
	"dbms/internal/transfer"
	"log"
	"dbms/internal/parser"
)

type Command func() *transfer.Result

type CommandFactory struct {
	txProxy *TxProxy
}

func NewCommandFactory(txProxy *TxProxy) *CommandFactory {
	f := new(CommandFactory)
	f.txProxy = txProxy
	return f
}

func (f *CommandFactory) Create(cmd transfer.Cmd) Command {
	switch cmd.Type {
	case parser.BegShCmd:
		return createBeginCommand(f.txProxy, concurrency.SharedMode)
	case parser.BegExCmd:
		return createBeginCommand(f.txProxy, concurrency.ExclusiveMode)
	case parser.CommitCmd:
		return createCommitCommand(f.txProxy)
	case parser.AbortCmd:
		return createAbortCommand(f.txProxy)
	case parser.HelpCmd:
		return createHelpCommand()
	default:
		return createDataManipulationCommand(f.txProxy, cmd)
	}
}

func createBeginCommand(txProxy *TxProxy, mode int) Command {
	return func() *transfer.Result {
		if err := txProxy.Init(mode); err != nil {
			return transfer.ErrResult(err)
		}
		return transfer.OkResult()
	}
}

func createCommitCommand(txProxy *TxProxy) Command {
	return func() *transfer.Result {
		txProxy.Commit()
		return transfer.OkResult()
	}
}

func createAbortCommand(txProxy *TxProxy) Command {
	return func() *transfer.Result {
		txProxy.Abort()
		return transfer.OkResult()
	}
}

func createHelpCommand() Command {
	return func() *transfer.Result {
		return transfer.ValueResult([]byte(`Commands structure:
Data manipulation commands:
	GET key         - finds value associated with key
	SET key value   - sets value associated with key
	DEL key         - removes value associated with key
Transaction management commands:
	BEGIN SHARED    - starts new transaction with per-operation isolation
	BEGIN EXCLUSIVE - starts new transaction with per-transation isolation
	COMMIT          - commits active transaction
	ABORT           - aborts active transaction`),
		)
	}
}

type encapsulatedCommand func(args transfer.Args)

type dataManipulationCommandState struct {
	txProxy     *TxProxy
	cmd         transfer.Cmd
	res         *transfer.Result
	index       *bp_tree.BPTree
	da          *dataAdapter.DataAdapter
	commandsMap map[int]encapsulatedCommand
}

func createDataManipulationCommand(txProxy *TxProxy, cmd transfer.Cmd) Command {
	f := new(dataManipulationCommandState)
	f.txProxy = txProxy
	f.cmd = cmd
	f.commandsMap = map[int]encapsulatedCommand{
		parser.GetCmd: f.getCommand,
		parser.SetCmd: f.setCommand,
		parser.DelCmd: f.delCommand,
	}
	return f.execute
}

func (f *dataManipulationCommandState) execute() *transfer.Result {
	if f.txProxy.Tx() == nil {
		f.txProxy.Init(concurrency.SharedMode)
		defer f.txProxy.Commit()
	}
	f.index = bp_tree.NewBPTree(100, bpAdapter.NewBPTreeAdapter(f.txProxy.Tx()))
	f.da = dataAdapter.NewDataAdapter(f.txProxy.Tx())
	defer func() {
		if err := recover(); err == concurrency.ErrTxLockTimeout {
			f.txProxy.Abort()
			*f.res = *transfer.ErrResult(concurrency.ErrTxLockTimeout)
		} else if err != nil {
			log.Panic(err)
		}
	}()
	f.commandsMap[f.cmd.Type](f.cmd.Args)
	return f.res
}

func (f *dataManipulationCommandState) getCommand(args transfer.Args) {
	defer f.txProxy.Tx().DowngradeLocks()
	pos, findErr := f.index.Find(args.Key)
	if findErr == bp_tree.ErrKeyNotFound {
		f.res = transfer.ErrResult(bp_tree.ErrKeyNotFound)
		return
	} else if findErr != nil {
		log.Panic(findErr)
	}
	data, findErr := f.da.FindAtPos(args.Key, pos)
	if findErr != nil {
		log.Panic(findErr)
	}
	f.res = transfer.ValueResult(data)
}

func (f *dataManipulationCommandState) setCommand(args transfer.Args) {
	defer f.txProxy.Tx().DowngradeLocks()
	pos, findErr := f.index.Find(args.Key)
	if findErr == nil {
		if writeErr := f.da.WriteAtPos(args.Key, args.Value, pos); writeErr != nil {
			log.Panic(writeErr)
		}
	} else if findErr == bp_tree.ErrKeyNotFound {
		writePos, writeErr := f.da.Write(args.Key, args.Value)
		if writeErr != nil {
			log.Panic(writeErr)
		}
		f.index.Insert(args.Key, writePos)
	} else {
		log.Panic(findErr)
	}
	f.res = transfer.OkResult()
}

func (f *dataManipulationCommandState) delCommand(args transfer.Args) {
	defer f.txProxy.Tx().DowngradeLocks()
	pos, err := f.index.Delete(args.Key)
	if err == bp_tree.ErrKeyNotFound {
		f.res = transfer.ErrResult(bp_tree.ErrKeyNotFound)
		return
	}
	if delErr := f.da.DeleteAtPos(args.Key, pos); delErr != nil {
		log.Panic(delErr)
	}
	f.res = transfer.OkResult()
}
