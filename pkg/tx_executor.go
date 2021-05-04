package pkg

import (
	"errors"
	"dbms/pkg/transaction"
	"dbms/pkg/concurrency"
	"sync"
)

type TxExecutionManager struct {
	txMgr *transaction.TransactionManager
	executorsModLock sync.Mutex
	executors map[int]*Executor
	cmdQueue map[int]chan *ExecutorCommand
	resultQueue map[int]chan *Result
}

/*

if cmd.Group() == TransactionGroup {
			switch cmd.Type() {
			case BeginSharedCmd:
				txId = TxExecutionManager.Begin(concurrency.SharedMode)
				break
			case BeginExclusiveCmd:
				txId = TxExecutionManager.Begin(concurrency.ExclusiveMode)
				break
			case CommitCmd:
				if txId == -1 {
					log.Panic("Invalid state: no tx to commit")
				}
				TxExecutionManager.Commit(txId)
				break
			case AbortCmd:
				if txId == -1 {
					log.Panic("Invalid state: no tx to abort")
				}
				TxExecutionManager.Abort(txId)
				break
			default:
				log.Panicf("Unknown tx command type: %v", cmd.Type())
			}
		}
		if txId == -1 {
			log.Panic("tx processing not started; TODO: wrap single operations in shared transactions")
		}

*/

/*

1. conn can have a transaction or not
2. when conn opens tx, then map[connId]txId stores given pair
3. when tx is commited or aborted, then pair is removed
4. command queue and result queue is bound to connection, not transaction
5. command contains txId
6. cmd queue proc extracts executor from txId, passes args to specified method
7. result contains txId
8. result is put in results queue after execution

NOTE: single command is wrapped in BEGIN SHARED/COMMIT/ABORT block

TxExecutionManager serves tx and manip requests by multiplexing

*/

func (m *TxExecutionManager) Begin(mode int) int {
	
}

func (m *TxExecutionManager) CmdQueue(txId int) {

}

func (m *TxExecutionManager) RunCmdQueueProc(txId int) {
	for cmd := range m.cmdQueue[txId] {
		// 1. process transactional features create executor, commit or abort (and remove executor deleting it from map)
		//		storing the result in result queue
		// 2. process DML command storing the result in result queue

		/*
		ExecuteCommand()

		
		*/
		var res *Result
		if cmd.Group() == TransactionGroup {
			res = m.processTransactionCmd(cmd)
		} else if cmd.Group() == ManipulationGroup {
			group := m.getCmdGroupExecutor(cmd.Group())
			exec := m.GetCmdExecutor(cmd.Type())
			// resultQueue may panic if closed
			res = exec(cmd.Args())
		}
		resultQueue[txId] <- res
	}
}

func NewTxExecutionManager(txMgr *transaction.TransactionManager) *TxExecutionManager {
	m := new(TxExecutionManager)
	m.txMgr = txMgr
	return m
}

func (m *TxExecutionManager) initExecutor(lockMode int) *Executor {
	return NewExecutor(txMgr.InitTx(lockMode))
}

func (m *TxExecutionManager) LoadOrCreateExecutor(txId int, lockMode int) *Executor {
	m.executorsModLock.Lock()
	defer m.executorsModLock.Unlock()
	e, found := m.executors[txId]
	if !found {
		e = m.initExecutor(lockMode)
	}
}

func (m *TxExecutionManager) Begin() int {
	e, found := m.executors[txId]
	if !found {
		
	}
	e = m.initExecutor(lockMode)
	m.executors[txId]
}

func (m *TxExecutionManager) Commit(txId int) {
	
}

func (m *TxExecutionManager) Abort(txId int) {
	
}

func (m *TxExecutionManager) ExecutorContext(txId int, exec func(*Executor)) error {
	defer func() {
		// handle transaction resources lock timeout
		if err := recover(); err == concurrency.ErrTxLockTimeout {
			tx.Abort()
		}
	}()
	exec(m.LoadOrCreateExecutor(txId))
}

te.ExecuteCommand(cmdType, args)
te.ExecuteTxCommand(cmdType, args)
te.ExecuteDataCommand(cmdType, args)
// facade method
te.Begin()
// facade method
te.Commit()
// facade method
te.Abort()


/*

func ProcessTxCommands() {
	if cmd.Type() == BeginCommand {
		txId = TxExecutionManager.Begin() // runs RunQueueProcessing() for txBackgroundProcessing
	}
	// commit
	// abort
}

// single-thread method
func handleConn(conn) {
	// 2. ON CONNECTION CLOSE CLOSE CHAN; HANDLE INPUT PANIC WITH recover
	txId := -1
	for msg in conn {
		1) get string from msg
		2) parse string as AST-stmt
		3) convert AST-stmt into executor's command
		4) put command in transaction's queue:
		
		TxExecutionManager.CommandQueue(txId) <- cmd
		select {
		case result <- TxExecutionManager.ResultQueue(txId) {
			conn.Send(result...)
			break;
		}
		case <-killChan:
			TxExecutionManager.Abort(txId)
			break;
		}
	}
}

func main() {
	recoveryStuff()
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			// Print the error using a log.Fatal would exit the server
			log.Println(err)
		}
		// Using a go routine to handle the connection
		func(){
			defer conn.Close()???
			go handleConn(conn)
		}()
	}
}

1. ALL QUEUES ARE BUFFERED (to prevent system saturation)

input queue server:
1) get string from request body
2) parse string as AST-stmt
3) convert AST-stmt into executor's command
4) put command in transaction's queue:
TxExecutionManager.CommandQueue(txId) <- cmd


transaction processor:
select {
case cmd <-someTxCommandQueue:
	resultQueue <- executor.ExecuteCommand(cmd)
	break;
case <-killChan:
	break;
}


Query language grammar

<stmt> ::= tx_mgmt_stmt | data_manip_stmt
tx_mgmt_stmt ::= beg_stmt | commit_stmt | abort_stmt
data_manip_stmt ::= get_stmt | set_stmt | del_stmt
beg_stmt ::= BEGIN EXCLUSIVE | BEGIN SHARED
commit_stmt ::= COMMIT
abort_stmt ::= ABORT
get_stmt ::= GET key_tok
set_stmt ::= SET key_tok val_tok
del_stmt ::= DEL key_tok
key_tok ::= UTF-8, some_limit_len
val_tok ::= UTF-8, some_limit_len
*/