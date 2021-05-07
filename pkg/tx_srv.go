package pkg

import (
	"dbms/pkg/concurrency"
	"dbms/pkg/transaction"
	"log"
	"sync"
	"sync/atomic"
)

const helpSplash = `Commands structure:
Data manipulation commands:
    GET key         - finds value associated with key
    SET key value   - sets value associated with key
    DEL key         - removes value associated with key
Transaction management commands:
    BEGIN SHARED    - starts new transaction with per-operation isolation
    BEGIN EXCLUSIVE - starts new transaction with per-transation isolation
    COMMIT          - commits active transaction
    ABORT           - aborts active transaction
`

const (
	GetCmd    = 0
	SetCmd    = 1
	DelCmd    = 2
	BegShCmd  = 3
	BegExCmd  = 4
	CommitCmd = 5
	AbortCmd  = 6
	HelpCmd   = 7
)

type Args struct {
	key   string
	value []byte
}

type Cmd struct {
	cmdType int
	key     string
	value   []byte
}

func (c *Cmd) Type() int {
	return c.cmdType
}

func (c *Cmd) Args() *Args {
	a := new(Args)
	a.key = c.key
	a.value = c.value
	return a
}

type Result struct {
	value []byte
	err   error
}

type DesctiptorFactory struct {
	counter int64
}

func (f *DesctiptorFactory) GenerateUniqueDescriptor() int {
	// TODO: check ranges
	return int(atomic.AddInt64(&f.counter, 1))
}

type TxServer struct {
	txMgr         *transaction.TransactionManager
	descFact      DesctiptorFactory
	clientTxTable sync.Map
}

func NewTxServer(txMgr *transaction.TransactionManager) *TxServer {
	s := new(TxServer)
	s.txMgr = txMgr
	return s
}

func (s *TxServer) Init() int {
	clientDesc := s.descFact.GenerateUniqueDescriptor()
	var newTx *transaction.Transaction
	s.clientTxTable.Store(clientDesc, newTx)
	return clientDesc
}

func (s *TxServer) Terminate(clientDesc int) {
	if e, found := s.clientTxTable.LoadAndDelete(clientDesc); found {
		tx := e.(*transaction.Transaction)
		if tx != nil {
			tx.Abort()
		}
	} else {
		log.Panic("Can't terminate client's session: session not found")
	}
}

func (s *TxServer) loadTx(clientDesc int) *transaction.Transaction {
	e, found := s.clientTxTable.Load(clientDesc)
	if !found {
		log.Panic("Session for provided client descriptor not found")
	}
	return e.(*transaction.Transaction)
}

func (s *TxServer) runExecutorCommandsInTx(exec func(*Executor), tx *transaction.Transaction, res *Result) {
	defer func() {
		if err := recover(); err == concurrency.ErrTxLockTimeout {
			tx.Abort()
			*res = Result{nil, concurrency.ErrTxLockTimeout}
		} else if err != nil {
			log.Panic(err)
		}
	}()
	exec(NewExecutor(tx))
}

func (s *TxServer) ExecuteCmd(clientDesc int, cmd *Cmd) *Result {
	// TODO: state validate for commands
	switch cmd.Type() {
	case BegShCmd:
		tx := s.loadTx(clientDesc)
		if tx != nil {
			log.Panic("TMP panic: can't open tx in tx")
		}
		s.clientTxTable.Store(clientDesc, s.txMgr.InitTx(concurrency.SharedMode))
		break
	case BegExCmd:
		tx := s.loadTx(clientDesc)
		if tx != nil {
			log.Panic("TMP panic: can't open tx in tx")
		}
		s.clientTxTable.Store(clientDesc, s.txMgr.InitTx(concurrency.ExclusiveMode))
		break
	case CommitCmd:
		tx := s.loadTx(clientDesc)
		if tx == nil {
			log.Panic("TMP panic: can't commit when no tx")
		}
		tx.Commit()
		var newTx *transaction.Transaction
		s.clientTxTable.Store(clientDesc, newTx)
		break
	case AbortCmd:
		tx := s.loadTx(clientDesc)
		if tx == nil {
			log.Panic("TMP panic: can't abort when no tx")
		}
		tx.Abort()
		var newTx *transaction.Transaction
		s.clientTxTable.Store(clientDesc, newTx)
		break
	case HelpCmd:
		res := new(Result)
		res.value = []byte(helpSplash)
		return res
	default:
		tx := s.loadTx(clientDesc)
		res := new(Result)
		txRes := new(Result)
		func() {
			if tx == nil {
				tx = s.txMgr.InitTx(concurrency.SharedMode)
				defer tx.Commit()
			}
			s.runExecutorCommandsInTx(func(e *Executor) {
				res = e.ExecuteCmd(cmd)
				log.Print(res)
			}, tx, txRes)
		}()
		if txRes.err != nil {
			tx.Abort()
			var newTx *transaction.Transaction
			s.clientTxTable.Store(clientDesc, newTx)
			return txRes
		}
		return res
	}
	return new(Result)
}
