package pkg

import (
	"dbms/pkg/concurrency"
	"log"
	"sync"
	"sync/atomic"
)

/*

type DesctiptorFactory struct {
	counter int64
}

func (f *DesctiptorFactory) Generate() int {
	// TODO: check ranges
	return int(atomic.AddInt64(&f.counter, 1))
}

var connToClientDesc = map[*net.Conn]

func handleConnection(conn *net.Conn) {
	// store or generate new descriptor
	// lock
	clientDesc, found := connToClientDesc[conn]
	if !found {
		clientDesc = descFact.Generate()
		connToClientDesc[conn] = clientDesc
	}
	// unlock
	TxServer.Init(clientDesc)
	defer TxServer.Terminate(clientDesc)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		strCmd, err := reader.ReadString('\n')
		if err != nil {
			log.Panic(err)
		}
		cmd := CommandParser.Parse(strCmd)
		res := TxServer.ExecuteCmd(cmd)
		writer.Write(marshalResult(res))
	}
}

ln, err := net.Listen("tcp", ":8080")
if err != nil {
	// handle error
}
for {
	conn, err := ln.Accept()
	if err != nil {
		// handle error
	}
	go handleConnection(conn)
}

*/

const (
	GetCmd    = 0
	SetCmd    = 1
	DelCmd    = 2
	BegShCmd  = 3
	BegExCmd  = 4
	CommitCmd = 5
	AbortCmd  = 6
)

type Cmd struct {
	cmdType int
	key     string
	value   []byte
}

type Result struct {
	ok    bool
	value []byte
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
	descFact      *DesctiptorFactory
	clientTxTable sync.Map
}

func NewTxServer() *TxServer {
	s := new(TxServer)
	s.descFact = new(DesctiptorFactory)
}

func (s *TxServer) Init() int {
	clientDesc := descFact.GenerateUniqueDescriptor()
	s.clientTxTable.Store(clientDesc, nil.(*transaction.Transaction))
	return clientDesc
}

func (s *TxServer) Terminate(clientDesc int) {
	if e, found := s.clientTxTable.LoadAndDelete(clientDesc); found {
		e.Value.(*transaction.Transaction).Abort()
	} else {
		log.Panic("Can't terminate client's session: session not found")
	}
}

func (s *TxServer) loadTx(clientDesc int) *transaction.Transaction {
	e, found := s.clientTxTable.Load(clientDesc)
	if !found {
		log.Panic("Session for provided client descriptor not found")
	}
	return e.Value.(*transaction.Transaction)
}

func (s *TxServer) runExecutorCommandsInTx(exec func(*Executor) *Result, tx *transaction.Transaction) (res *Result) {
	defer func() {
		if err := recover(); err == concurrency.ErrTxLockTimeout {
			tx.Abort()
		} else if err != nil {
			log.Panic(err)
		}
	}()
	return exec(NewExecutor(tx))
}

func (s *TxServer) ExecuteCmd(clientDesc int, cmd Cmd) *Result {
	// TODO: state validate for commands
	switch cmd.Type() {
	case BegShCmd:
		if _, found := s.clientTxTable.LoadOrStore(clientDesc, txMgr.InitTx(concurrency.SharedMode)); found {
			log.Panic("TMP panic: can't open tx in tx")
		}
		break
	case BegExCmd:
		if _, found := s.clientTxTable.LoadOrStore(clientDesc, txMgr.InitTx(concurrency.ExclusiveMode)); found {
			log.Panic("TMP panic: can't open tx in tx")
		}
		break
	case CommitCmd:
		tx := s.loadTx(clientDesc)
		if tx == nil {
			log.Panic("TMP panic: can't commit when no tx")
		}
		tx.Commit()
		s.clientTxTable.Store(clientDesc, nil.(*transaction.Transaction))
		break
	case AbortCmd:
		tx := s.loadTx(clientDesc)
		if tx == nil {
			log.Panic("TMP panic: can't abort when no tx")
		}
		tx.Abort()
		s.clientTxTable.Store(clientDesc, nil.(*transaction.Transaction))
		break
	default:
		tx := s.loadTx(clientDesc)
		res = new(Result)
		func() {
			if tx == nil {
				tx = txMgr.InitTx(concurrency.SharedMode)
				defer tx.Commit()
			}
			s.runExecutorCommandsInTx(func(e *Executor) {
				res = e.ExecuteCmd(cmd)
			}(), tx)
		}()
		return res
	}
	return nil
}
