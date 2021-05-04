package server

var txOpsStateTransitions = [][]int{
	
}

const maxRequests = 100
const maxTx = 100

type TransactionServer struct {
	txMgr *transaction.TransactionManager
	executors map[int]*Executor
	cmdQueues map[int]chan *ExecutorCommand
	resultQueues map[int]chan *Result
}

func NewTransactionServer(txMgr *transaction.TransactionManager) *TransactionServer {
	s := new(TransactionServer)
	s.txMgr = txMgr
	// TODO: limit maxTx at runtime by semaphore
	s.executors = make(map[int]*Executor, maxTx)
	s.cmdQueues = make(map[int]chan *ExecutorCommand, maxTx)
	s.resultQueues = make(map[int]chan *Result, maxTx)
}

func (s *TransactionServer) CmdQueue(txId int) map[int]chan<- *ExecutorCommand {

}

func (s *TransactionServer) ResultQueue(txId int) map[int]<-chan *Result {

}

func (s *TransactionServer) InitTx(mode int) int {
	tx := txMgr.InitTx(mode)
	s.executors = NewExecutor(tx)
	s.cmdQueues[tx.Id()] = make(chan *ExecutorCommand, maxRequests)
	s.resultQueues[tx.Id()] = make(chan *Result, maxRequests)
}

func (s *TransactionServer) CleanUpTx(txId int) {
	delete(s.executors, txId)
	// first close command queue to prevent result queue access
	close(s.cmdQueues[txId])
	close(s.resultsQueue[txId])
	delete(s.cmdQueues, txId)
	delete(s.resultsQueue, txId)
}

func (s *TransactionServer) RunCmdQueueProc(txId int) {
	for cmd := range s.cmdQueues[txId] {
		res := ...
		s.resultQueues[txId] <- res
	}
}

/*

conn = ...
cmd := parseCmd(conn.Read()...)
txId := txIdFromConn(conn)
TransactionServer.CmdQueue(txId) <- cmd

case cmd <- TransactionServer.ResultQueue(txId):
	if cmd.RequestsCleanUp() {
		TransactionServer.CleanUpTx(txId)
		// and terminate routine here
		wg.Done()
	}
*/
