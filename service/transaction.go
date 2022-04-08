package service

import (
	"bytepower_room/commands"
	"sync"

	"github.com/tidwall/redcon"
)

var transactionManager = TransactionManager{
	connTransMap: make(map[redcon.Conn]*commands.Transaction),
	mutex:        &sync.Mutex{},
}

type TransactionManager struct {
	connTransMap map[redcon.Conn]*commands.Transaction
	mutex        *sync.Mutex
}

func (manager *TransactionManager) addTransaction(conn redcon.Conn, tx *commands.Transaction) {
	manager.mutex.Lock()
	oldTx := manager.connTransMap[conn]
	delete(manager.connTransMap, conn)
	manager.connTransMap[conn] = tx
	conn.SetTxStatus(true)
	manager.mutex.Unlock()
	if oldTx != nil {
		oldTx.Close(commands.TransactionCloseReasonReset)
	}
}

func (manager *TransactionManager) getTransaction(conn redcon.Conn) *commands.Transaction {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return manager.connTransMap[conn]
}

func (manager *TransactionManager) removeTransaction(conn redcon.Conn, reason commands.TransactionCloseReason) {
	manager.mutex.Lock()
	tx := manager.connTransMap[conn]
	delete(manager.connTransMap, conn)
	conn.SetTxStatus(false)
	manager.mutex.Unlock()
	if tx != nil {
		tx.Close(reason)
	}
}

func (manager *TransactionManager) transactionCount() int {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return len(manager.connTransMap)
}
