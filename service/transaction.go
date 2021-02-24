package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/commands"
	"sync"

	"github.com/go-redis/redis/v8"
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
	manager.mutex.Unlock()
	if oldTx != nil {
		if err := oldTx.Close(); err != nil {
			logger := base.GetServerLogger()
			logger.Error(
				"close transaction error",
				log.String("command", "add transaction"),
				log.Error(err),
			)
		}
	}
}

func (manager *TransactionManager) getTransaction(conn redcon.Conn) *commands.Transaction {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return manager.connTransMap[conn]
}

func (manager *TransactionManager) removeTransaction(conn redcon.Conn, reason string) {
	manager.mutex.Lock()
	tx := manager.connTransMap[conn]
	delete(manager.connTransMap, conn)
	manager.mutex.Unlock()
	if tx != nil {
		if err := tx.Close(); err != nil && err != redis.ErrClosed {
			logger := base.GetServerLogger()
			logger.Error(
				"close transaction error",
				log.String("reason", reason),
				log.Error(err),
			)
		}
	}
}

func (manager *TransactionManager) transactionCount() int {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return len(manager.connTransMap)
}
