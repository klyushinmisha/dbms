package core

import (
	"dbms/internal/config"
	"dbms/internal/core/access/bp_tree"
	"dbms/internal/core/concurrency"
	bpAdapter "dbms/internal/core/storage/adapters/bp_tree"
	"dbms/pkg"
	"log"
	"os"
)

const serverSplash = `

__/\\\\\\\\\\\\_____/\\\\\\\\\\\\\____/\\\\____________/\\\\_____/\\\\\\\\\\\___        
 _\/\\\////////\\\__\/\\\/////////\\\_\/\\\\\\________/\\\\\\___/\\\/////////\\\_       
  _\/\\\______\//\\\_\/\\\_______\/\\\_\/\\\//\\\____/\\\//\\\__\//\\\______\///__      
   _\/\\\_______\/\\\_\/\\\\\\\\\\\\\\__\/\\\\///\\\/\\\/_\/\\\___\////\\\_________     
    _\/\\\_______\/\\\_\/\\\/////////\\\_\/\\\__\///\\\/___\/\\\______\////\\\______    
     _\/\\\_______\/\\\_\/\\\_______\/\\\_\/\\\____\///_____\/\\\_________\////\\\___   
      _\/\\\_______/\\\__\/\\\_______\/\\\_\/\\\_____________\/\\\__/\\\______\//\\\__  
       _\/\\\\\\\\\\\\/___\/\\\\\\\\\\\\\/__\/\\\_____________\/\\\_\///\\\\\\\\\\\/___ 
        _\////////////_____\/////////////____\///______________\///____\///////////_____

                    DBMS (version %s) - key-value database management system server


`

type BootstrapManager struct {
	cfg      *config.CoreConfig
	factory     DBMSCoreFactory
	strgFile *os.File
}

func NewBootstrapManager(cfg *config.CoreConfig, factory DBMSCoreFactory) *BootstrapManager {
	m := new(BootstrapManager)
	m.cfg = cfg
	m.factory = factory
	return m
}

func (m *BootstrapManager) StrgFile() *os.File {
	return m.strgFile
}

func (m *BootstrapManager) Init() {
	log.Printf(serverSplash, pkg.Version)
	// load log segments
	m.factory.SegMgr().LoadSegments()
	// init storage before recovery attempt
	m.initStorage()
	// run recovery from journal
	m.factory.RecMgr().RollForward(m.factory.TxMgr())
}

func (m *BootstrapManager) Finalize() {
	m.closeStrg()
	m.factory.SegMgr().CloseSegments()
}

func (m *BootstrapManager) openStrg() {
	strgFile, err := os.OpenFile(m.cfg.DataPath(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln(err)
	}
	m.strgFile = strgFile
}

func (m *BootstrapManager) closeStrg() {
	if m.strgFile != nil {
		m.strgFile.Close()
	}
}

func (m *BootstrapManager) initStorage() {
	m.openStrg()
	// now TxMgr can access storage
	tx := m.factory.TxMgr().InitTx(concurrency.ExclusiveMode)
	bp_tree.NewDefaultBPTree(bpAdapter.NewBPTreeAdapter(tx)).Init()
	tx.Commit()
	log.Printf("Initialized storage %s", m.cfg.DataPath())
}
