package bootstrap

// BootstrapManager is an interface for core runtime initialization and finalization
type CoreBootstrapManager interface {
	Init()
	Finalize()
}

type DefaultCoreBootstrapManager struct {
	cfg *config.CoreConfig
	dataFile *os.File
	logFile *os.File
}

type StorageBootstrap struct {
	txMgr *TxManager
	...
	ErrDBAlreadyExists
	Init() {
		tx := txMgr.InitTx()
		index = bp_tree.NewBPTree(100, bpAdapter.NewBPTreeAdapter(tx))
		index.Init()
	}
}

func (m *DefaultBootstrapManager) Init() {
	dataFile, err := os.OpenFile(cfgLdr.CoreCfg().DataPath(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln(err)
	}

	logFile, err := os.OpenFile(cfgLdr.CoreCfg().LogPath(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln(err)
	}

	coreCfgr := core.NewDefaultDBMSCoreConfigurator(cfgLdr.CoreCfg(), dataFile, logFile)
	// init storage before recovery attempt
	if err = coreCfgr.StrgBtstrp().Init(); err == ErrDBAlreadyExists {
		// run recovery from journal
		coreCfgr.RecMgr().RollForward(coreCfgr.TxMgr())
		log.Printf("Recovered from journal %s", cfgLdr.CoreCfg().LogPath())
	} else if err != nil {
		log.Panic(err)
	} else {
		log.Printf("Initialized storage %s", cfgLdr.CoreCfg().DataPath())
	}
}

func (m *DefaultBootstrapManager) Finalize() {
	finalizeFile(m.dataFile)
	finalizeFile(m.logFile)
}

func finalizeFile(file *os.File) {
	// durability aspect;
	// ensures all fs caches are flushed on disk
	if err := file.Sync(); err != nil {
		log.Panic(err)
	}
	file.Close()
}
