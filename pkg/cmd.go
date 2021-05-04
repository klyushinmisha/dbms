package pkg

const (
	ManipulationGroup = 0
	TransactionGroup  = 1
)

const (
	// manipulation commands
	GetCmd = 0b01
	SetCmd = 0b10
	DelCmd = 0b11
	// transaction commands
	BeginSharedCmd  = 0b100
	BeginExclusiveCmd  = 0b101
	CommitCmd = 0b110
	AbortCmd  = 0b111
)

type ExecutorCommand struct {
	cmdType byte
	key     []byte
	value   []byte
}

func (c *ExecutorCommand) Group() int {
	return (int(c.cmdType) & 0b100) >> 2
}

func (c *ExecutorCommand) Type() int {
	return int(c.cmdType)
}

func (c *ExecutorCommand) Args() *Args {
	return &Args{
		key: string(c.key),
		value: value
	}
}

func (c *ExecutorCommand) TerminatesProcessing() bool {
	return c.Type() == CommitCmd || c.Type() == AbortCmd
}

func (c *ExecutorCommand) UnmarshalBinary(data []byte) error {
	reader := bytes.NewReader(data)
	if err := binary.Read(reader, binary.LittleEndian, &c.cmdType); err != nil {
		return err
	}
	if c.Group() == TransactionCmd {
		return nil
	}
	bytesLen := int32(0)
	if err := binary.Read(reader, binary.LittleEndian, &bytesLen); err != nil {
		return err
	}
	c.key = make([]byte, bytesLen, bytesLen)
	if _, err := reader.Read(c.key); err != nil {
		return err
	}
	if c.Type() != SetCmd {
		return nil
	}
	if err := binary.Read(reader, binary.LittleEndian, &bytesLen); err != nil {
		return err
	}
	c.value = make([]byte, bytesLen, bytesLen)
	if _, err := reader.Read(c.key); err != nil {
		return err
	}
	return nil
}
