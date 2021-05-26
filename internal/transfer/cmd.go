package transfer

type Args struct {
	Key   string
	Value []byte
}

type Cmd struct {
	Type int
	Args
}

// TODO: add marhaler to use commands in client

/*

conn := dbms.Connect("localhost:8080")

conn.Exec(rawCmd)
value, ok := conn.Get(key)
conn.Set(key, value)
ok := conn.Del(key, value)

tx := conn.BeginSh()
tx := conn.BeginEx()

tx...


*/