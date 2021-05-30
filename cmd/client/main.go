package main

import (
	"os"
	"bufio"
	"fmt"
	"dbms/pkg/client"
	"io"
	"dbms/internal/transfer"
	"flag"
	"dbms/pkg"
	"dbms/internal/parser"
)

var (
	host string
	port uint
)


func init() {
	flag.StringVar(&host, "host", "localhost", "DBMS's hostname")
	flag.UintVar(&port, "port", 8080, "DBMS's TCP-port")
}

// simple REPL for manual tests
func main() {
	flag.Parse()
	dbClient, err := client.Connect(fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer dbClient.Finalize()
	reader := bufio.NewReader(os.Stdin)
	// TODO: maybe check major version for server before REPL starts?
	fmt.Printf(`DBMS (version %s)
Server: %s
Port: %d
`, pkg.Version, host, port)
	for {
		var msg string
		fmt.Print("> ")
		rawStrCmd, _ := reader.ReadString('\n')
		res, err := dbClient.Exec(rawStrCmd)
		if err == io.EOF {
			return
		} else if err == parser.ErrInvalidCmdStruct {
			fmt.Println(err)
			continue
		} else if err != nil {
			fmt.Println(err)
			return
		}
		switch res.Type() {
		case transfer.OkResultCode:
			msg = "OK"
			break
		case transfer.ValueResultCode:
			msg = string(res.Value())
			break
		case transfer.ErrResultCode:
			msg = res.Error()
			break
		}
		fmt.Println(msg)
	}
}
