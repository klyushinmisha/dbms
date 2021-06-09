package main

import (
	"bufio"
	"dbms/internal/parser"
	"dbms/internal/transfer"
	"dbms/pkg"
	"dbms/pkg/client"
	"flag"
	"fmt"
	"io"
	"os"
)

var (
	host string
	port uint
)

func init() {
	flag.StringVar(&host, "host", "localhost", "DBMS's hostname")
	flag.UintVar(&port, "port", 8080, "DBMS's TCP-port")
}

func createResMsgExtractor() func(res *transfer.Result) string {
	codeMap := map[int]func(res *transfer.Result) string{
		transfer.OkResultCode:    func(_ *transfer.Result) string { return "OK" },
		transfer.ValueResultCode: func(res *transfer.Result) string { return string(res.Value()) },
		transfer.ErrResultCode:   func(res *transfer.Result) string { return res.Error() },
	}
	return func(res *transfer.Result) string {
		return codeMap[res.Type()](res)
	}
}

func printSplash() {
	fmt.Printf(`DBMS (version %s)
Server: %s
Port: %d
`, pkg.Version, host, port)
}

// main is a simple REPL for manual tests
func main() {
	flag.Parse()
	dbClient, err := client.Connect(fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer dbClient.Finalize()
	ext := createResMsgExtractor()
	reader := bufio.NewReader(os.Stdin)
	printSplash()
	for {
		fmt.Print("> ")
		rawStrCmd, _ := reader.ReadString('\n')
		// TODO: process help command
		res, err := dbClient.Exec(rawStrCmd)
		switch err {
		case io.EOF:
			fmt.Println("Server connection has been closed")
			return
		case parser.ErrInvalidCmdStruct:
			fmt.Println(err)
			continue
		case nil:
			// noop
			break
		default:
			fmt.Println(err)
			return
		}
		fmt.Println(ext(res))
	}
}
