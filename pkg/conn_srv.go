package pkg

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

type ConnServer struct {
	parser Parser
	txSrv  *TxServer
}

func NewConnServer(parser Parser, txSrv *TxServer) *ConnServer {
	s := new(ConnServer)
	s.parser = parser
	s.txSrv = txSrv
	return s
}

const splash = `

__/\\\\\\\\\\\\_____/\\\\\\\\\\\\\____/\\\\____________/\\\\_____/\\\\\\\\\\\___        
 _\/\\\////////\\\__\/\\\/////////\\\_\/\\\\\\________/\\\\\\___/\\\/////////\\\_       
  _\/\\\______\//\\\_\/\\\_______\/\\\_\/\\\//\\\____/\\\//\\\__\//\\\______\///__      
   _\/\\\_______\/\\\_\/\\\\\\\\\\\\\\__\/\\\\///\\\/\\\/_\/\\\___\////\\\_________     
    _\/\\\_______\/\\\_\/\\\/////////\\\_\/\\\__\///\\\/___\/\\\______\////\\\______    
     _\/\\\_______\/\\\_\/\\\_______\/\\\_\/\\\____\///_____\/\\\_________\////\\\___   
      _\/\\\_______/\\\__\/\\\_______\/\\\_\/\\\_____________\/\\\__/\\\______\//\\\__  
       _\/\\\\\\\\\\\\/___\/\\\\\\\\\\\\\/__\/\\\_____________\/\\\_\///\\\\\\\\\\\/___ 
        _\////////////_____\/////////////____\///______________\///____\///////////_____

        DBMS - key-value database management system server (type HELP or cry for help)


`

func (s *ConnServer) Serve(conn net.Conn) {
	desc := s.txSrv.Init()
	defer s.txSrv.Terminate(desc)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	writer.Write([]byte(splash))
	writer.Flush()
	for {
		strCmd, err := reader.ReadString('\n')
		log.Print(strings.TrimSpace(strCmd))
		if err != nil {
			log.Panic(err)
		}
		cmd, parseErr := s.parser.Parse(strings.TrimSpace(strCmd))
		var resp string
		if parseErr != nil {
			resp = fmt.Sprintf("%s", parseErr)
		} else {
			res := s.txSrv.ExecuteCmd(desc, cmd)
			if res.err != nil {
				resp = fmt.Sprintf("%s", res.err)
			} else if res.value != nil {
				resp = string(res.value)
			} else {
				resp = "OK"
			}
		}
		if _, writeErr := writer.Write([]byte(fmt.Sprintf("%s\n", resp))); writeErr != nil {
			log.Panic(writeErr)
		}
		writer.Flush()
	}
}
