package server

import (
	"bufio"
	"context"
	"dbms/pkg/config"
	"fmt"
	"golang.org/x/sync/semaphore"
	"io"
	"log"
	"net"
	"strings"
)

type ConnServer struct {
	cfg    *config.ServerConfig
	parser Parser
	txSrv  *TxServer
}

func NewConnServer(cfg *config.ServerConfig, parser Parser, txSrv *TxServer) *ConnServer {
	s := new(ConnServer)
	s.cfg = cfg
	s.parser = parser
	s.txSrv = txSrv
	return s
}

const clientSplash = `

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

func (s *ConnServer) Run() {
	ln, err := net.Listen(s.cfg.TransportProtocol, fmt.Sprintf(":%d", s.cfg.Port))
	defer ln.Close()
	if err != nil {
		log.Panic(err)
	}
	log.Printf("Server is up on port %d", s.cfg.Port)
	sem := semaphore.NewWeighted(int64(s.cfg.MaxConnections))
	ctx := context.TODO()
	for {
		// acquire weighted semaphore to reduce concurrency
		sem.Acquire(ctx, 1)
		conn, err := ln.Accept()
		if err != nil {
			log.Panic(err)
		}
		log.Printf("Accepted connection with host %s", conn.RemoteAddr())
		go func() {
			defer func() {
				log.Printf("Release connection with host %s", conn.RemoteAddr())
				sem.Release(1)
			}()
			s.serve(conn)
		}()
	}
}

func (s *ConnServer) serve(conn net.Conn) {
	desc := s.txSrv.Init()
	defer s.txSrv.Terminate(desc)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	writer.Write([]byte(clientSplash))
	writer.Flush()
	for {
		strCmd, err := reader.ReadString('\n')
		if err == io.EOF {
			return
		} else if err != nil {
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
