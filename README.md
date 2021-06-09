# DBMS
Key-value database management system server

## Main features
* Simple key-value command interface (GET, SET, DEL)
* ACID transaction management (concurrency control via 2PL, write-ahead logging)
* Exclusive (per transaction; READ COMMITTED equivalent) and shared (per operation; READ UNCOMMITTED equivalent) locking
* Uses simple plaintext protocol to send commands from remote

## Testing

Main parts of code are covered with smoke system tests and benchmarks

## Workflow example
Server:

```
$ go run cmd/server/main.go
2021/06/09 15:49:33 

__/\\\\\\\\\\\\_____/\\\\\\\\\\\\\____/\\\\____________/\\\\_____/\\\\\\\\\\\___        
 _\/\\\////////\\\__\/\\\/////////\\\_\/\\\\\\________/\\\\\\___/\\\/////////\\\_       
  _\/\\\______\//\\\_\/\\\_______\/\\\_\/\\\//\\\____/\\\//\\\__\//\\\______\///__      
   _\/\\\_______\/\\\_\/\\\\\\\\\\\\\\__\/\\\\///\\\/\\\/_\/\\\___\////\\\_________     
    _\/\\\_______\/\\\_\/\\\/////////\\\_\/\\\__\///\\\/___\/\\\______\////\\\______    
     _\/\\\_______\/\\\_\/\\\_______\/\\\_\/\\\____\///_____\/\\\_________\////\\\___   
      _\/\\\_______/\\\__\/\\\_______\/\\\_\/\\\_____________\/\\\__/\\\______\//\\\__  
       _\/\\\\\\\\\\\\/___\/\\\\\\\\\\\\\/__\/\\\_____________\/\\\_\///\\\\\\\\\\\/___ 
        _\////////////_____\/////////////____\///______________\///____\///////////_____

                    DBMS (version 0.0.1) - key-value database management system server


2021/06/09 15:49:33 Initialized storage /home/mikhail/Projects/Go/dbms/data.bin
2021/06/09 15:49:33 Recovered from journal segment /home/mikhail/Projects/Go/dbms/log/segment1.bin
2021/06/09 15:49:33 Server is up on port 8080
2021/06/09 15:49:41 Accepted connection with host 127.0.0.1:36674
2021/06/09 15:49:55 Release connection with host 127.0.0.1:36674
2021/06/09 15:50:01 Accepted connection with host 127.0.0.1:36688
2021/06/09 15:50:56 Release connection with host 127.0.0.1:36688
```

Client:

```
$ go run cmd/client/main.go -host=127.0.0.1 -port=8080
DBMS (version 0.0.1)
Server: 127.0.0.1
Port: 8080
> HELP
Commands structure:
Data manipulation commands:
        GET key         - finds value associated with key
        SET key value   - sets value associated with key
        DEL key         - removes value associated with key
Transaction management commands:
        BEGIN SHARED    - starts new transaction with per-operation isolation
        BEGIN EXCLUSIVE - starts new transaction with per-transation isolation
        COMMIT          - commits active transaction
        ABORT           - aborts active transaction
> BEGIN EXCLUSIVE
OK
> SET key value
OK
> COMMIT
OK
> GET key
value
> BEGIN EXCLUSIVE      
OK
> SET key new-value
OK
> ABORT
OK
> GET key
value
```