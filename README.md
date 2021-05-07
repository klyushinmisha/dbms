# DBMS
Key-value database management system server

## Main features
* Simple key-value command interface (GET, SET, DEL)
* ACID transaction management (concurrency control via 2PL, write-ahead logging)
* Exclusive and shared (prevents from exclusive locking) transactions. Locks acquired per transaction of per operation
* Uses simple plaintext protocol to send commands from remote

## Workflow example
```
$ telnet localhost 8080
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.


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


HELP
Commands structure:
Data manipulation comands:
        GET key         - finds value associated with key
        SET key value   - sets value associated with key
        DEL key         - removes value associated with key
Transaction management commands:
        BEGIN SHARED    - starts new transaction with per-operation isolation
        BEGIN EXCLUSIVE - starts new transaction with per-transation isolation
        COMMIT          - commits active transaction
        ABORT           - aborts active transaction

BEGIN EXCLUSIVE
OK
SET hello world
OK
COMMIT   
OK
GET hello
world
```