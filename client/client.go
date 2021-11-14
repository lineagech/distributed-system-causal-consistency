package main

import (
    _ "net"
    "fmt"
    "bufio"
    "os"
    "flag"
    "connect"
    "messages"
    "strings"
    _"strconv"
    _"sort"
    _"math/rand"
    _"log"
    _"sync"
    _"golang.org/x/sys/unix"
    _"io/ioutil"
)

type UserInput int
var PeerName string

const (
    Read UserInput = iota
    Write
    Invalid
)

var ipAddr *string

func cmdArgs() {
    ipAddr = flag.String("ip", "", "ip exposed to the network")
    flag.Parse()
}


func parseUserInput(line string) (UserInput, string, string, string) {
    var res UserInput = Invalid
    var server_addr string
    var key string
    var value string
    if strings.HasPrefix(line, "read") {
        res = Read
    } else if strings.HasPrefix(line, "write") {
        res = Write
    }
    //log.Println("Line ", line)
    split_line := strings.Split(line, " ")
    server_addr = split_line[1]
    key = split_line[2]
    if (res == Read){
        value = ""
    }else{
        value = split_line[3]
    }

    return res, server_addr, key, value
}

func readData(key string, server_addr string){
    for {
        server_addr_split := strings.Split(server_addr, ":")
        conn := connect.ConnectToServer(server_addr_split[0], server_addr_split[1])
        err := connect.SendReadRequest(conn, key, PeerName)
        if err != nil {
            conn.Close()
            continue
        }
        recv_msg, err := connect.RecvMsg(conn, messages.Read_Request)
        if err != nil {
            conn.Close()
            continue
        }
        read_resp := recv_msg.(messages.Read_response_t)

        if read_resp.Read_succ == 1 {
            //log.Printf("%s: Read for key = %d  Succeeded, value is %s\n", PeerName, key, value)
            break
        } else {
            //log.Printf("%s: Read for key = %d  failed\n", PeerName, key)
            continue
        }
    }
}


func writeData(key string, value string, server_addr string){
    for {
        server_addr_split := strings.Split(server_addr, ":")
        conn := connect.ConnectToServer(server_addr_split[0], server_addr_split[1])
        err := connect.SendWriteRequest(conn, key, value, PeerName)
        if err != nil {
            conn.Close()
            continue
        }
        recv_msg, err := connect.RecvMsg(conn, messages.Write_Request)
        if err != nil {
            conn.Close()
            continue
        }
        write_resp := recv_msg.(messages.Write_response_t)

        if write_resp.Write_succ == 1 {
            //log.Printf("%s: Write for key = %d  Succeeded\n", PeerName, key)
            break
        } else {
            //log.Printf("%s: Write for key = %d  failed\n", PeerName, key)
            continue
        }
    }
}


func processClientRequest() {
    var line string
    scanner := bufio.NewScanner(os.Stdin)
    for {
        fmt.Println("------ Input the request ------")
        fmt.Println("")
        fmt.Println("")
        fmt.Print("$: ")
        //fmt.Scanln(&line)
        scanner.Scan()
        line = scanner.Text()
        if len(line) == 0 {
            continue
        }
        input, server_addr, key, value := parseUserInput(line)

        if input == Read {
            go readData(key, server_addr) // for read request, the value is ""
        } else if input == Write {
            go writeData(key, value, server_addr)
        }
    }
}


func main() {
    cmdArgs()
    if len(*ipAddr) == 0 {
        fmt.Println("ip addr is empty")
        os.Exit(-1)
    }
    PeerName = *ipAddr // 127.0.0.1:port number (used to identify each client)
    processClientRequest()
}

