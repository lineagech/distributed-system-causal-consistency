package main

import (
    _ "net"
    "fmt"
    "bufio"
    "os"
    _"flag"
    "connect"
    "messages"
    "strings"
    "strconv"
    _"sort"
    _"math/rand"
    _"log"
    _"sync"
    _"golang.org/x/sys/unix"
    _"io/ioutil"
)

type UserInput int

const (
    Read UserInput = iota
    Write
    Invalid
)


func parseUserInput(line string) (UserInput, string, string, string, int, int) {
    var res UserInput = Invalid
    var server_addr string
    var key string
    var value string
    var delay_1 int
    var delay_2 int
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
        delay_1 = 0
        delay_2 = 0
    }else{
        value = split_line[3]
        delay_1, _ = strconv.Atoi(split_line[4])
        delay_2, _ = strconv.Atoi(split_line[5])
    }

    return res, server_addr, key, value, delay_1, delay_2
}

func readData(key string, server_addr string){
    for {
        server_addr_split := strings.Split(server_addr, ":")
        conn := connect.ConnectToServer(server_addr_split[0], server_addr_split[1])
        err := connect.SendReadRequest(conn, key)
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
            fmt.Printf("Read for key = %s  Succeeded, value is %s\n", key, read_resp.Value)
            break
        } else {
            fmt.Printf("Read for key = %s  failed\n", key)
            continue
        }
    }
}


func writeData(key string, value string, server_addr string, delay_1 int, delay_2 int){
    for {
        server_addr_split := strings.Split(server_addr, ":")
        conn := connect.ConnectToServer(server_addr_split[0], server_addr_split[1])
        err := connect.SendWriteRequest(conn, key, value, delay_1, delay_2)
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
            fmt.Printf("Write for key = %s  Succeeded\n", key)
            break
        } else {
            fmt.Printf("Write for key = %s  failed\n", key)
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
        input, server_addr, key, value, delay_1, delay_2 := parseUserInput(line)

        if input == Read {
            go readData(key, server_addr) // for read request, the value is ""
        } else if input == Write {
            go writeData(key, value, server_addr, delay_1, delay_2)
        }
    }
}


func main() {
    processClientRequest()
}

