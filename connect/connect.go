package connect

import (
    "fmt"
    "net"
    "os"
    "bufio"
    _ "log"
    "messages"
    _ "encoding/json"
    "time"
)

var handlePeerRequestCallback func ([]byte, string) []byte = nil

func SendMsg(conn net.Conn, msg []byte) error {
    //log.Println("Send msg ", string(msg))
    _, err := conn.Write([]byte(msg))
    if err != nil {
        fmt.Println("Send Msg: ", err.Error())
    }
    return nil
}

func RecvMsg(conn net.Conn, req messages.PeerRequest) (interface{}, error) {
    //log.Printf("RecvMsg (%s)", conn.RemoteAddr().String())
    msg, err := bufio.NewReader(conn).ReadBytes('\n')
    if err != nil {
        fmt.Printf("%s: Error %s\n", err.Error())
        return nil, nil
    }

    res := messages.ParseResponse(req, msg[0:len(msg)-1])
    //log.Printf("RecvMsg (%s) %v\n", conn.RemoteAddr().String(), res.(messages.Register_response_t))
    return res, err
}

func ConnectToServer(dest_ip string, dest_port string) net.Conn {
    //fmt.Println("Connecting to ", dest_ip+":"+dest_port)
    var conn net.Conn
    for {
        c, err := net.Dial("tcp", dest_ip+":"+dest_port)
        if err != nil {
            fmt.Println("Error connecting:", err.Error())
            time.Sleep(5 * time.Second)
            //os.Exit(1)
        } else {
            conn = c
            break
        }
    }

    return conn
}

func SendReadRequest(conn net.Conn, key string) error {
    //fmt.Println("Send Register Request ", conn.RemoteAddr().String(), files, lengths)
    req := messages.EncodeReadRequest(key)
    return SendMsg(conn, req)
}


func SendWriteRequest(conn net.Conn, key string, value string, delay_1 int, delay_2 int) error {
    //fmt.Println("Send File Locations Request for %s", conn.RemoteAddr().String(), filename)
    req := messages.EncodeWriteRequest(key, value, delay_1, delay_2)
    return SendMsg(conn, req)
}

func SendPropagateRequest(conn net.Conn, key string, value string, dependency_list []messages.Dependency_t) error {
    //fmt.Println("Send Chunk Register Request", conn.RemoteAddr().String())
    req := messages.EncodePropagateRequest(key, value, dependency_list)
    return SendMsg(conn, req)
}

func RegisterHandlePeerRequestCallBack(callback func([]byte, string) []byte){
    handlePeerRequestCallback = callback
}

func RunServer(ip string, port string) {
    /* tcp connection */
    //fmt.Println("Create server at " + ip + ":" + port)
    ln, err := net.Listen("tcp", ip+":"+port)
    if err != nil {
        fmt.Println("Error listening:", err.Error());
        os.Exit(1);
    }

    defer ln.Close()

    for {
        c, err := ln.Accept()
        if err != nil {
            fmt.Println("Error connecting:", err.Error())
            return
        }
        //fmt.Println("Peer " + c.RemoteAddr().String() + " connected.")

        // handle connection concurrently
        go handleConnection(c)
    }
}

func handleConnection(conn net.Conn) {
    buffer, err := bufio.NewReader(conn).ReadBytes('\n')

    if err != nil {
        //fmt.Println("Client left.")
        conn.Close()
        return
    }

    //fmt.Println("handle connection ", string(buffer))

    // handle the request
    if (handlePeerRequestCallback != nil){
        response := handlePeerRequestCallback(buffer, conn.RemoteAddr().String())
        // Send response message to the client
        conn.Write(response)
    }

    // Restart the process
    go handleConnection(conn)

}
