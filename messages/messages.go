package messages

import (
    _ "os"
    _ "fmt"
    _ "net"
    _"container/list"
    _ "strings"
    _ "bytes"
    _ "io"
    "strconv"
    _ "sort"
    "encoding/json"
    _ "bufio"
    _ "log"
    _ "time"
    _ "path/filepath"
)

type PeerRequest int

const (
    Read_Request PeerRequest = iota
    Write_Request
    Propagate_Request
)


/* Internal Structs */
type Version_t struct{
    Timestamp int
    Datacenter_id int
}

type Data_info_t struct{
    Value string
    Version Version_t
}

type Dependency_t struct{
    Key string
    Version Version_t
}

type Pending_write_t struct{
    Key string
    Value string
    Timestamp int
    Datacenter_id int
}

/* Request and Response Definition */
type Read_request_t struct {
    Key string
}

type Read_response_t struct {
    Read_succ int
    Value string
}

type Write_request_t struct {
    Key string
    Value string
    Delay_1 int
    Delay_2 int
}

type Write_response_t struct {
    Write_succ int
}

type Propagate_request_t struct {
    Write_request Write_request_t
    Dependency_list []Dependency_t
}

type Propagate_response_t struct {
    Prop_succ int
}


func EncodeReadRequest(key string) []byte {
    var req = Read_request_t {
                key,
              }
    req_bytes, _ := json.Marshal(&req)
    //fmt.Println("Encode Read Request: ", string(req_bytes))

    var req_type PeerRequest = Read_Request
    var req_str string
    req_str = strconv.Itoa(int(req_type)) + ";" + string(req_bytes) + "\n"

    //fmt.Println("Encode Read Request: ", req_str)

    return []byte(req_str)
}

func EncodeWriteRequest(key string, value string, delay_1 int, delay_2 int) []byte {
    var req = Write_request_t {
                key,
                value,
                delay_1,
                delay_2,
              }
    req_bytes, _ := json.Marshal(&req)
    //fmt.Println("Encode Write Request: ", string(req_bytes))

    var req_type PeerRequest = Write_Request
    var req_str string
    req_str = strconv.Itoa(int(req_type)) + ";" + string(req_bytes) + "\n"

    //fmt.Println("Encode Write Request: ", req_str)

    return []byte(req_str)
}

func EncodePropagateRequest(key string, value string, dependency_list []Dependency_t) []byte {
    var write_req = Write_request_t {
                key,
                value,
                0,
                0,
              }
    var req = Propagate_request_t {
                write_req,
                dependency_list,
              }

    req_bytes, _ := json.Marshal(&req)
    //fmt.Println("Encode Write Request: ", string(req_bytes))

    var req_type PeerRequest = Propagate_Request
    var req_str string
    req_str = strconv.Itoa(int(req_type)) + ";" + string(req_bytes) + "\n"

    //fmt.Println("Encode Write Request: ", req_str)

    return []byte(req_str)
}

func ParseResponse(req PeerRequest, msg []byte) interface{} {
    switch req {
    case Read_Request:
        //fmt.Println("Handle Peer Response: Register Request Response", string(msg))
        var response Read_response_t
        json.Unmarshal(msg, &response)
        //fmt.Println("Response content: ", response)
        return response
    case Write_Request:
        //fmt.Println("Handle Peer Response: File List Response", string(msg))
        var response Write_response_t
        json.Unmarshal(msg, &response)
        //fmt.Println("Response content: ", response)
        return response
    case Propagate_Request:
        //fmt.Println("Handle Peer Response: File Locations Response", string(msg))
        var response Propagate_response_t
        json.Unmarshal(msg, &response)
        //fmt.Println("Response content: ", response)
        return response
    default:
        break

    }
    return nil
}

