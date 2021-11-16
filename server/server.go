package main

import (
    "fmt"
    "connect"
    "messages"
    _"os"
    "db"
    "strings"
    "strconv"
    "encoding/json"
    "bytes"
    _"log"
    _ "unsafe"
    "flag"
    "time"
)

/*-----------------------------------------------------------------*/

/* A list of all datacenter ports (to handle request), hard coded */

/*-----------------------------------------------------------------*/
var datacenter_port_list = []string{"9997", "9998", "9999"}


/*------------------------------------------*/

/* Datacenter Resources, unique for each datacenter */

/*------------------------------------------*/
var my_datacenter_id int = -1
var timestamp int = 0
var dependency_list = []messages.Dependency_t{}
var received_write_list = []messages.Dependency_t{}
var pending_queue_map = map[messages.Pending_write_t][]messages.Dependency_t{} // <pending write, list of not received dependent writes>

func increment_time(){
    timestamp++
}

func update_time(ts int){
    if (ts > timestamp + 1){
        timestamp = ts
    }else{
        timestamp++
    }
}

func get_time() int{
    return timestamp
}

func get_datacenter_id() int{
    return my_datacenter_id
}

func check_err(err error) {
    if err != nil {
        fmt.Println(err.Error())
        panic(err)
    }
}

func get_data_info(key string) messages.Data_info_t{
    var data_info_str string
    data_info_str = db.DB_Get(key)
    split_str := strings.Split(data_info_str, ";")
    var timestamp int
    var write_datacenter_id int

    timestamp, err := strconv.Atoi(split_str[1])
    check_err(err)
    write_datacenter_id, err = strconv.Atoi(split_str[2])
    check_err(err)

    var version = messages.Version_t{
                    timestamp,
                    write_datacenter_id,
                    }

    var data_info = messages.Data_info_t{
                    split_str[0],
                    version,
                    }
    return data_info
}

func get_value(key string) string{
    var data_info messages.Data_info_t
    data_info = get_data_info(key)
    return data_info.Value
}

func update_local_DB(key string, new_value string, new_version messages.Version_t){
    var new_data_info_str string
    new_data_info_str = new_value + ";" + strconv.Itoa(new_version.Timestamp) + ";" + strconv.Itoa(new_version.Datacenter_id)
    db.DB_Set(key, new_data_info_str)

    return
}

func get_version(key string) messages.Version_t{
    var data_info messages.Data_info_t
    data_info = get_data_info(key)
    return data_info.Version
}


func recreate_dependency_list(key string, new_version messages.Version_t){
    // put <key, new_version> into the dependency list
    var new_dependency = messages.Dependency_t{
                        key,
                        new_version}
    dependency_list = []messages.Dependency_t{new_dependency}
    return
}


func append_to_dependency_list(dependency messages.Dependency_t){
    dependency_list = append(dependency_list, dependency)
    return
}

func search_received_writes(dependency messages.Dependency_t) bool{
    var found bool
    found = false
    for _, received_write := range received_write_list{
        if (received_write.Key == dependency.Key &&
            received_write.Version.Timestamp == dependency.Version.Timestamp &&
            received_write.Version.Datacenter_id == dependency.Version.Datacenter_id){
            found = true
        }
    }
    return found
}

func update_not_received_list(dependency messages.Dependency_t, pending_write messages.Pending_write_t){
    _, exists := pending_queue_map[pending_write]
    if (exists == false){
        pending_queue_map[pending_write] = []messages.Dependency_t{dependency}
    }else{
        curr_not_received_list := pending_queue_map[pending_write]
        curr_not_received_list = append(curr_not_received_list, dependency)
        pending_queue_map[pending_write] = curr_not_received_list
    }
    return
}

func add_to_received_writes(arrived_dependency messages.Dependency_t){
    received_write_list = append(received_write_list, arrived_dependency)
    return
}

func clear_prior_pendings(arrived_dependency messages.Dependency_t){
    for pending_write, not_received_list := range pending_queue_map{
        var list_copy []messages.Dependency_t
        for _, not_received_write := range not_received_list{
            // only copy the dependency if it is not the arrived dependency
            // same as remove the arrived dependency from the not received list
            if (not_received_write != arrived_dependency){
                list_copy = append(list_copy, not_received_write)
            }
        }
        pending_queue_map[pending_write] = list_copy
    }
    for pending_write, not_received_list := range pending_queue_map{
        // this write is no longer pending
        if len(not_received_list) == 0{
            delete(pending_queue_map, pending_write)
            fmt.Printf("propagate request for enables dependency for key = %s, ts = %d, dcID = %d to be commited", arrived_dependency.Key, pending_write.Key, pending_write.Timestamp, pending_write.Datacenter_id)
            // commit pending_write if all of its dependencies have arrived
            var version = messages.Version_t{
                            pending_write.Timestamp,
                            pending_write.Datacenter_id,
                            }
            update_local_DB(pending_write.Key, pending_write.Value, version)
            update_time(pending_write.Timestamp + 1)
        }
    }
}

/*----------------------End of datacenter resources-----------------------*/

var serverPort *string
var dcIdStr *string
var dbPort *string

var my_addr string
var my_port string

func cmdArgs(){
    serverPort = flag.String("serverPort", "", "server port exposed to the network")
    dcIdStr = flag.String("dcID", "", "datacenter id")
    dbPort = flag.String("dbPort", "", "database port connected to this datacenter")
    flag.Parse()
}


func handle_read_request(msg []byte) messages.Read_response_t {

    var req messages.Read_request_t

    json.Unmarshal(msg, &req)

    var curr_value string
    var curr_version messages.Version_t

    curr_value = get_value(req.Key)
    curr_version = get_version(req.Key)

    var dependency = messages.Dependency_t{
                        req.Key,
                        curr_version,
                        }

    append_to_dependency_list(dependency)

    increment_time()

    var res = messages.Read_response_t {
                1,
                curr_value}
    return res
}


func handle_write_request(msg []byte) messages.Write_response_t {
    var req messages.Write_request_t

    json.Unmarshal(msg, &req)

    increment_time()

    var new_version = messages.Version_t {
                        get_time(),
                        get_datacenter_id(),
                      }
    // update both value and ts, datacenter id in local database
    update_local_DB(req.Key, req.Value, new_version)


    // propagate data to every other datacenter
    fmt.Printf("propagate write to other datacenters, key = %s, value = %s, new time = %d, new dcID = %d\n", req.Key, req.Value, new_version.Timestamp, new_version.Datacenter_id)
    propagateData(req.Key, req.Value, dependency_list, new_version, req.Delay_1, req.Delay_2)

    // recreate dependency list of this client
    recreate_dependency_list(req.Key, new_version)

    var res  = messages.Write_response_t {1}

    return res
}


func handle_propagate_request(msg []byte) messages.Propagate_response_t{
    var req messages.Propagate_request_t

    json.Unmarshal(msg, &req)

    fmt.Printf("receives propagate request for key = %s, value = %s\n", req.Write_request.Key, req.Write_request.Value)

    // separate the dependency about this coming write request from the other dependencies in the list
    var attached_dependency_list []messages.Dependency_t
    attached_dependency_list = req.Dependency_list
    var dependency_for_write_request = attached_dependency_list[len(attached_dependency_list) - 1]
    attached_dependency_list = attached_dependency_list[:(len(attached_dependency_list) - 1)]

    var pending_write = messages.Pending_write_t{
                            req.Write_request.Key,
                            req.Write_request.Value,
                            dependency_for_write_request.Version.Timestamp,
                            dependency_for_write_request.Version.Datacenter_id,
                        }

    // check if any of the dependencies in the dependency list (except for the last element) has arrived
    var need_delay bool
    need_delay = false
    for _, dependency := range attached_dependency_list{
        var received bool
        received = search_received_writes(dependency)
        if (received == false){
            need_delay = true
            fmt.Printf("propagate request for key = %s, value = %s has a not arrived dependency for key = %s, ts = %d, dcID = %d", req.Write_request.Key, req.Write_request.Value, dependency.Key, dependency.Version.Timestamp, dependency.Version.Datacenter_id)
            // put this dependency to the coming write's pending queue
            update_not_received_list(dependency, pending_write)
        }
    }


    // put the dependency about this coming write request into received list
    add_to_received_writes(dependency_for_write_request)
    // see if this coming write can clear any pending writes
    clear_prior_pendings(dependency_for_write_request)

    // commit this write if no pending dependencies
    if (need_delay == false){
        fmt.Printf("can commit propagate request for key = %s, value = %s", req.Write_request.Key, req.Write_request.Value)
        update_local_DB(req.Write_request.Key, req.Write_request.Value, dependency_for_write_request.Version)
        update_time(pending_write.Timestamp + 1)
    }

    var res  = messages.Propagate_response_t {1}

    return res
}



func propagateData(key string, value string, curr_dependency_list []messages.Dependency_t, new_version messages.Version_t, delay_1 int, delay_2 int){
    var new_dependency = messages.Dependency_t{
                            key,
                            new_version,
                        }

    var updated_dependency_list []messages.Dependency_t
    updated_dependency_list = append(curr_dependency_list, new_dependency)

    var neighbor_num int = 0 // left to right in datacenter_port_list
    for _, port_num := range datacenter_port_list{
        if (port_num != my_port){
            if (neighbor_num == 0){
                fmt.Printf("Delay sending propagate request for key = %s, value = %s to datacenter port = %s for %d seconds\n", key, value, port_num, delay_1)
                time.Sleep(time.Duration(delay_1) * time.Second)
            }else{
                fmt.Printf("Delay sending propagate request for key = %s, value = %s to datacenter port = %s for %d seconds\n", key, value, port_num, delay_2)
                time.Sleep(time.Duration(delay_2) * time.Second)
            }
            fmt.Printf("Start sending propagate request for key = %s, value = %s to datacenter port = %s\n", key, value, port_num)
            neighbor_num++
            for{
                conn := connect.ConnectToServer("127.0.0.1", port_num)
                err := connect.SendPropagateRequest(conn, key, value, updated_dependency_list)
                if (err != nil){
                    conn.Close()
                    continue
                }
                recv_msg, err := connect.RecvMsg(conn, messages.Propagate_Request)
                if (err != nil){
                    conn.Close()
                    continue
                }
                propagate_resp := recv_msg.(messages.Propagate_response_t)
                if propagate_resp.Prop_succ == 1 {
                    fmt.Printf("%s: Propagate for key = %s to 127.0.0.1:%s Succeeded\n", my_addr, key, port_num)
                    break
                }else{
                    fmt.Printf("%s: Propagate for key = %s to 127.0.0.1:%s failed\n", my_addr, key, port_num)
                    continue
                }
            }
        }
    }
}

func HandlePeerRequestCallBack(msg []byte, peerAddr string) []byte {
    var req messages.PeerRequest

    msg_reader := bytes.NewBuffer(msg)
    s, _ := msg_reader.ReadBytes(';')
    i, _ := strconv.ParseInt(string(s[0:len(s)-1]), 10, 32)
    req = messages.PeerRequest(i)
    msg, _ = msg_reader.ReadBytes('\n');

    switch req {
    case messages.Read_Request:
        fmt.Println("Handle Peer Request: Read Request")
        res := handle_read_request(msg)
        res_byte, _ := json.Marshal(&res)
        //fmt.Println("Response for the register request ", string(res_byte))
        return []byte(string(res_byte)+"\n")
    case messages.Write_Request:
        fmt.Println("Handle Peer Request: Write Request")
        res := handle_write_request(msg)
        res_byte, _ := json.Marshal(res)
        //fmt.Println("Response for the file list request", string(res_byte))
        return []byte(string(res_byte)+"\n")
    case messages.Propagate_Request:
        fmt.Println("Handle Peer Request: Propagate Request")
        res := handle_propagate_request(msg)
        res_byte, _ := json.Marshal(res)
        //fmt.Println("Response for the file list request", string(res_byte))
        return []byte(string(res_byte)+"\n")
    default:
        break
    }
    return nil
}


func main() {
    cmdArgs()

    my_port = *serverPort
    my_addr = "127.0.0.1:" + my_port

    my_datacenter_id, _ = strconv.Atoi(*dcIdStr)

    var db_addr string
    db_addr = "127.0.0.1:" + *dbPort // can be 7001, 7002, 7003

    db.InitDB(db_addr)

    fmt.Println("Start datacenter at " + "127.0.0.1" + ":" + my_port + ", id = " + strconv.Itoa(my_datacenter_id) + ", db addr = " + db_addr);

    connect.RegisterHandlePeerRequestCallBack(HandlePeerRequestCallBack)
    connect.RunServer("127.0.0.1", my_port)
}
