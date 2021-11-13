package main

import (
    "fmt"
    "connect"
    . "messages"
    "flag"
    "os"
    "db"
)


/*------------------------------------------*/

/* Datacenter Resources, unique for each datacenter */

/*------------------------------------------*/
var timestamp int = 0
var client_dependency_lists = map[string][]messages.Dependency_t{}


func update_time(){
    timestamp++
}

func get_time() int{
    return timestamp
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

    var version = messages.Version_t{
                    split_str[1],
                    split_str[2]
                    }

    var data_info = messages.Data_info_t{
                    split_str[0],
                    version}
    return data_info
}

func get_value(key string) string{
    var data_info messages.Data_info_t
    data_info = get_data_info(key)
    return data_info.Value
}

func update_local_value(key string, new_value string){
    // get the current version of the key
    var curr_data_info_str string
    curr_data_info_str = db.DB_Get(key)
    split_str := strings.Split(curr_data_info_str, ";")

    var timestamp int
    var datacenter_id int
    timestamp = split_str[1]
    datacenter_id = split_str[2]

    var new_data_info_str string
    new_data_info_str = new_value + ";" + timestamp + ";" + datacenter_id
    db.DB_Set(key, new_data_info_str)

    return
}

func get_version(key string) Version_t{
    var data_info messages.Data_info_t
    data_info = get_data_info(key)
    return data_info.Version
}

func update_version(key string, new_version Version_t) {
    // get the current value of the key
    var curr_data_info_str string
    curr_data_info_str = db.DB_Get(key)
    split_str := strings.Split(curr_data_info_str, ";")
    var value string
    value = split_str[0]

    // update the version and store it back to DB
    var new_data_info_str string
    new_data_info_str = value + ";" + new_version.Timestamp + ";" + new_version.Datacenter_id
    db.DB_Set(key, new_data_info_str)
    return
}

func recreate_dependency_list(key string, new_version Version_t, string peer_addr){
    // clear the client's current dependency list
    client_dependency_lists[peer_addr] = nil

    // put <key, new_version> into the client's dependency list
    var new_dependency = messages.Dependency_t{
                        key,
                        new_version}
    client_dependency_lists[peer_addr] = []messages.Dependency_t{new_dependency}
    return
}

func finish_local_update(key string, new_version Version_t, string peer_addr) {
    // first update the database
    update_version(key, new_version)
    // recreate dependency list of this client
    recreate_dependency_list(key, new_version, peer_addr)
    return
}

func append_to_dependency_list(peer_addr string, dependency messages.Dependency_t){
    var curr_dependency_list []messages.Dependency_t
    curr_dependency_list = client_dependency_lists[peer_addr]
    curr_dependency_list = append(curr_dependency_list, dependency)
    client_dependency_list[peer_addr] = curr_dependency_list
    return
}

/*----------------------End of datacenter resources-----------------------*/
func handle_read_request(msg []byte) messages.Read_response_t {
    var req messages.Read_request_t

    json.Unmarshal(msg, &req)

    var curr_value string
    var curr_version Version_t

    curr_value = get_value(req.Key)
    curr_version = get_version(req.Key)

    var dependency = messages.Dependency_t{
                        req.Key,
                        curr_version
                        }

    append_to_dependency_list(req.PeerAddr, dependency)

    update_time()

    var res = messages.Read_response_t {
                1,
                curr_value}
    return res
}


func handle_write_request(msg []byte) messages.Write_response_t {
    var req messages.Write_request_t

    json.Unmarshal(msg, &req)

    update_local_value(req.key, req.Value)

    var curr_dependency_list []messages.Dependency_t
    curr_dependency_list = client_dependency_lists[req.PeerAddr]

    var propagate_req = messages.Propagate_request_t {
                        req,
                        curr_dependency_list}

    go connect.SendPropagateRequest(propagate_req)

    update_time()

    var new_version = Version_t {
                        get_time(),
                        get_datacenter_id()
                        }
    finish_local_update(req.Key, new_version, req.PeerAddr)

    var res  = Write_response_t {1}

    return res
}

func HandlePeerRequestCallBack(msg []byte, peerAddr string) []byte {
    var req PeerRequest

    msg_reader := bytes.NewBuffer(msg)
    s, _ := msg_reader.ReadBytes(';')
    i, _ := strconv.ParseInt(string(s[0:len(s)-1]), 10, 32)
    req = PeerRequest(i)
    //log.Println("Handle Peer Request ", req)
    msg, _ = msg_reader.ReadBytes('\n');

    switch req {
    case Read_Request:
        fmt.Println("Handle Peer Request: Read Request")
        res := handle_read_request(msg)
        res_byte, _ := json.Marshal(&res)
        //fmt.Println("Response for the register request ", string(res_byte))
        return []byte(string(res_byte)+"\n")
    case Write_Request:
        fmt.Println("Handle Peer Request: Write Request")
        res := handle_write_request(msg)
        res_byte, _ := json.Marshal(res)
        //fmt.Println("Response for the file list request", string(res_byte))
        return []byte(string(res_byte)+"\n")
    case Propagate_Request:
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
    db.InitDB()
    var datacenter_ip string
    var datacenter_port string

    datacenter_ip = "127.0.0.1"
    datacenter_port = os.Args[1] // first argument passed to the command line
    fmt.Println("Start datacenter at " + datacenter_ip + ":" + datacenter_port);

    connect.RegisterHandlePeerRequestCallBack(HandlePeerRequestCallBack)
    connect.RunServer(datacenter_ip, datacenter_port)
}
