package main

/* Al useful imports */
import (
    "flag"
    "fmt"
    "net"
    "strings"
    "strconv"
    "time"
    "math/rand"
    "encoding/json"
    "io/ioutil"
    "log"
	"html/template"
    "net/http" 
    "sync"  
	//"os"

)

type KeyValue struct {
    Key   string
    Value int
}

/* Information/Metadata about node */
type NodeInfo struct {
    NodeId  int  `json:"nodeId"`
    NodeIpAddr  string  `json:"nodeIpAddr"`
    Port  string  `json:"port"`
}

/* A standard format for a Request/Response for adding node to cluster */
type AddToClusterMessage struct {
    Source NodeInfo  `json:"source"`
    Dest NodeInfo  `json:"dest"`
    Message string  `json:"message"`
}

/* Just for pretty printing the node info */
func (node NodeInfo) String() string {
    return "NodeInfo:{ nodeId:" + strconv.Itoa(node.NodeId) + ", nodeIpAddr:" + node.NodeIpAddr + ", port:" + node.Port + " }"
}

/* Just for pretty printing Request/Response info */
func (req AddToClusterMessage) String() string {
    return "AddToClusterMessage:{\n  source:" + req.Source.String() + ",\n  dest: " + req.Dest.String() + ",\n  message:" + req.Message + " }"
}
var count int

var ip1 string
var ip2 string
var ip3 string
var ip4 string

var chunk1 string
var chunk2 string
var chunk3 string
var chunk4 string

var result1 string
var result2 string
var result3 string
var result4 string

var wg = sync.WaitGroup{}  
/* The entry point for our System */
func main(){
    count = 0
	split_file()
	wg.Add(2)  
    
    /* Parse the provided parameters on command line */
    makeMasterOnError := flag.Bool("makeMasterOnError", false, "make this node master if unable to connect to the cluster ip provided.")
    clusterip := flag.String("clusterip", "127.0.0.1:8001", "ip address of any node to connnect")
    myport := flag.String("myport", "8001", "ip address to run this node on. default is 8001.")
    flag.Parse()
	
    /* Generate id for myself */
    rand.Seed(time.Now().UTC().UnixNano())
    myid := rand.Intn(99999999)

    myIp,_ := net.InterfaceAddrs()
    me := NodeInfo{ NodeId: myid, NodeIpAddr: myIp[0].String(),  Port: *myport}
    dest := NodeInfo{ NodeId: -1, NodeIpAddr: strings.Split(*clusterip, ":")[0], Port: strings.Split(*clusterip, ":")[1]}
    fmt.Println("My details:", me.String())
    //count = count +1
    /* Try to connect to the cluster, and send request to cluster if able to connect */
    ableToConnect := connectToCluster(me, dest)

    /* 
     * Listen for other incoming requests form other nodes to join cluster
     * Note: We are not doing anything fancy right now to make this node as master. Not yet!
     */
    if ableToConnect || (!ableToConnect && *makeMasterOnError) {
        if *makeMasterOnError {fmt.Println("Will start this node as master.")}
        go listenOnPort(me)
    } else {
        fmt.Println("Quitting system. Set makeMasterOnError flag to make the node master.", myid)
    }
	
    go httpp()
	//http.HandleFunc("/", handler)
    //http.ListenAndServe(":8080", nil)
	wg.Wait()  
}


/* 
 * This is a useful utility to format the json packet to send requests
 * This tiny block is sort of important else you will end up sending blank messages.
 */
func getAddToClusterMessage(source NodeInfo, dest NodeInfo, message string) (AddToClusterMessage){
    return AddToClusterMessage{
        Source: NodeInfo{
                NodeId: source.NodeId,
                NodeIpAddr: source.NodeIpAddr,
                Port: source.Port,
                },
        Dest: NodeInfo{
                NodeId: dest.NodeId,
                NodeIpAddr: dest.NodeIpAddr,
                Port: dest.Port,
                },
        Message: message,
    }
}


func connectToCluster(me NodeInfo, dest NodeInfo) (bool){
    /* connect to this socket details provided */
    connOut, err := net.DialTimeout("tcp", dest.NodeIpAddr + ":" + dest.Port, time.Duration(10) * time.Second)
    if err != nil {
        if _, ok := err.(net.Error); ok {
            fmt.Println("Couldn't connect to cluster.", me.NodeId)
            return false
        }
    } else {
        fmt.Println("Connected to cluster. Sending message to node.")
        text := "Hi nody.. please add me to the cluster.."
        requestMessage := getAddToClusterMessage(me, dest, text)
        json.NewEncoder(connOut).Encode(&requestMessage)

        decoder := json.NewDecoder(connOut)
        var responseMessage AddToClusterMessage
        decoder.Decode(&responseMessage)
        fmt.Println("Got response:\n" + responseMessage.String())
        
        return true
    }
    return false
}



func listenOnPort(me NodeInfo){
    /* Listen for incoming messages */
    ln, _ := net.Listen("tcp", fmt.Sprint(":" + me.Port))
    /* accept connection on port */
    /* not sure if looping infinetely on ln.Accept() is good idea */
    for{
        connIn, err := ln.Accept()
        if err != nil {
            if _, ok := err.(net.Error); ok {
                fmt.Println("Error received while listening.", me.NodeId)
            }
        } else {
            var requestMessage AddToClusterMessage
            json.NewDecoder(connIn).Decode(&requestMessage)
			count = count +1
            fmt.Println("Got request:\n" + requestMessage.String() + "\n")

             var message string
             switch count {
              case 1:
			  ip1 = requestMessage.Source.NodeIpAddr
              message = chunk1
              case 2:
			  ip2 = requestMessage.Source.NodeIpAddr
              message = chunk2
              case 3:
			  ip3 = requestMessage.Source.NodeIpAddr
              message = chunk3
              case 4:
			  ip4 = requestMessage.Source.NodeIpAddr
              message = chunk4
	          default:
              message = "Error"
             }
            //text := "Sure buddy.. too easy.."
            responseMessage := getAddToClusterMessage(me, requestMessage.Source, message)
            json.NewEncoder(connIn).Encode(&responseMessage)
			
			/* Got the result from slave */ 
			
			var requestMessage2 AddToClusterMessage
            json.NewDecoder(connIn).Decode(&requestMessage2)
			fmt.Println("Got request:\n" + "Result received successfully" + "\n")
			res := string(requestMessage2.Message)
			
			switch count {
              case 1:
			  result1 = res
			  
              case 2:
			  result2 = res
			  
              case 3:
			  result3 = res
			  
              case 4:
			  result4 = res
			  
            }
			
			text := "Got result successfully "
            responseMessage2 := getAddToClusterMessage(me, requestMessage2.Source, text)
            json.NewEncoder(connIn).Encode(&responseMessage2)

            connIn.Close()
        }
    }
}

func split_file() {
    // Open the file
    file, err := ioutil.ReadFile("CVPR_2019_paper.txt")
    if err != nil {
        log.Fatal(err)
    }

    fileSize := int64(len(file))
    chunkSize := fileSize / 4 // divide the file into 4 equal chunks

    // Split the file into 4 chunks
    var chunks []string
    for i := int64(0); i < 4; i++ {
        start := i * chunkSize
        end := start + chunkSize
        if i == 3 {
            end = fileSize // for the last chunk, read to the end of the file
        }
        chunks = append(chunks, string(file[start:end]))
    }

    // Print the chunks
    for i, chunk := range chunks {
        //fmt.Printf("Chunk %d:\n%s\n", i+1, chunk)
		if i==0 {
		chunk1 = chunk
		}else if i==1{
		chunk2 = chunk
		}else if i==2{
		chunk3 = chunk
		}else if i==3{
		chunk4 = chunk
		}
    }
	
	//fmt.Printf("Chunk1:\n%s\n", chunk1)
}
/*
func handler(w http.ResponseWriter, r *http.Request) {
    t, _ := template.ParseFiles("page.html")
    file := r.FormValue("file")
    var message string
    switch file {
    case "P1":
        message = chunk1
    case "P2":
        message = chunk2
    case "P3":
        message = chunk3
    case "P4":
        message = chunk4
	case "P5":
        message = chunk1 + chunk2 + chunk3 +chunk4
     }
	 t.Execute(w, message)
}
*/


func httpp() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	t, _ := template.ParseFiles("page2.html")
    file := r.FormValue("file")
    var message string
    switch file {
    case "P1":
        message = result1
    case "P2":
        message = result2
    case "P3":
        message = result3
    case "P4":
        message = result4
	case "P5":
        message = result1 + result2 + result3 +result4
     }
    t.Execute(w, message)
})
	
	
	http.ListenAndServe(":8088", nil)
	//fmt.Println("in")
}

