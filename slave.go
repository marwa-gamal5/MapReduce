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
	"os"
	//"io/ioutil"
    "log"
	//"html/template"
    //"net/http" 
    //"sync"
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

var result string

//var wg = sync.WaitGroup{}  

/* The entry point for our System */
func main(){
    result=""
    //wg.Add(1)
    //count = 0
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
    //httpp()
    /* 
     * Listen for other incoming requests form other nodes to join cluster
     * Note: We are not doing anything fancy right now to make this node as master. Not yet!
     */
    if ableToConnect || (!ableToConnect && *makeMasterOnError) {
        if *makeMasterOnError {fmt.Println("Will start this node as master.")}
		
		listenOnPort(me)
    } else {
        fmt.Println("Quitting system. Set makeMasterOnError flag to make the node master.", myid)
    }
	//httpp()
	//go httpp()
	//http.HandleFunc("/", handler)
    //http.ListenAndServe(":8080", nil)
	//wg.Wait() 
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
        fmt.Println("Connected to cluster to ask for a part of file. Sending message to node.")
        text := "Hi nody.. please add me to the cluster.."
		
        requestMessage := getAddToClusterMessage(me, dest, text)
        json.NewEncoder(connOut).Encode(&requestMessage)

        decoder := json.NewDecoder(connOut)
        var responseMessage AddToClusterMessage
        decoder.Decode(&responseMessage)
		
		input := strings.Fields(string(responseMessage.Message))
	
        // Define the mapper and reducer    
        mapper := func(s string) []KeyValue { return mapFunc(s) }
        reducer := func( values []int) int { return reduceFunc(values) }
		// Run the MapReduce job
        output := mapReduce(input, mapper, reducer)
		
		for word, count := range output {
		c := strconv.Itoa(count)
        result = result + (word + "\t" + c + "\n")
		}
	   
		// Store the output in the OutPut File
        err = storeInFile(output)
		if err != nil {
		    fmt.Println("Error storing output in File")
			log.Fatal(err)
			return false
			}
		          
        fmt.Println("Got response:\n" + responseMessage.String() + "\n")
		
		/*This connection send the result of map reduce to the master */
		
		fmt.Println("Connected to cluster to send result. Sending message to node.")
        text2 := result
		
		requestMessage2 := getAddToClusterMessage(me, dest, text2)
        json.NewEncoder(connOut).Encode(&requestMessage2)
		
		decoder2 := json.NewDecoder(connOut)
        var responseMessage2 AddToClusterMessage
        decoder2.Decode(&responseMessage2)
		
		fmt.Println("Got response:\n" + responseMessage2.String())

        
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
            fmt.Println("Got request:\n" + requestMessage.String())
             
            text := "Sure buddy.. too easy.."
            responseMessage := getAddToClusterMessage(me, requestMessage.Source, text)
            json.NewEncoder(connIn).Encode(&responseMessage)
            connIn.Close()
        }
    }
}

func mapFunc(input string) []KeyValue {
    words := strings.Fields(input)
    kvs := make([]KeyValue, len(words))
    for i, word := range words {
        kvs[i] = KeyValue{word, 1}
    }
	
    /*
    for _, kv := range kvs {
	    fmt.Printf("Key: %s, Value: %d\n", kv.Key, kv.Value)
    }
	*/
	
    return kvs
}

func reduceFunc(values []int) int {
    count := 0
    for _, value := range values {
        count += value
    }
    return count
}

func mapReduce(input []string, mapper func(string) []KeyValue, reducer func([]int) int) map[string]int {
    intermediate := make(map[string][]int)
    for _, in := range input {
        kvs := mapper(in)
        for _, kv := range kvs {
            intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
        }
    }
	/*
	for key, values := range intermediate {
	    fmt.Printf("Key: %s, Value: %d\n", key, values)
        
    }*/
	
    output := make(map[string]int)
    for key, values := range intermediate {
        output[key] = reducer(values)
    }
    return output
}

func storeInFile(output map[string]int) error {

    // Create a File to insert data into it 
    OutFile, err := os.Create("Map_Reduce.txt")
    if err != nil {
		fmt.Println("Error Creating file")
        log.Fatal(err)
    }
    defer OutFile.Close()

    // Loop over the output data and execute the SQL statement for each row
    for word, count := range output {
		c := strconv.Itoa(count)
       _, err := OutFile.WriteString(word + "\t" + c + "\n")
        if err != nil {
			fmt.Println("Error Writing Result")
            log.Fatal(err)
        }
    }

    return nil
}
/*
func httpp() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
	t, _ := template.ParseFiles("page.html")
    file := r.FormValue("file")
    var message string
    switch file {
    case "P1":
        message = result
     }
    t.Execute(w, message)
})
    http.ListenAndServe(":8088", nil)
}
*/