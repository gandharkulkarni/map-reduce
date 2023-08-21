package main

import (
	"dfs/client_handler"
	"dfs/constants"
	"dfs/helper"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

var nodeManagerMap = make(map[string]string)                            //Key - Orion, Value - Port
var replicaMap = make(map[string]*client_handler.ClientMsgReplicaNodes) //replica Map
var chunkDetails = make(map[string]string)
var loadBalancedChunkDetails = make(map[string][]string)

// var loadBalancedChunkDetails = make(map[string]*client_handler.ClientMsgReplicaNodes) // {"orion03:4000":["file.txt_1", "file.txt_2"]}

func main() {
	/*
		start listening on port orion07:5000 (from constants)
		accepts nodeManagers and add to map
		Listen for client
		Accept client msg for fileName, JobPath, reducerCount
		Connect to controller to get storage nodes where file chunks are located
		connect to respective node manager for each storage node, send (optional chunkname) job file to node manager
		Node manager executes .so file for each chunkname

	*/
	var wg sync.WaitGroup

	wg.Add(1)
	go ListenForClient(&wg)
	//go ListenForNodeManager(&wg)
	wg.Wait()
}
func ListenForClient(wg *sync.WaitGroup) {
	defer wg.Done()
	listener, err := net.Listen("tcp", ":"+"5000")
	helper.CheckErr(err)
	fmt.Println("Resource Manager started on port for Client:: ", 5000)
	helper.CheckErr(err)
	for {
		if conn, err := listener.Accept(); err == nil {
			clientHandler := client_handler.NewFileHandler(conn)
			handleClientMsg(clientHandler)
		}
	}
}

func handleClientMsg(clientHandler *client_handler.FileHandler) {
	clientMsg, err := clientHandler.Receive()
	helper.CheckErr(err)
	//fmt.Println(clientMsg.GetData())         //Data -> .so file in bytes format
	fmt.Println(clientMsg.GetFilename())     //Filename -> File for which map reduce job needs to run
	fmt.Println(clientMsg.GetReducerCount()) //ReducerCount -> Number of reducers from client
	fileName := clientMsg.GetFilename()
	reducerCount := clientMsg.GetReducerCount()
	data := clientMsg.GetData()
	//Connect to controller
	//Send FileName to controller
	//Get StorageNode containing file chunks from Controller
	controllerOperations(fileName)

	//loadBalancing logic - <"orion03:4000",[file.txt_1, file.txt_2_r]>.
	balanceLoad()

	//Connect to nodeManager on updated port of storage Node: port+200.
	//Send .so file, fileName, reducerNodes, chunklocation to node managers running on those orion

	nodeManagerOperations(fileName, data, reducerCount, clientHandler)

	//empty the map once done
	loadBalancedChunkDetails = make(map[string][]string)
}

func nodeManagerOperations(fileName string, data []byte, reducerCount int64, clientHandler *client_handler.FileHandler) {
	reducerList := []string{}
	for key := range loadBalancedChunkDetails {
		reducerList = append(reducerList, key)
		if len(reducerList) == int(reducerCount) {
			break
		}
	}
	nonReducerList := []string{}
	for key := range loadBalancedChunkDetails {
		flag := false //Flag to check if key is a reducer
		for _, reducer := range reducerList {
			if key == reducer {
				flag = true // key is a reducer
			}
		}
		if !flag { //Key is not a reducer
			nonReducerList = append(nonReducerList, key)
		}
	}
	var waitg sync.WaitGroup
	//Adding weight group to each orion machine
	waitg.Add(len(loadBalancedChunkDetails))
	for key := range loadBalancedChunkDetails {
		//key = orion03:5000 value["file.txt_1","file.txt_2_r"]
		go assignTaskToNodeManager(key, fileName, data, reducerList, nonReducerList, clientHandler, &waitg)
	}
	waitg.Wait()
	clientHandler.Send(&client_handler.ClientMsg{Status: "Done"})
}
func assignTaskToNodeManager(key string, fileName string, data []byte, reducerList []string, nonReducerList []string, clientHandler *client_handler.FileHandler, wg *sync.WaitGroup) {
	fmt.Println("In assignTaskToNodeManager")

	splittedArray := strings.Split(key, ":")
	port, err := strconv.Atoi(splittedArray[1])
	helper.CheckErr(err)
	port = port + 200
	host := splittedArray[0] + ":" + strconv.Itoa(port)
	fmt.Println("Connecting to ", host)
	conn, err := net.Dial("tcp", host)
	helper.CheckErr(err)

	//Adding other reducer to nonReducerList as reducer[0] will send reducer_1 to reducer[1] and vice versa. So treating it as non reducer node that will send data
	for _, reducer := range reducerList {
		if key != reducer {
			nonReducerList = append(nonReducerList, key)
		}
	}

	nodeManagerHandler := client_handler.NewFileHandler(conn)

	//send filenames
	nodeManagerHandler.Send(&client_handler.ClientMsg{Filename: fileName, Data: data, FileList: loadBalancedChunkDetails[key], ReducerList: reducerList, NonReducerList: nonReducerList})
	for {
		fmt.Println("Sending status to client")
		response, err := nodeManagerHandler.Receive()
		helper.CheckErr(err)

		status := response.GetStatus()

		fmt.Println(status)
		if status == "Done" {
			break
		}
		if status == "StoreFile" {
			storeFileOnController(response, key)
		}
		status = key + "=>" + status
		clientHandler.Send(&client_handler.ClientMsg{Status: status})
	}
	wg.Done()
}
func storeFileOnController(response *client_handler.ClientMsg, machinename string) {
	host := constants.ControllerMachineName + ":" + constants.ControllerPortForResourceManager
	fmt.Println("Connecting to ", host)
	conn, err := net.Dial("tcp", host)
	helper.CheckErr(err)
	clientHandler := client_handler.NewFileHandler(conn)
	filename := response.GetFilename()

	//send filename to controller
	clientHandler.Send(&client_handler.ClientMsg{Filename: filename, Status: "StoreFile", ReducedOutputMachineName: machinename})
}
func balanceLoad() {
	//fileStorageNodeMap {file.txt_1: orion01:3000}
	//chunkDetails {file.txt_1:["orion05", "orion06", "orion07"]}

	for key := range replicaMap {
		chunkName := key
		chunkLocations := replicaMap[key].GetReplicaList() //
		//check if chunkName and chunkLocations are correct.
		//loadBalancedChunkDetails // {"orion03:4000":["file.txt_1", "file.txt_2"]}
		_, exist := loadBalancedChunkDetails[chunkLocations[0]]
		//No mapper on original node
		if !exist {
			var newArray []string
			newArray = append(newArray, chunkName)
			fmt.Println("Index::", 0, " CHunk12:", chunkName)
			loadBalancedChunkDetails[chunkLocations[0]] = newArray //err
		} else {
			// get minimum chunk count index
			minimumCount := 9999
			index := 900
			for i := 0; i < 3; i++ {
				chunks, exist := loadBalancedChunkDetails[chunkLocations[i]] //.GetReplicaList()
				if exist {
					//
					if len(chunks) < minimumCount {
						minimumCount = len(chunks)
						index = i
					}
				}
			}
			if index == 0 {
				//insert key to loadBalancedChunkDetails[chunkLocations[index]]
				fmt.Println("Index:", index, " chunk::", key)
				chunks, exist := loadBalancedChunkDetails[chunkLocations[index]] //.GetReplicaList()
				if exist {
					chunks = append(chunks, key)
					loadBalancedChunkDetails[chunkLocations[index]] = chunks
				} else {
					newArray := make([]string, 1)
					newArray = append(newArray, key)
					loadBalancedChunkDetails[chunkLocations[index]] = newArray //error
				}

			} else {
				//insert key + "_r" to chunkLocation[index]
				fmt.Println("Index:", index, " chunkkyy::", key)
				chunks, exist := loadBalancedChunkDetails[chunkLocations[index]] //.GetReplicaList()
				if exist {
					chunks = append(chunks, chunkName+"_r")
					loadBalancedChunkDetails[chunkLocations[index]] = chunks
				} else {
					newArray := make([]string, 1)
					newArray = append(newArray, chunkName+"_r")
					loadBalancedChunkDetails[chunkLocations[index]] = newArray //err
				}
			}
		}
	}

	fmt.Println("Map::", loadBalancedChunkDetails)

	// for key, _ := range loadBalancedChunkDetails {
	// 	loadBalancedChunkDetails[key] =  &client_handler.ClientMsgReplicaNodes{ReplicaList: loadBalancedChunkDetails[key]}
	// }

}

func controllerOperations(fileName string) {
	host := constants.ControllerMachineName + ":" + constants.ControllerPortForResourceManager
	fmt.Println("Connecting to ", host)
	conn, err := net.Dial("tcp", host)
	helper.CheckErr(err)
	clientHandler := client_handler.NewFileHandler(conn)
	//fmt.Println("Connected to resourceManager",clientHandler)
	//send filename to controller
	clientHandler.Send(&client_handler.ClientMsg{Filename: fileName})

	//Receive fileStorageNodeMap and replicaStorageNodeMap details from controller
	clientMsg, err := clientHandler.Receive()
	helper.CheckErr(err)
	// chunkDetails = clientMsg.GetChunkDetails()
	// fmt.Println("ClientMsg:",chunkDetails)
	replicaMap = clientMsg.GetReplicaDetails()
	fmt.Println("CLientttReplica::", replicaMap)
}
