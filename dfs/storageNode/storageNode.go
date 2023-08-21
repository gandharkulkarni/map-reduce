//Storage node code
package main

import (
	"dfs/client_handler"
	"dfs/constants"
	"dfs/helper"
	"dfs/storageNode_handler"
	"fmt"
	"log"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

var currentUser, _ = user.Current()
var destination string = "/bigdata/students/" + currentUser.Username + "/"

func getDiskSpace() string {
	var stat unix.Statfs_t
	unix.Statfs(destination, &stat)
	return strconv.FormatUint((stat.Bavail*uint64(stat.Bsize))/(1024*1024*1024), 10)
}

func sendHeartbeat(SNMachineName string, SNPort string, wg *sync.WaitGroup) {
	defer wg.Done()

	//connect as a storage node to central Component
	// host := "orion02:4000" //host of central component orion02:4000
	host := constants.ControllerMachineName + ":" + constants.ControllerPortForStorageNode
	availableDiskSpace := getDiskSpace()
	beat := &storageNode_handler.Beat{
		MachineName: SNMachineName,
		Port:        SNPort,
		Beat:        true,
		DiskSpace:   availableDiskSpace,
	}
	for {
		conn, err := net.Dial("tcp", host)
		if err != nil {
			log.Fatalln(err.Error())
			return
		} else {
			//fmt.Println("Connected to central Component!")

			storageNode := storageNode_handler.NewStorageNode(conn)
			//fmt.Println("Sending heartbeat")
			storageNode.Send(beat)

			//receive response
			// response, _ := storageNode.ReceiveResponse()
			storageNode.ReceiveResponse()

			//fmt.Println("Reponse from centralComponent:", response.GetMessage())
			conn.Close()

			time.Sleep(5 * time.Second)
		}
	}
}

func main() {
	//create server for file transfer
	splittedArgs := strings.Split(os.Args[1], ":")
	SNMachineName := splittedArgs[0]
	SNPort := splittedArgs[1]

	var wg sync.WaitGroup

	wg.Add(3)

	//client-storage node operations
	go performClientOperations(SNMachineName, SNPort, &wg)
	//controller - storage node operations
	go sendHeartbeat(SNMachineName, SNPort, &wg)
	//storage-storage node operations
	go performStorageNodeOperations(SNMachineName, SNPort, &wg)

	wg.Wait()
}

/*-------------------------------------------------------------------------------------------------*/
func performStorageNodeOperations(SNMachineName string, SNPort string, wg *sync.WaitGroup) {
	defer wg.Done()
	port, err := strconv.Atoi(SNPort)
	helper.CheckErr(err)
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port+1)) //need to change
	helper.CheckErr(err)
	fmt.Println("Storage listening for other storage node on port:: ", SNPort)
	var waitg sync.WaitGroup
	for {
		if conn, err := listener.Accept(); err == nil {
			//get response from client i.e client serverName, port and beat
			clientHandler := client_handler.NewFileHandler(conn)
			//handle a single client
			waitg.Add(1)
			go handleStorageNode(clientHandler, &waitg) //check if "go" can be used.
			waitg.Wait()
			conn.Close()
		}
	}
}
func handleStorageNode(storageHandler *client_handler.FileHandler, waitg *sync.WaitGroup) {
	defer waitg.Done()
	fmt.Println("In handle StorageNode.")
	response, err := storageHandler.Receive()
	helper.CheckErr(err)
	action := response.GetAction()
	fmt.Println(action)
	switch {
	case action == "put":
		saveReplica(response)
	case action == "get":
		getReplica(storageHandler, response.GetFilename()+"_checksum_r", response)
	default:
		fmt.Println("Unexpected action. Please choose between get/put")
	}
}
func saveReplica(response *client_handler.ClientMsg) {
	//Write checksum to a file
	err := os.WriteFile(destination+response.GetFilename()+"_checksum_r", []byte(response.GetChecksum()), 0644)
	helper.CheckErr(err)
	fmt.Println("Replica Checksum file saved on server successfully.")
	fmt.Println("Replicated Checksum::", response.GetFilename())

	//Write chunk to a file
	err = os.WriteFile(destination+response.GetFilename()+"_r", response.GetData(), 0644)
	helper.CheckErr(err)
	fmt.Println("Replica File saved on server successfully.")
	fmt.Println("Replicated Response::", response.GetFilename())

}

/*--------------------------------------------------------------------------------------------------*/
func performClientOperations(SNMachineName string, SNPort string, wg *sync.WaitGroup) {
	defer wg.Done()

	listener, err := net.Listen("tcp", ":"+SNPort)
	if err != nil {
		log.Fatalln(err.Error())
		return
	} else {
		fmt.Println("Storage node started on port:: ", SNPort)
	}
	var waitg sync.WaitGroup
	for {
		if conn, err := listener.Accept(); err == nil {
			//get response from client i.e client serverName, port and beat
			clientHandler := client_handler.NewFileHandler(conn)
			//handle a single client
			waitg.Add(1)
			go handleAClient(clientHandler, &waitg) //check if "go" can be used.
			waitg.Wait()
			//conn.Close()
		}
	}
}

func handleAClient(clientHandler *client_handler.FileHandler, waitg *sync.WaitGroup) {
	defer waitg.Done()
	fmt.Println("In handle client")
	response, err := clientHandler.Receive()
	helper.CheckErr(err)
	action := response.GetAction()
	fmt.Println("Action::", action)
	switch {
	case action == "put":
		putFile(clientHandler, response)
	case action == "get":
		filename := response.GetFilename()
		if strings.HasSuffix(filename, "_r") {
			filename = strings.TrimSuffix(filename, "_r")
			filename = filename + "_checksum_r"
		} else {
			filename = filename + "_checksum"
		}
		getFile(clientHandler, filename, response)
	case action == "delete":
		deleteChunkFiles(clientHandler, response.GetFilename())
	default:
		fmt.Println("Unexpected action. Please choose between get/put")
	}

	//replication
	//SN ->controller connection
	//controller ->SN activesnode
	//SN map (
	// 	"filename"
	// )

	//getActiveNodes
	//check if next and Next to next nodes are active
	//If Yes then add to a temp replica  map
	//return mao to controller

}

func deleteChunkFiles(clientHandler *client_handler.FileHandler, filename string) {
	err := filepath.Walk(destination, func(path string, info os.FileInfo, err error) error {
		helper.CheckErr(err)
		if !info.IsDir() && strings.Contains(info.Name(), filename) {
			if strings.Contains(path[len(destination):], string(filepath.Separator)) {
				fmt.Printf("Skipping file in subdirectory: %s\n", path)
				return nil
			}
			fmt.Printf("Deleting file: %s\n", path)
			err = os.Remove(path)
			helper.CheckErr(err)
		}
		return nil
	})
	helper.CheckErr(err)
}

func putFile(clientHandler *client_handler.FileHandler, response *client_handler.ClientMsg) {
	//Write checksum to a file
	err := os.WriteFile(destination+response.GetFilename()+"_checksum", []byte(response.GetChecksum()), 0644)
	helper.CheckErr(err)
	fmt.Println("Checksum file saved on server successfully.")
	fmt.Println("Checksum::", response.GetFilename())

	//Write chunk to a file
	err = os.WriteFile(destination+response.GetFilename(), response.GetData(), 0644)
	helper.CheckErr(err)
	fmt.Println("File saved on server successfully.")
	fmt.Println("Response::", response.GetFilename())

	clientHandler.Close()
	fmt.Println("Connection with client closeds")
	//Send replica to other storage nodes
	replicaDetails := response.GetReplicaDetails()
	replicaList := replicaDetails[response.GetFilename()].GetReplicaList()
	for i := 1; i < len(replicaList); i++ {
		sendReplicaToStorageNode(replicaList[i], response)
	}
	fmt.Println("End of sendReplicaToStorageNode")
}

func sendReplicaToStorageNode(machineName string, response *client_handler.ClientMsg) {
	fmt.Println("Machine name SN ", machineName)
	machineNamePortArr := strings.Split(machineName, ":")
	port, err := strconv.Atoi(machineNamePortArr[1])
	fmt.Println("Port no ::", port)
	helper.CheckErr(err)
	conn, err := net.Dial("tcp", machineNamePortArr[0]+":"+strconv.Itoa(port+1))
	helper.CheckErr(err)
	clientHandler := client_handler.NewFileHandler(conn)
	clientHandler.Send(response)
	fmt.Println("Forwarding replica to storage node from storage node")
}

func getFile(clientHandler *client_handler.FileHandler, checkSumName string, response *client_handler.ClientMsg) {
	//Read data from file
	data, err := os.ReadFile(destination + response.GetFilename())
	helper.CheckErr(err)
	//Read data from checksum file
	//checkSumName : file.txt_1_checksum
	checkSum, err := os.ReadFile(destination + checkSumName)
	helper.CheckErr(err)
	//calculate data checksum
	dataCheckSum := helper.GetChunkCheckSum(data)
	if dataCheckSum == string(checkSum) {
		fmt.Println("Checksum matched !!!!! chunkName ->", response.GetFilename())
		clientHandler.Send(&client_handler.ClientMsg{Data: data, Filename: response.GetFilename()})
	} else {
		fmt.Println("------Checksum Error-----")
		fmt.Println("Getting checksum from other replica")
		replicaDetails := response.GetReplicaDetails()
		filename := response.GetFilename()
		replicaNodeList := replicaDetails[filename].GetReplicaList()
		fmt.Println("Chunkname::::", filename)
		fmt.Println("Replica list::::", replicaNodeList)
		var storageResponse *client_handler.ClientMsg
		for i := 1; i < len(replicaNodeList); i++ {
			//Get machinename and port
			machineNamePortArr := strings.Split(replicaNodeList[i], ":")
			//get port
			port, err := strconv.Atoi(machineNamePortArr[1])
			helper.CheckErr(err)
			fmt.Println("Connecting to replica node --> ", machineNamePortArr[0]+":"+strconv.Itoa(port+1))
			//Connect to port + 1 storage node -> storage node communicate on port + 1
			conn, err := net.Dial("tcp", machineNamePortArr[0]+":"+strconv.Itoa(port+1))
			helper.CheckErr(err)
			defer conn.Close()
			storageHandler := client_handler.NewFileHandler(conn)
			storageHandler.Send(response)
			storageResponse, err = storageHandler.Receive()
			helper.CheckErr(err)
			if strings.Contains(storageResponse.GetStatus(), "Success") {

				//Update chunk
				err = os.WriteFile(destination+response.GetFilename(), storageResponse.GetData(), 0644)
				helper.CheckErr(err)
				fmt.Println("File updated on server successfully.")

				//Update checksum
				err := os.WriteFile(destination+response.GetFilename()+"_checksum", []byte(storageResponse.GetChecksum()), 0644)
				helper.CheckErr(err)
				fmt.Println("Checksum updated on server successfully.")
				break
			}
			fmt.Println("Forwarding replica to storage node from storage node")

		}
		clientHandler.Send(storageResponse)
		// get file from replcia storage node and send to client
	}

}

//getReplica(storageHandler, response.GetFilename()+"_checksum_r", response)
func getReplica(storageHandler *client_handler.FileHandler, checkSumName string, response *client_handler.ClientMsg) {
	fmt.Println("Reading file from ", destination+response.GetFilename()+"_r")
	data, err := os.ReadFile(destination + response.GetFilename() + "_r")
	helper.CheckErr(err)
	fmt.Println("Reading checksum from ", destination+checkSumName)
	checkSum, err := os.ReadFile(destination + checkSumName)
	helper.CheckErr(err)
	dataCheckSum := helper.GetChunkCheckSum(data)
	if dataCheckSum == string(checkSum) {
		fmt.Println("Checksum matched !!!!!")
		storageHandler.Send(&client_handler.ClientMsg{Data: data, Filename: response.GetFilename(), Checksum: string(checkSum), Status: "Success: Checksum valid"})
	} else {
		fmt.Println("------Checksum Error-----")
		storageHandler.Send(&client_handler.ClientMsg{Status: "Err: Checksum not valid"})
	}
}
