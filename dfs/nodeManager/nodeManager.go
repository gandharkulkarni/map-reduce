package main

import (
	"bufio"
	"dfs/client_handler"
	"dfs/helper"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"os/user"
	"plugin"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/slices"
)

var currentUser, _ = user.Current()
var destination string = "/bigdata/students/" + currentUser.Username + "/"
var nodeManagerMachineNameWithPort string
var shuffledFileData = []string{} //store reducer file data

func main() {
	/*
		start nodeManager on storage node port + 200
		register to resource manager
		Listen for resource manager, get .so file and execute
	*/

	splittedArgs := strings.Split(os.Args[1], ":")
	nodeManagerMachineNameWithPort = os.Args[1]
	SNPort, err := strconv.Atoi(splittedArgs[1])
	helper.CheckErr(err)
	SNPort = SNPort + 200
	nodeManagerPort := SNPort + 1
	//go run nodeManager.go orion02:7000
	//add 200 to 7000 = 7200
	//listen to resourceManager on updated port number
	//get .so file, fileName, reducerNodes, chunklocation from resourceManager
	//save to .so file
	var waitg sync.WaitGroup
	waitg.Add(2)
	go resourceManagerOperations(SNPort, &waitg)
	go nodeManagerOperations(nodeManagerPort, &waitg)
	waitg.Wait()
}
func nodeManagerOperations(nodeManagerPort int, wg *sync.WaitGroup) {
	defer wg.Done()
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(nodeManagerPort)) //need to change
	helper.CheckErr(err)
	fmt.Println("Node manager listening for other node manager node on port:: ", nodeManagerPort)
	for {
		if conn, err := listener.Accept(); err == nil {
			//get response from client i.e client serverName, port and beat
			clientHandler := client_handler.NewFileHandler(conn)
			handleANodeManager(clientHandler)
		}
	}

}
func handleANodeManager(clientHandler *client_handler.FileHandler) {
	fmt.Println("In A handle nodeManager")
	response, err := clientHandler.Receive()
	helper.CheckErr(err)
	//Read file data
	shuffledFileBytes := response.GetData()
	fmt.Println("Received file shuffledFileBytes")
	//store data on reducer
	shuffledFileData = append(shuffledFileData, string(shuffledFileBytes))
	fmt.Println("Bytes appended to slice")
	//? How do I know I got data from all the node managers
}

func resourceManagerOperations(SNPort int, wg *sync.WaitGroup) {
	defer wg.Done()
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(SNPort)) //need to change
	helper.CheckErr(err)
	fmt.Println("Storage listening for other storage node on port:: ", SNPort)
	var waitg sync.WaitGroup
	for {
		if conn, err := listener.Accept(); err == nil {
			//get response from client i.e client serverName, port and beat
			clientHandler := client_handler.NewFileHandler(conn)
			//handle a single client
			waitg.Add(1)
			go handleAResourceManager(clientHandler, &waitg)
			waitg.Wait()
			conn.Close()
		}
	}
}

func handleAResourceManager(clientHandler *client_handler.FileHandler, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("In A handle resourceManager")
	response, err := clientHandler.Receive()
	helper.CheckErr(err)
	soFileName := response.GetFilename() + "_MRJOB.so"
	soFileData := response.GetData()
	chunkFileList := response.GetFileList()
	reducerList := response.GetReducerList()
	nonReducerList := response.GetNonReducerList()
	//Each node manager has only one chunk
	outputFileName := chunkFileList[0] + "_output_file"

	//build .so file at node manager side
	fmt.Println("Creating .so file")
	err = ioutil.WriteFile(destination+soFileName, soFileData, 0644)
	helper.CheckErr(err)
	plug, err := plugin.Open(destination + soFileName)
	helper.CheckErr(err)
	MapSym, err := plug.Lookup("Map")
	helper.CheckErr(err)
	MapFn := MapSym.(func(int, string) (string, int))

	//Create a reducer file for each reducer  {reducer_0, reducer_1,....}
	for index := range reducerList {
		os.Create(destination + "reducer_" + strconv.Itoa(index))
	}

	var wg2 sync.WaitGroup
	wg2.Add(len(chunkFileList))
	for _, chunkFile := range chunkFileList {
		go MapAndShuffle(clientHandler, chunkFile, reducerList, MapFn, &wg2)
	}
	wg2.Wait()

	//If current node present in reducer node then wait for all the data from other nodes and call reduce method
	//else send the reducer file to reducer node
	SendDataToReducer(reducerList)

	//wait for all the nodes to send the data
	if slices.Contains(reducerList, nodeManagerMachineNameWithPort) {
		for {
			if len(shuffledFileData) == len(nonReducerList) {
				fmt.Println("Data from all node managers")
				for reducerIndex := range reducerList {
					reducerFileBytes, err := os.ReadFile(destination + "reducer_" + strconv.Itoa(reducerIndex))
					helper.CheckErr(err)
					shuffledFileData = append(shuffledFileData, string(reducerFileBytes))
				}
				fmt.Println("Data on reducer appended")
				consolidatedData := strings.Join(shuffledFileData, "\n")
				fmt.Println("Data consolidated")
				ReduceSym, err := plug.Lookup("Reduce")
				helper.CheckErr(err)
				ReduceFn := ReduceSym.(func(string, []int) (string, int))
				finalOutputFileName := Reduce(clientHandler, consolidatedData, outputFileName, ReduceFn)
				SendOutputFileDetailsToResourceManager(clientHandler, finalOutputFileName)
				break
			} else {
				time.Sleep(1 * time.Second)
			}
		}
	}
	fmt.Println("Cleaning up")
	//Clean up .so file after reduce
	os.Remove(destination + soFileName)
	for reducerIndex := range reducerList {
		err = os.Remove(destination + "reducer_" + strconv.Itoa(reducerIndex))
		if err != nil {
			fmt.Println("Error deleting files")
		}
	}
	clientHandler.Send(&client_handler.ClientMsg{Status: "Done"})
	fmt.Println("Done !!")
	//Reset global variable
	shuffledFileData = []string{}
}
func SendOutputFileDetailsToResourceManager(clientHandler *client_handler.FileHandler, finalOutputFileName string) {
	fmt.Println(finalOutputFileName)
	clientHandler.Send(&client_handler.ClientMsg{Status: "StoreFile", Filename: finalOutputFileName})
}
func Reduce(clientHandler *client_handler.FileHandler, data string, outputFileName string, ReduceFn func(string, []int) (string, int)) string {
	reducedDataMap := make(map[string]int)
	lines := strings.Split(data, "\n")
	completionPercentage := 0
	for lineNumber, line := range lines {
		splittedArray := strings.Split(line, "\t")
		if len(splittedArray) > 1 {
			//replace [ ] braces
			strCount := strings.ReplaceAll(splittedArray[1], "[", "")
			strCount = strings.ReplaceAll(strCount, "]", "")
			countSlice := []int{}
			//split 1,1,1,1,1 and count them
			for _, c := range strings.Split(strCount, ",") {
				intCount, err := strconv.Atoi(c)
				helper.CheckErr(err)
				countSlice = append(countSlice, intCount)
			}
			word, count := ReduceFn(splittedArray[0], countSlice)
			_, exist := reducedDataMap[word]
			if exist {
				reducedDataMap[word] += count
			} else {
				reducedDataMap[word] = count
			}
			if completionPercentage < (lineNumber*100)/len(lines) {
				completionPercentage = (lineNumber * 100) / len(lines)
				fmt.Println("Reduce phase complete : ", completionPercentage, "%")
				clientHandler.Send(&client_handler.ClientMsg{Status: "Reduce phase complete : " + strconv.Itoa(completionPercentage) + "%"})
			}
		}
	}
	clientHandler.Send(&client_handler.ClientMsg{Status: "Finishing up"})
	fmt.Println("Finishing up")
	sortedKeys := make([]string, 0, len(reducedDataMap))
	for key := range reducedDataMap {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)
	finalOutputFileName := destination + outputFileName
	finalOutputFile, err := os.Create(finalOutputFileName)
	finalOutputFileWriter := bufio.NewWriter(finalOutputFile)
	helper.CheckErr(err)
	for _, key := range sortedKeys {
		finalOutputFileWriter.Write([]byte(key + "\t" + strconv.Itoa(reducedDataMap[key]) + "\n"))
		finalOutputFileWriter.Flush()
	}
	//Need to write checksum to get the file
	dataInBytes, err := os.ReadFile(finalOutputFileName)
	helper.CheckErr(err)

	finalOutputChecksumFileName := destination + outputFileName + "_checksum"
	finalOutputChecksumFile, err := os.Create(finalOutputChecksumFileName)
	helper.CheckErr(err)

	finalOutputChecksumFileWriter := bufio.NewWriter(finalOutputChecksumFile)
	dataChecksum := helper.GetChunkCheckSum(dataInBytes)
	finalOutputChecksumFileWriter.Write([]byte(dataChecksum))
	finalOutputChecksumFileWriter.Flush()

	fmt.Println("Reduce phase completed")
	clientHandler.Send(&client_handler.ClientMsg{Status: "Reduce phase completed"})
	fmt.Println("Outputfile: ", finalOutputFileName)
	return outputFileName
}
func SendDataToReducer(reducerList []string) {
	fmt.Println("Sending data to reducer")
	for reducerIndex, reducer := range reducerList {
		//Reducer files are reducer_0, reducer_1......
		//Avoid connecting to self node
		if reducer != nodeManagerMachineNameWithPort {
			//Send reducer_1 to node at index 1 in reducer list
			reducerFileName := "reducer_" + strconv.Itoa(reducerIndex)
			reducerHostDetails := strings.Split(reducer, ":")
			reducerName := reducerHostDetails[0]
			reducerPort, err := strconv.Atoi(reducerHostDetails[1])
			helper.CheckErr(err)
			reducerPort += 201
			//Connect to reducer node on port + 1
			fmt.Println("Connecting to reducer ", reducerName+":"+strconv.Itoa(reducerPort))
			conn, err := net.Dial("tcp", reducerName+":"+strconv.Itoa(reducerPort))
			helper.CheckErr(err)
			clientHandler := client_handler.NewFileHandler(conn)
			fmt.Println("Connected to reducer")
			//Open reducer_0 / reducer_1 .... file in bytes
			reducerFile, err := os.ReadFile(destination + reducerFileName)
			helper.CheckErr(err)
			//Send reducer file to reducer node
			clientHandler.Send(&client_handler.ClientMsg{Data: reducerFile})
			fmt.Println("Data sent to reducer")
		} else {
			fmt.Println("I am the reducer")
		}
	}
}
func MapAndShuffle(clientHandler *client_handler.FileHandler, chunkName string, reducerList []string, MapFn func(int, string) (string, int), wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("Starting Map phase")

	//Read chunk lines
	data, err := ioutil.ReadFile(destination + chunkName)
	helper.CheckErr(err)

	//Get all lines
	lines := strings.Split(string(data), "\n")
	lines = lines[:len(lines)-1] //Removing empty line at the end
	intermediateFileName := (destination + chunkName + "_map_intrm")
	fmt.Println("Creating file ", intermediateFileName)
	intermediateFile, err := os.Create(intermediateFileName)
	helper.CheckErr(err)

	intermediateFileWriter := bufio.NewWriter(intermediateFile)
	completionPercentage := 0
	for lineNumber, line := range lines {
		word, count := MapFn(lineNumber, line)
		lineToWrite := word + "\t" + strconv.Itoa(count) + "\n"
		if lineNumber == len(lines)-1 {
			lineToWrite = word + "\t" + strconv.Itoa(count)
		}
		intermediateFileWriter.Write([]byte(lineToWrite))
		intermediateFileWriter.Flush()
		if completionPercentage < (lineNumber*100)/len(lines) {
			completionPercentage = (lineNumber * 100) / len(lines)
			fmt.Println("Map phase complete : ", completionPercentage, "%")
			clientHandler.Send(&client_handler.ClientMsg{Status: "Map phase complete : " + strconv.Itoa(completionPercentage) + "%"})
		}
		//ToDo: Check how to fix
		// if len(lines)-2 == lineNumber {
		// 	fmt.Println(lineNumber)
		// 	break
		// }
	}
	fmt.Println("Map phase complete : 100%")
	shuffle(clientHandler, chunkName, reducerList)
	fmt.Println("Shuffle phase complete: 100%")
	fmt.Println("Cleaning up mapper files")
	os.Remove(intermediateFileName)
}
func shuffle(clientHandler *client_handler.FileHandler, chunkFile string, reducerList []string) {
	reducerCount := len(reducerList)
	var intermediateDataMap = make(map[string]string)
	fmt.Println("Shuffle phase started")
	clientHandler.Send(&client_handler.ClientMsg{Status: "Shuffle phase started"})
	//For every chunk

	//Read respective intermediate file
	file, err := os.Open(destination + chunkFile + "_map_intrm")
	helper.CheckErr(err)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := scanner.Text()
		splittedArray := strings.Split(text, "\t")
		var key string
		var value int
		if splittedArray[0] != "" {
			key = splittedArray[0]
			value, err = strconv.Atoi(splittedArray[1])
			helper.CheckErr(err)
		}

		//Populate map [string, int[]] key = word, val =[1,1,1,1,1]
		_, exist := intermediateDataMap[key]
		if exist {
			intermediateDataMap[key] = intermediateDataMap[key] + "," + strconv.Itoa(value)
		} else {
			intermediateDataMap[key] = strconv.Itoa(value)
		}

	}
	fmt.Println("Map created")
	index := 0
	//for every key, val in intermediateDataMap
	for word, countList := range intermediateDataMap {
		//Get md5 hash for key
		wordHash := helper.GetChunkCheckSum([]byte(word))
		wordHashIntValue := new(big.Int)
		_, err := fmt.Sscan("0x"+wordHash, wordHashIntValue) //Convert hash to bigint
		helper.CheckErr(err)
		reducerIndex := new(big.Int).Mod(wordHashIntValue, big.NewInt(int64(reducerCount))).Int64() //Perform mod operation and convert it to int64
		file, err := os.OpenFile(destination+"reducer_"+strconv.Itoa(int(reducerIndex)), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		helper.CheckErr(err)

		lineToWrite := word + "\t" + countList + "\n"
		if index == len(intermediateDataMap) {
			lineToWrite = word + "\t" + countList
		}
		index++
		//Write to file as word	1,1,1,1,1,
		file.Write([]byte(lineToWrite))
		file.Close()
	}
	fmt.Println("Shuffle phase complete")
	clientHandler.Send(&client_handler.ClientMsg{Status: "Shuffle phase complete"})
	//Call reduce phase and send 1 KV pair as parameter to reduce() at a time
}
