# Project 2: Distributed Computation Engine


**A. COMMUNICATION FLOW DIAGRAM**

**PUT**<br/>
<img width="793" alt="Screen Shot 2023-04-06 at 1 21 35 AM" src="https://user-images.githubusercontent.com/109916498/230320276-6d72b2f8-3d4d-47dd-8e5b-8b3521418b8f.png">

**GET**<br/>
<img width="738" alt="Screen Shot 2023-04-06 at 1 21 52 AM" src="https://user-images.githubusercontent.com/109916498/230320248-6d141435-7ca0-4262-bb8d-a23e51e06135.png">

**DELETE**<br/>
<img width="725" alt="Screen Shot 2023-04-06 at 1 22 20 AM" src="https://user-images.githubusercontent.com/109916498/230320186-3b401dba-1687-4ae8-a9a5-ea7aef03cdac.png">

**RUNMR**<br/>

![Screenshot 2023-05-17 at 7 57 38 PM](https://github.com/usf-cs677-sp23/P2-p2/assets/111133878/50e80c4d-1c65-4b98-9aad-4ad2c88375c2)


![Screenshot 2023-05-17 at 8 04 06 PM](https://github.com/usf-cs677-sp23/P2-p2/assets/111133878/3e0bf326-f419-44ee-926c-8dfbf40ecc93)


**B. PROTO**

**1. CONTROLLER <-> STORAGE NODE**

    message beat{
        string machineName = 1;
        string port = 2;
        bool beat = 3;
        string diskSpace = 4;
    }

    message response{
        string message = 1;
    }

**2. CONTROLLER <-> CLIENT**

    message clientMsg{
    bytes data = 1;
    string filename = 2;
    string action = 3;
    string checksum = 4;
    string status = 5;
    int64 filesize = 6;
    int64 chunksize = 7;
    map<string, string> chunkDetails = 8;
    map<string, replicaNodes> replicaDetails = 9;
    message replicaNodes{
        repeated string replicaList = 1;
    }
    repeated string fileList = 10;
    map<string, string> diskSpaceMap = 11;
    bool isTextFile = 12;
    repeated string activeNodes = 13;
    int64 requiredNoOfChunks = 14;
    int64 reducerCount = 15;
    repeated string reducerList = 16;
    repeated string nonReducerList = 17;
    int64 activeNodesCount = 18;
    string reducedOutputMachineName = 19;
}



**C. COMMAND LINE INTERFACE**

**1. CLIENT INTERFACE**
    
Action (PUT/GET/DELETE/RUNMR)

    ./client put FILEPATH chunkSize //Put file

    ./client get FILENAME DESTINATION_PATH //Get file

    ./client delete FILENAME //Delete file

    ./client ls //List files

    ./client runMR FILENAME .so_FILE_PATH REDUCER_COUNT //Run map reduce job on file


**2. CONTROLLER INTERFACE**

    ./controller <Port>

**3. STORAGE NODE INTERFACE**

    ./storage_node <Machinename:port>

**4. RESOURCE MANAGER INTERFACE**

    ./resourceManager

**5. NODE MANAGER INTERFACE**

    ./nodeManager <Machinename:port>


**Note -> Make sure you run ./storage_node ./nodeManager with same <Machinename:port> on one node of cluster. 
    
    
