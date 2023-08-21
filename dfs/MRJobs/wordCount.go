package MR

// type MapReduce interface {
// 	Preprocess()
// }

//go build -buildmode=plugin -o MRJobs/wordCountJob.so MRJobs/wordCount.go
/*
func StartMapPhase(chunkName string) {
	chunkContent, err := os.Open(chunkName)
	defer chunkContent.Close()
	CheckError(err)
	fileScanner := bufio.NewScanner(chunkContent)

	intermediateFileName := (chunkName + "_map_intrm")
	intermediateFile, err := os.Create(intermediateFileName)
	defer intermediateFile.Close()
	CheckError(err)
	bufio.NewWriter(intermediateFile)

	lineNumber := 0
	for fileScanner.Scan() {
		line := fileScanner.Text()
		word, count := Map(lineNumber, line)
		intermediateFile.Write([]byte(word + "\t" + strconv.Itoa(count) + "\n"))
	}

}
func StartReducePhase(jobName string, intermediateDataMap map[string][]int) {
	outputFileName := (jobName + "_output")
	outputFile, err := os.Create(outputFileName)
	defer outputFile.Close()
	CheckError(err)
	bufio.NewWriter(outputFile)
	for key, val := range intermediateDataMap {
		word, count := Reduce(key, val)
		outputFile.Write([]byte(word + "\t" + strconv.Itoa(count) + "\n"))
	}
}
*/

/*
func Map(lineNumber int, line string) (string, int) {
	words := strings.Fields(line)
	return words[0], 1
}

func Reduce(word string, count []int) (string, int) {
	sum := 0
	for i := range count {
		sum += i
	}
	return word, sum
}
*/
// func CheckError(e error) {
// 	if e != nil {
// 		fmt.Println(e)
// 	}
// }
