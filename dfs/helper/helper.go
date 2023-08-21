package helper

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
)

func CheckErr(err error) {
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
}
func GetFileCheckSum(filePath string) string {
	f, err := os.Open(filePath)
	CheckErr(err)
	defer f.Close()
	hash := md5.New()
	if _, err := io.Copy(hash, f); err != nil {
		CheckErr(err)
	}
	hashInBytes := hash.Sum(nil)
	return hex.EncodeToString(hashInBytes)
}
func GetFileSize(file string) int64 {
	fi, err := os.Stat(file)
	CheckErr(err)
	return fi.Size()
}
func CheckFileExists(filePath string) bool {
	if _, err := os.Stat(filePath); err == nil {
		fmt.Printf("File exists\n")
		return true
	} else {
		fmt.Printf("File does not exist\n")
		return false
	}
}
func GetChunkCheckSum(data []byte) string {
	hashInBytes := md5.Sum(data)
	// fmt.Println("Hash::", hashInBytes)
	return hex.EncodeToString(hashInBytes[:])
}
