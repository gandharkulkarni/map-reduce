package MR

//go build -buildmode=plugin -o MRJobs/url-count.so MRJobs/url-count.go
/*
func Map(lineNumber int, line string) (string, int) {
	words := strings.Fields(line)
	url_str := words[len(words)-1]
	unescapedURL := strings.ReplaceAll(url_str, "%2e", ".")
	//Unique hostnames
	url, _ := url.Parse(unescapedURL)
	hostname := strings.TrimPrefix(url.Hostname(), "https://")
	return hostname, 1
}

func Reduce(word string, count []int) (string, int) {
	sum := 0
	for _, c := range count {
		sum += c
	}
	return word, sum
}
*/
