package cos418_hw1_1

import (
	"fmt"
	"sort"
	"strings"
	"io/ioutil"
	"regexp"
)

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuations and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	file, err := ioutil.ReadFile(path)
	checkError(err)
	
	str := string(file) 
	words := strings.Fields(str) 
	countMap := make(map[string]int)
	
	reg, err := regexp.Compile("[^0-9a-zA-Z]+")
	checkError(err)
	
	for _, word := range words {
		processedString := strings.ToLower(reg.ReplaceAllString(word, ""))  
		//fmt.Println(processedString)
		_, ok := countMap[processedString]
			if ok{
				countMap[processedString] += 1
			} else {
				countMap[processedString] = 1
			}
    }
	
	
	var wordCount []WordCount
	for word, count := range countMap{
		if len(word) >= charThreshold{
			wordCount = append(wordCount, WordCount{word,count})		
		}
	}
	
	sortWordCounts(wordCount)
	
	//fmt.Println(wordCount)
	//fmt.Println(wordCount[0:numWords])
	
	return wordCount[0:numWords]
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
