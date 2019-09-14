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
	// Open up the file and check for errors
	file, err := ioutil.ReadFile(path)
	checkError(err)

	// String will parse the whole file into a string
	// fields function splits the string word by word 
	words := strings.Fields(string(file)) 

	// Setup the regular expression to filter out 
	// un-alphanumeric characters and check for errors
	reg, err := regexp.Compile("[^0-9a-zA-Z]+")
	checkError(err)

	// Use a map to easily keep track of words and counts
	countMap := make(map[string]int)
	
	// Iterate over each word, apply the regexp and update counts
	for _, word := range words {
		processedString := strings.ToLower(reg.ReplaceAllString(word, ""))  
		//fmt.Printf("Word after regexp %sn", processedString)

		// If word is already in the map, then increment count
		// If not, make a new entry and start count
		_, ok := countMap[processedString]
			if ok{
				countMap[processedString] += 1
			} else {
				countMap[processedString] = 1
			}
    }
	
	// Set up the key/value pair output as a WordCount struct
	var wordCount []WordCount

	// Iterate over the map, only add words over the length threshold
	// to the output WordCount key/value pair
	for word, count := range countMap{
		if len(word) >= charThreshold{
			wordCount = append(wordCount, WordCount{word,count})		
		}
	}
	
	// Sort key/value pair output with helper function
	sortWordCounts(wordCount)
	
	//fmt.Printf("Output all WordCount %v\n", wordCount)
	fmt.Printf("Output top N words %v\n", wordCount[0:numWords])
	
	// return N key/value pairs corresponding to N most frequent words
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
