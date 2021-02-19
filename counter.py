import sys
import csv
from pyspark import *


def main():	
	# create Spark context with necessary configuration
	sc = SparkContext()
	
	dataFile = open("./input/1.txt")
	relatedList = []
	videoList = []
	for line in dataFile:
		content = line.split('\t')
		videoRow = []
		if (len(content) > 8):
			for i in range(0, 9):
				videoRow.append(content[i].rstrip())
			videoList.append(videoRow)
			for i in range(9, len(content)):
				relatedList.append(content[i].rstrip())
		else:
			videoList.append([content[0].rstrip(), 'NA', -1, 'NA', -1, -1, -1, -1, -1])

	# read data from text file and split each line into words
	words = sc.parallelize(relatedList)
	
	# count the occurrence of each word
	wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
	wordList = wordCounts.collect()
	print('Frequency count complete.')
	print('Beginning to pair video ID\'s to list')


	#next is to correlate data in the array

	for ID in wordList:
		location = SearchForID(videoList, ID[0])
		if location != -1:
			videoList[location].append(ID[1])
		else:
			videoList.append([ID[0], 'NA', -1, 'NA', -1, -1, -1, -1, -1, ID[1]])
	
	print('data paired. Outputting to CSV.')
	with open ("./out/results2.csv", 'w', newline='') as csvfile:
		csvWriter = csv.writer(csvfile)
		fields = ['ID', 'Uploader', 'Age', ' Category', 'Length', 'Views', 'Rate', 'Ratings', 'Comments', 'PageRank']
		csvWriter.writerow(fields)
		csvWriter.writerows(videoList)

	# save the counts to output
	#wordCounts.saveAsTextFile("C:\\Users\\Zduss\\Spark\\learning\\out")
	return

def SearchForID(arr, ID):
	for i in range(len(arr)):
		if arr[i][0] == ID:
			return i
	return -1

	 
if __name__ == "__main__":
	main()