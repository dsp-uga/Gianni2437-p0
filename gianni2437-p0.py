from pyspark import SparkConf, SparkContext
import os
import math
import json
import re
import getopt, sys

def main():
    # list of command line arguments 
    argumentList = sys.argv[1:] 
    # Options 
    options = 'd:s:'
    # Long options 
    long_options = ['Documents_PATH', 'Stopwords_PATH'] 

    # check for added command line arguements
    if len(argumentList) > 0:
        try: 
            # Parsing argument 
            arguments, values = getopt.getopt(argumentList, options, long_options) 

            # checking each argument 
            for currentArgument, currentValue in arguments:
                # use specific documents path
                if currentArgument in ('-d', '--Documents_PATH'): 
                    docs_path = currentValue
                elif currentArgument in ('-s', '--Stopwords_PATH'): 
                    sw_path = currentValue

        except getopt.error as err: 
            # output error, and return with an error code 
            print (str(err)) 
    
    conf = SparkConf().setAppName("p0").setMaster("local")
    spark = SparkContext(conf=conf)
    # Loop though all txt docs, stopwords separated manually
    all_text = spark.textFile(f"{docs_path}/*.txt")
    words = all_text.flatMap(lambda line: line.split(" "))
    lowercase_words = words.map(lambda word: word.lower())
    all_counts = lowercase_words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

    """ sp1: filter out keys with 2 or less instances in
        the document, order based on keys repeated the most """
    counts = all_counts.filter(lambda indiv: indiv[1] > 2)
    top_counts = counts.takeOrdered(40, lambda x: -x[1]) # Documentation used negative value for sorting, idk kind of weird

    with open(f"{sw_path}/sp1.json", 'w+') as f:
        json.dump(dict(top_counts), f, sort_keys=True, indent=4)  # indent and sorting for readability 


    """sp2: filter out specified stopwords by broadcasting RDD
        and searching that array for filtering specific keys """
    stopwords = spark.textFile(f"{sw_path}/stopwords.txt")
    stopwords = stopwords.flatMap(lambda line: line.split(" "))
    stopwords = stopwords.map(lambda word: word.lower()).collect()
    b = spark.broadcast(stopwords)
    counts = counts.filter(lambda x: x[0] not in b.value)
    top_counts = counts.takeOrdered(40, lambda x: -x[1])

    with open(f"{sw_path}/sp2.json", 'w+') as f:
        json.dump(dict(top_counts), f, sort_keys=True, indent=4) 


    """sp3: remove all keys with a length of 1 or less;
    check first and last values of key and remove value
    if within the specified punctation array """
    punc = [",",".","?","!","'",";",":"]
    counts = counts.filter(lambda x: len(x[0]) > 1)
    # Check front of array
    counts = counts.map(lambda x: (x[0][1:], x[1]) if x[0][0] in punc else (x[0],x[1]))
    # Check back of array 
    counts = counts.map(lambda x: (x[0][0:len(x[0])-1], x[1]) if x[0][-1:] in punc else (x[0],x[1]))
    # Recombine in case identical keys created 
    counts = counts.reduceByKey(lambda a, b: a + b)
    top_counts = counts.takeOrdered(40, lambda x: -x[1])

    with open(f"{sw_path}/sp3.json", 'w+') as f:
        json.dump(dict(top_counts), f, sort_keys=True, indent=4) 

    """sp4: gather document locations by looping through directory;
        *abuse* list comprehension to create an array of RDDs, one
        for each document; after recreating indivdual RDDs following
        the steps from sp1-sp3, union all RDDs and use countByKey to 
        get number of times each key appears in a unique document;
        use value to calculate TF-IDF for each key and output top 5 from
        each document """
    base_filepath = docs_path
    all_docs = []
    for doc in os.listdir(base_filepath):
        all_docs.append(f'{base_filepath}{doc}')

    # Repeat processing from sp1-sp3
    temp_words = [spark.textFile(path) for path in all_docs]
    temp_words = [x.flatMap(lambda line: line.split(" ")) for x in temp_words] 
    temp_words = [x.map(lambda word: word.lower()) for x in temp_words] 
    temp_words = [x.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b) for x in temp_words]
    temp_words = [x.filter(lambda indiv: indiv[1] > 2) for x in temp_words]
    temp_words = [y.filter(lambda x: len(x[0]) > 1) for y in temp_words]
    temp_words = [y.map(lambda x: (x[0][1:], x[1]) if x[0][0] in punc else (x[0],x[1])) for y in temp_words]
    temp_words = [y.map(lambda x: (x[0][0:len(x[0])-1], x[1]) if x[0][-1:] in punc else (x[0],x[1])) for y in temp_words]
    temp_words = [y.reduceByKey(lambda a, b: a + b) for y in temp_words]
    temp_words = [y.filter(lambda x: x[0] not in b.value) for y in temp_words]
    # Find number of times key is repeated across documents
    count = spark.union(temp_words).countByKey()
    temp_words = [y.map(lambda x: [x[0], x[1], count[x[0]]]) for y in temp_words]
    # Calculate TF-IDF inside RDD
    temp_words = [y.map(lambda x: (x[0], math.log(len(all_docs)/x[2]) * x[1])) for y in temp_words]
    # Array of outputs from each document
    output = [y.takeOrdered(5, lambda x: -x[1]) for y in temp_words]

    with open(f"{sw_path}/sp4.json", 'w+') as f:
        combined_output = []
        for o in output:
            combined_output += o
        json.dump(dict(combined_output), f, sort_keys=True, indent=4) 

    spark.stop()

if __name__ == '__main__':
    main()