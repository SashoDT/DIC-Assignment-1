# Assignment 1 job
# This file does not work 

from mrjob.job import MRJob
from mrjob.step import MRStep

import re
import json

class MapReduce(MRJob):

    stopwords =r"stopwords.txt" # Stopwords path 
    # filepath is in bash command 

    def mapper(self, _, line):

        ## Create stopwords list: 
        with open(stopwords, 'r', encoding='utf-8') as f:
            stopwords = set(line.strip().lower() for line in f if line.strip())

        # Tokenization to unigrams, using whitespaces, tabs, digits, and the characters
        # ()[]{}.!?,;:+=-_"'`~#@&*%€$§\/
        delimiters = r"[ \t\d\(\)\[\]\{\}\.\!\?,;:+=\-_'\"`~#@&\*\%€\$§\\/]+"

        review = json.loads(line)

        # tokenises each line by using whitespaces, tabs, digits, and the characters ()[]{}.!?,;:+=-_"'`~#@&*%€$§\/ as delimiters 
        word_list = re.split(delimiters, line.get("reviewText").lower())
        # filter out stopwords and duplicates  
        word_list = set([token for token in word_list if token not in stopwords and len(token) > 1])

        #for loop through the terms in pre-processed list
        for word in word_list:
            yield (word, line.get("category"), 1)
                
    def reducer_count(self, word, category, counts):
        # sums up the values of all appearances of the term in each category 
        yield (word, category, sum(counts)) # 
    
    def steps(self):
        return [
            MRStep(mapper  = self.mapper,
                   reducer = self.reducer_count)
        ]
    
if __name__ == '__main__':
    
    MapReduce.run()