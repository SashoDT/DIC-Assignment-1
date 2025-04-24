from mrjob.job import MRJob
from mrjob.step import MRStep

import re
import json
import os

class MapReduce(MRJob):

    # Stopwords file, pass with --stopwords argument
    def configure_args(self):
        # To avoid overwriting parent and add custom argument such as --stopwords 
        super().configure_args() # python < 3: super(MapReduce, self).configure_args()
        self.add_file_arg("--stopwords", help="Path to stopwords.txt")

    def mapper_init(self):  # Read stopwords file only once per mapper
        # Tokenization to unigrams, using whitespaces, tabs, digits, and the characters
        delimiters = r"[ \t\d\(\)\[\]\{\}\.\!\?,;:+=\-_'\"`~#@&\*\%€\$§\\/]+"
        self.delimiter = re.compile(delimiters)

        self.stopwords = set()
        if self.options.stopwords:
            with open(self.options.stopwords, 'r', encoding='utf-8') as f:
                for line in f: 
                    temp = line.strip().lower()
                    if temp: 
                        self.stopwords.add(temp)

    def mapper(self, _, line):
        review = json.loads(line)
        review_text = review.get("reviewText", "").lower()
        category = review.get("category", "")

        # Tokenization, remove duplicates and remove empty tokens 
        word_list = set(self.delimiter.split(review_text))
        word_list.discard("")

        # filter out stopwords and very short words
        word_list = [
            token for token in word_list
            if len(token) > 1 and token not in self.stopwords # reversed "and" for more speed  
        ]

        for word in word_list:  # words and category 
            yield ("n_tc", word, category), 1 # to count docs with this word and category, A 
            yield ("n_term", word), 1 # to count total docs per words, A + B (docs in c and not in c)
        yield ("n_category", category), 1 # to count total docs per categories, A + C (docs with t and not with t)
        yield ("n_documents", None), 1 # to count total number of documents, A + B + C + D

    def combiner(self, key, counts): # new, use combiner to compute partial sums 
        yield key, sum(counts)

    def reducer(self, key, counts): # sums up the values of all appearances of the term in each category 
        # (word, category): count
        yield key, sum(counts) 

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init, # stopword file 
                   mapper=self.mapper,  # data file 
                   combiner=self.combiner, # combiner 
                   reducer=self.reducer) # output 
        ]

if __name__ == '__main__':
    MapReduce.run()
