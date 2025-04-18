# Basic Word Count Map-Reduce in Python

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

    def mapper_init(self):
        # Read stopwords file only once per mapper
        self.stopwords = set()
        if self.options.stopwords:
            with open(self.options.stopwords, 'r', encoding='utf-8') as f:
                self.stopwords = set(
                    line.strip().lower() for line in f if line.strip()
                )

    def mapper(self, _, line):
        try:
            review = json.loads(line)
            review_text = review.get("reviewText")
            category = review.get("category")

            # Tokenization to unigrams, using whitespaces, tabs, digits, and the characters
            # ()[]{}.!?,;:+=-_"'`~#@&*%€$§\/
            delimiters = r"[ \t\d\(\)\[\]\{\}\.\!\?,;:+=\-_'\"`~#@&\*\%€\$§\\/]+"
            word_list = re.split(delimiters, review_text.lower())

            # filter out stopwords, very short words and duplicates  
            word_list = set(
                token for token in word_list
                if token and token not in self.stopwords and len(token) > 1
            )

            for word in word_list:  # words and category 
                yield ("n_tc", word, category), 1 # to count docs with this word and category, A 
                yield ("n_term", word), 1 # to count total docs per words, A + B (docs in c and not in c)
            yield ("n_category", category), 1 # to count total docs per categories, A + C (docs with t and not with t)
            yield ("n_documents", None), 1 # to count total number of documents, A + B + C + D

        except json.JSONDecodeError: # skip bad lines if needed (just in case)
            pass  

    def reducer(self, key, counts): # sums up the values of all appearances of the term in each category 
        # (word, category): count
        yield key, sum(counts) 

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init, # stopword file 
                   mapper=self.mapper,  # data file 
                   reducer=self.reducer) # output 
        ]

if __name__ == '__main__':
    MapReduce.run()
