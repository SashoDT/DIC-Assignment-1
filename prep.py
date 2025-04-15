import re
import json

# todo: install and import mrjob
# todo: logging

stopwords =r"C:\Users\hanna\OneDrive\Uni\4. Semester\dic\ex1\ex1\stopwords.txt"

file_path = r"C:\Users\hanna\OneDrive\Uni\4. Semester\dic\ex1\ex1\reviews_devset.json"

with open(stopwords, 'r', encoding='utf-8') as f:
    stopwords = set(line.strip().lower() for line in f if line.strip())

# Tokenization to unigrams, using whitespaces, tabs, digits, and the characters
# ()[]{}.!?,;:+=-_"'`~#@&*%€$§\/
delimiters = r"[ \t\d\(\)\[\]\{\}\.\!\?,;:+=\-_'\"`~#@&\*\%€\$§\\/]+"

cleaned_reviews = []

with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        review = json.loads(line)
        text = review.get("reviewText")
        category = review.get("category")

        # Tokenize
        tokens = re.split(delimiters, text)

        # Case folding
        tokens = [token.lower() for token in tokens]

        # Stopword and one-character filtering
        filtered = [token for token in tokens if token not in stopwords and len(token) > 1]

        cleaned_reviews.append({ # feel free to change when/how the data is returned for later
            'reviewerID': review.get('reviewerID'),
            'asin': review.get('asin'),
            'tokens': filtered,
            'category': category
        })

# just to understand what is happening
for r in cleaned_reviews:
    print(f"Reviewer: {r['reviewerID']}, ASIN: {r['asin']}, Tokens: {r['tokens']}, Category: {r['category']}")
    break

# e.g.
# Reviewer: A3UICZSYI3ZRLO, 
# ASIN: B000005J7Z, 
# Tokens: ['heard', 'jill', 'sobule', 'radio', 'thinking', 'great', 'people', 'jill', 'sobule', 'kissed', 'girl', 'nice', 'novelty', 'weak', 'point', 'rest', 'showcases', 'masterful', 'storytelling', 'excellent', 'combining', 'great', 'rhythm', 'melody', 'placing', 'nice', 'stand', 'top', 'unique', 'voice', 'stands', 'works'], 
# Category: CDs_and_Vinyl