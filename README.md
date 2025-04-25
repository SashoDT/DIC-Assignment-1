# DIC-Assignment-1
# Run: 
Use sth like this to run in console (change "local" if ran in lbd and add paths): 
"python DIC_runner.py -r local --stopwords stopwords.txt reviews_devset.json > word_count.txt"

How to run on lbd cluster: 
- create zip-file out of "DIC_runner", "MapReduce_v2", "stopwords.txt" and maybe small json data file into folder Assignment1 (name here: Assignment1.zip)
- upload: scp Assignment1.zip e{studentID}@lbd.tuwien.ac.at:. 
- login: ssh e{studentID}@lbd.tuwien.ac.at 
- unzip: unzip Assignment1.zip
- change directory if zip contains folder Assignment1, else skip: cd Assignment1
- list files, if needed: ls - 1
- run (small dataset): 
    python DIC_runner.py --stopwords stopwords.txt --hadoop-streaming-jar \
    /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -r hadoop hdfs:///user/dic25_shared/amazon-reviews/full/reviews_devset.json > output.txt

- run (large dataset): 
    python DIC_runner.py --stopwords stopwords.txt --hadoop-streaming-jar \
    /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -r hadoop hdfs:///user/dic25_shared/amazon-reviews/full/reviewscombined.json > output.txt
