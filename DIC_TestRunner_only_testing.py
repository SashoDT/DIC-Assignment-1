#!python

# runner script to run the Word Count

from MapReduce_v3 import MapReduce
from heapq import nlargest 
from collections import defaultdict 
import time

if __name__ == '__main__':
    start = time.time()
    
    n_tc = {} # A
    n_term = {} # A + B 
    n_category = {} # A + C 
    n_documents = 0 # A + B + C + D

    myjob1 = MapReduce()
    with myjob1.make_runner() as runner:
        runner.run()
        
        # Read in reducer output: 
        for key, value in myjob1.parse_output(runner.cat_output()): 
            if key[0] == "n_tc": 
                n_tc.update({(key[1], key[2]):value})
            elif key[0] == "n_term": 
                n_term.update({(key[1]):value})
            elif key[0] == "n_category": 
                n_category.update({(key[1]):value})
            elif key[0] == "n_documents": 
                n_documents = value 
                
    MRend = time.time()

    # Compute ChiSquare: 
    chiSquare = defaultdict(list) # built {c:[(chiSquare,t),...]} instead of {(t,c):chiSquare} for tactical reasons 
    for (t, c), A in n_tc.items(): 
        B = n_term.get(t, 0) - A # important to add default = 0
        C = n_category.get(c, 0) - A 
        D = n_documents - A - B - C
        statistic = (n_documents*(A*D-B*C)**2)/((A+B)*(A+C)*(B+D)*(C+D))
        chiSquare[c].append((statistic,t)) 

    # Order the terms: 
    ChiSquare_top75 = {
        c: [(t, v) for v, t in nlargest(75, valueToken)]
        for c, valueToken in chiSquare.items()
    }

    # Output (to get the txt-file, use the bash command "> output.txt"): 
    unique_terms = set()

    for key, values in sorted(ChiSquare_top75.items()): 
        print(key, end = "") 
        for t, v in values:
            print(" ", t, ":", v, sep ="", end = "")
            #terms.append(t)
            unique_terms.add(t)
        print("\n", end = "")

    #One line containing the merged dictionary (all terms space-separated and ordered alphabetically)
    print(" ".join(sorted(unique_terms)))
    
    end = time.time()
    print("start:", start)
    print("MRJob:", MRend)
    print("end:", end)
    print("MRJob Time elapsed:", MRend - start)
    print("Total Time elapsed:", end - start)
    
    