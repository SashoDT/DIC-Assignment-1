from collections import defaultdict 
test_set = defaultdict(list)
test_list = ["b", "a", "c"]
test_list2 = [3, 2, 1]

for i in range(3): 
    for j in range(3): 
        test_set[i].append((test_list[j], test_list2[j]))

for key, values in test_set.items(): 
    print(key, end = "") 
    [print(" ", t, ":" ,v, sep ="", end = "") for t,v in values]
    print("\n", end = "")

