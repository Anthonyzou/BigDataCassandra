# Finds how many entries are filled per 1000 columns in the table.

# Get information from table data and labels.
# I dunno why there is one less column than label though...
with open("partial_data_table1.txt") as pdt:
    stuff = [tuple(line.strip().split(";")) for line in pdt]

with open("bigdata_setup1.sql") as labelfile:
    labels = [line.strip() for line in labelfile]
labels = labels[5:-1]

# count instances of each column entry
counter = [0] * len(stuff[0])
for line in stuff:
    for index, element in enumerate(line):
        if element:
            counter[index] += 1

# print frequency (out of 1000) for each label
for i in range(len(labels)):
    print(labels[i] + str(counter[i]))
