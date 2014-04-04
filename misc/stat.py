# Finds how many entries are filled per 1000 columns in the table.

#import pylab as pl
# Get information from table data and labels.
# I dunno why there is one less column than label though...
with open("partial_data_table1.txt") as pdt:
    stuff = [tuple(line.strip().split(";")) for line in pdt]

with open("cdr_table.sql") as labelfile:
    labels = [line.strip().rstrip(',') for line in labelfile]
labels = [label.split() for label in labels]
print labels
exit()
# get counts of each column and plot the count
count = [0] * len(stuff[0])
for line in stuff:
    for index, element in enumerate(line):
        if element:
            count[index] += 1
labels, count = zip(*sorted(zip(labels, count), key=lambda e: -e[1]))



# plot stuffs
#pl.bar(range(len(count)), count, label=labels)
#pl.show()
