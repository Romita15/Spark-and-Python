from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("AmountSpentbyCust")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    return (int(fields[0]),float(fields[2]))
    
lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLine)
totalAmount = parsedLines.reduceByKey(lambda x,y : x+y)
sortedAmount = totalAmount.map( lambda x :(x[1],x[0])).sortByKey(ascending = False)
results = sortedAmount.collect()
for result in results:
    print("Amount:{:.2f}\t Customer ID:{}".format(result[0],result[1]))
    
    