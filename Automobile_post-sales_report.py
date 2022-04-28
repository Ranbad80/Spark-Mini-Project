from pyspark import SparkContext
from operator import add

def extract_vin_key_value(line:str):
    raw_line = line.split(",")
    vin_number = raw_line[2]
    inc_type = raw_line[1]
    make_val = raw_line[3]
    year_val = raw_line[5]
    value = (make_val, year_val,inc_type)
    return (vin_number, value)

def populate_make(vals):

    level_master_info = []
    for val in vals:
        # iterate through the list and append if make value exists
        if val[0] != "":
            make=val[0]
        # iterate through the list and append if year value exists
        if val[1] != "":
            year = val[1]
        # append make, year and incident_type which is 'val[2]'
        level_master_info.append((make,year,val[2]))
    return level_master_info

def extract_make_key_value(list):
    if list[2]=="A":
        make=list[0]
        year=list[1]
        key = (make, year)

        return (key, 1)
    else:
        make = list[0]
        year = list[1]
        key = (make, year)
        return (key, 0)


# Initialize SparkContext
sc = SparkContext("local", "My Application")

# Load csv file
raw_rdd = sc.textFile("input/data.csv")  # Recall that we saved the data.csv file in the HDFS directory "/user/root/test_dir/"

# Use map operation to produce PairRDD
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

# Populate make and year to all the records
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))  # Using kv[1] because we only want to iterate through all the values of the vin_number keys

# Count the number of occurence for accidents of vehicle make and year
make_kv = enhance_make.map(lambda list_val: extract_make_key_value(list_val))

# Assign final result
final_output = make_kv.reduceByKey(add).collect()
print(final_output)

# Export to text file
with open("final_output.txt", "w") as output:
    for val in final_output :
        print(val)
        output.write(str(val).strip("( )") + "\n")

# Stop Spark Application
sc.stop()


