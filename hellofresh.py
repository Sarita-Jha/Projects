from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func

######### Task 1 - Read/Preprocess the Data ##########

spark = SparkSession.builder.appName('hellofresh').getOrCreate()
df = spark.read.json("file:///C:/Users/shubh/Downloads/data-engineering-test-master/data-engineering-test-master/input")

df.show(1)
print(df.count())

######### Task 2 ########################################

df.createOrReplaceTempView("recepies")

beefDF = spark.sql("select * from recepies where ingredients rlike '.*[b|B]eef.*'")

beefDF.show()
print(beefDF.count())

def time_taken(x, y):

    cooktime = x[2:]
    preptime = y[2:]

    print([cooktime,preptime])

    hours_cooktime_index = None
    minutes_cooktime_index = None
    for i in range(len(cooktime)):
        if(cooktime[i] == 'H'):
            hours_cooktime_index = i
        if(cooktime[i] == 'M'):
            minutes_cooktime_index = i
            break
    cooktime_in_minutes = 0
    if (hours_cooktime_index):
        cooktime_in_minutes += int(cooktime[:hours_cooktime_index]) * 60
    if(minutes_cooktime_index):
        if (hours_cooktime_index):
            cooktime_in_minutes += int(cooktime[hours_cooktime_index+1:minutes_cooktime_index])
        else:
            cooktime_in_minutes += int(cooktime[:minutes_cooktime_index])
    elif(len(cooktime[hours_cooktime_index+1:])>0):
        cooktime_in_minutes += int(cooktime[hours_cooktime_index+1:])
    
    hours_preptime_index = None
    minutes_preptime_index = None
    for i in range(len(preptime)):
        if(preptime[i] == 'H'):
            hours_preptime_index = i
        if(preptime[i] == 'M'):
            minutes_preptime_index = i
            break
    preptime_in_minutes = 0
    if (hours_preptime_index):
        preptime_in_minutes += int(preptime[:hours_preptime_index]) * 60
    if(minutes_preptime_index):
        if(hours_preptime_index):
            preptime_in_minutes += int(preptime[hours_preptime_index+1:minutes_preptime_index])
        else:
            preptime_in_minutes += int(preptime[:minutes_preptime_index]) 
    elif(len(preptime[hours_preptime_index+1:])>0):
        preptime_in_minutes += int(preptime[hours_preptime_index+1:])
    return cooktime_in_minutes+preptime_in_minutes

def calculate_difficulty(x):
    if (x < 30):
        return "Easy"
    elif (x >= 30 and x <= 60):
        return "Medium"
    else:
        return "Hard"

total_time_udf = func.udf(lambda x, y: time_taken(x, y))
difficulty_udf = func.udf(lambda x: calculate_difficulty(x))

DF_with_total_cookingtime = beefDF.withColumn("totalcooktime", total_time_udf(func.col("cookTime"), func.col("prepTime")))
df_with_difficulty = DF_with_total_cookingtime.withColumn("Difficulty", difficulty_udf(func.col("totalcooktime")))

finalDF = df_with_difficulty.createOrReplaceTempView('totaltime')

final_view = spark.sql('select Difficulty, totalcooktime as avg_total_cooking_time from totaltime')

final_view.show()


#df.write.csv('file:///C:/Users/shubh/Downloads/data-engineering-test-master/data-engineering-test-master/output/out.csv')