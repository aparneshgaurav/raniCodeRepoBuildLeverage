# Databricks notebook source
#import pyspark
#from pyspark.sql import SparkSession
#spark = SparkSession.builder.appName('sparkApp').getOrCreate()
#dbfs:/FileStore/shared_uploads/agaurav@aligntech.com/checkRepo/check_csv.txt

from pyspark.sql import functions as F
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col,lit,explode,when,create_map,flatten
from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql.types import *
from pyspark.sql.functions import array
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

def convertCase(str):
    resStr=str.upper()
    return resStr 

schemaHere = StructType().add("name",StringType(),True).add("tech",StringType(),True)

df = spark.read.option("header",True).option("delimiter",",").schema(schemaHere).csv("dbfs:/FileStore/shared_uploads/agaurav@aligntech.com/checkRepo/check_csv.txt")
df.show()
df.select("name").show()
df.printSchema()
df.show()
df2=df.withColumn("newlyAddedColumn11", lit("IndianPlayers**"))
df3=df2.withColumnRenamed("tech","technology")

convertCaseUDF = udf(lambda rowOfColumn: convertCase(rowOfColumn)) 

df3.select("name",convertCaseUDF("technology")).show()
#df3.select("name","technology",array("name","technolgoy").alias("mergedColumnsNameAndTech")).show()
df3.select(df3.name,df3.technology,array(df3.name,df3.technology).alias("mergedNameAndTech")).show()

df4 = df3.withColumn("propertiesMap",create_map(
        lit("name"),col("name"),
        lit("technology"),col("technology")
        ))

df4.show()
df4.select("propertiesMap").show()
# check code git push
# check code git push another 



# COMMAND ----------

structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
df2.printSchema()
df2.show(truncate=False)
df2.show()

# COMMAND ----------

arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjectsUnExplodedNestedArray'])
df.printSchema()
df.show(truncate=False)

#explodes one last layer of arrays
df = df.select(df.name,explode(df.subjectsUnExplodedNestedArray).alias("firstLevelExplodedColumnOnSubjects"))
df.show(truncate=False)
# df = df.select(df.name,explode(df.subjects).alias("cloum"))
df.select(df.name,explode(df.firstLevelExplodedColumnOnSubjects).alias("secondLevelExplodedColumnOnSubjects")).show(truncate=False)


# #explodes one last layer of arrays
# df = df.select(df.name,explode(df.subjects).alias("firstLevelExplodedColumnOnSubjects")).show(truncate=False)

# # df = df.select(df.name,explode(df.subjects).alias("cloum"))
# df.select(df.name,explode(df.cloum).alias("secondLevelExplodedColumnOnSubjects")).show(truncate=False)

#flatten is nothing but recursive explode
#explodes totally by exploding all arrays into atomic data strucutres , commenting the below correct line 
#df.select(df.name,flatten(df.subjects)).show(truncate=False)

# COMMAND ----------

#spark data frames creation 
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

# spark = spark.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)


dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()

columns = ["language","users_count"]
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()

dfFromRDD1.show(truncate=False)
dfFromRDD2 = dfFromRDD1.withColumn("newColumn",lit("newColumnData"))
dfFromRDD2.show(truncate=False)

# COMMAND ----------

#creating dataframes from the schema for students 

data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]
#struct stands for userData
schemaOfStudent = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data2,schemaOfStudent)
df.show(truncate=False)
df.select("firstname","middlename").show(truncate=False)

# COMMAND ----------

#show method of dataframes 

from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns = ["Seqno","Quote"]
data = [("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool."),("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool."),("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool."),("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool."),("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool."),("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool."),("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool."),("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool."),("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool."),("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool.")]
#spark variable is the sparkSession reference 
df = spark.createDataFrame(data,columns)
df.show(truncate=False)

df.show(n=25,truncate=False)

# COMMAND ----------

#nested struct type

structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
df2.printSchema()
df2.show(truncate=False)
textHere = df2.show(truncate=False)
print("hello")
print(textHere)


# COMMAND ----------

#handling array types and map types in schema inside struct type 



arrayStructureSchema = StructType([
    StructField('name', StructType([
       StructField('firstname', StringType(), True),
       StructField('middlename', StringType(), True),
       StructField('lastname', StringType(), True)
       ])),
       StructField('hobbies', ArrayType(StringType()), True),
       StructField('properties', MapType(StringType(),StringType()), True)
    ])



# COMMAND ----------

#column operators

data=[("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')] 
columns=["fname","lname","id","gender"]
df=spark.createDataFrame(data,columns)



# COMMAND ----------

#column operators
#very nice here 
data=[(100,2,1),(200,3,4),(300,4,4)]
df=spark.createDataFrame(data).toDF("col1","col2","col3")

#Arthmetic operations
df.select(df.col1,df.col2,df.col3).show()
df.select(df.col1 + df.col2).show()
df.select(df.col1 - df.col2).show()
df.select(df.col2 == df.col3).show()
df.select(df.col2 < df.col3).show()

# COMMAND ----------

#columns demo
from pyspark.sql import Row
data=[Row(name="James",prop=Row(hair="black",eye="blue")),
      Row(name="Ann",prop=Row(hair="grey",eye="black"))]
df=spark.createDataFrame(data)
df.printSchema()
#root
# |-- name: string (nullable = true)
# |-- prop: struct (nullable = true)
# |    |-- hair: string (nullable = true)
# |    |-- eye: string (nullable = true)

#Access struct column
df.select(df["name"]).show()
df.select(col("name")).show()

df.select(df["prop.hair"]).show()
df.select(col("name"),col("prop.hair")).show()


# COMMAND ----------

#column operations
#select operations
from pyspark.sql.functions import col
data=[("James","Bond","1000",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",'M'),
      ("Tom Brand",None,"400",'M')] 
columns=["fname","lname","id","gender"]
df=spark.createDataFrame(data,columns)
df.show()



# df.sort((df.id.cast("int")).asc()).show()
df.select(df.fname,df.id).printSchema()
df.select(df.fname,df.id.cast("int")).printSchema()
df.sort(df.id.asc()).show()
df.sort(df.id.desc()).show()

df.filter(df.id.between(150,500)).show()
df.filter(df.gender.contains("M")).show()
df.filter(df.fname.startswith("Ja")).show()

df.select(col("fname"),col("lname")).show()
df.select("*").show()
df.select(df.columns[2:4]).show(3)

df.select(df.id.cast("int")*10000).show()
df.withColumn("idNew",col("id")*10000).withColumn("inNewOne",col("id").cast("int")*10000).show()
# df.select(df.withcolumn("id",col("id").cast("int")*10000)).df.withcolumn("country",lit("usa")).show()

df.filter(df.id=="400").show()
df.filter((df.id!="400") & (df.gender == "F")).show()


# COMMAND ----------

#struct and fetching

data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

from pyspark.sql.types import StructType,StructField, StringType        
schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])), StructField('state', StringType(), True),StructField('gender', StringType(), True)
     ])
df2 = spark.createDataFrame(data = data, schema = schema)
df2.printSchema()
df2.show(truncate=False)
df2.select(df2.name.firstname.alias("firstName") , df2.name.lastname.alias("lastName")).show()

# COMMAND ----------

#with column for column transformation along with data type conversion . 

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data=data, schema = columns)

# COMMAND ----------

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,"inner").show()

# COMMAND ----------



simpleData = [("Prince","Sales","Delhi",90000,20,10000),
("Abhijeet","Technical","Lucknow",86000,56,20000),
("Prince","Transport","Noida",81000,30,23000)]


rdd = spark.sparkContext.parallelize(simpleData)
columns = ("employee_name","department","state","salary","age","bonus");
df = rdd.toDF(columns)

df.sort(df.employee_name,df.state).show()
#second layer sorting when first layer are of same level 
df.show()

# COMMAND ----------

cat /dbfs/mnt/ccdb/databricks/new/config/redshiftconfig.ini
