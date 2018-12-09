# spark

### Acción sobre dataframe iris

**show()** : If you want to see top `20 rows` of DataFrame in a tabular form then use the following command.
```scala
iris.show()
```

**show(n)** : If you want to see n rows of DataFrame in a tabular form then use the following command.
```scala
iris.show(2)
```

**take(n)** : take(n) Returns the first n rows in the DataFrame.
```scala
iris.take(2).foreach(println)
```

**count()** : Returns the number of rows.
```scala
iris.groupBy("sepal").count().show()
```

**head()** : head () is used to returns first row.
```scala
val resultHead = iris.head()

    println(resultHead.mkString(","))
```

**head(n)** : head(n) returns first n rows.
```scala
val resultHeadNo = iris.head(3)

    println(resultHeadNo.mkString(","))
```

**first()** : Returns the first row.
```scala
 val resultFirst = iris.first()

    println("fist:" + resultFirst.mkString(","))
```

**collect()** : Returns an array that contains all of Rows in this DataFrame.
```scala
val resultCollect = iris.collect()

    println(resultCollect.mkString(","))
```


### Funciones sobre dataframe iris

**printSchema()** : If you want to see the Structure (Schema) of the DataFrame, then use the following command.
```scala
iris.printSchema()
```

**toDF()** : Returns a new DataFrame with columns renamed. It can be quite convenient in conversion from a RDD of tuples into a DataFrame with meaningful names.
```scala
val flower = sc.textFile("src/main/resources/flowers.txt")
      .map(_.split(","))
      .map(f => Flower(f(0).trim.toInt, f(1), f(2).trim.toInt))
      .toDF().show()
```

**dtypes()** : Returns all column names and their data types as an array.
```scala
iris.dtypes.foreach(println)
```

**columns()** : Returns all column names as an array.
```scala
iris.columns.foreach(println)
```

**cache()** : Explicitly to store the data into memory. Or data stored in a distributed way in the memory by default.
```scala
val resultCache = iris.filter(iris("sepal") > 30)

    resultCache.cache().show()
```

### Operaciones sobre dataframe iris

**sort()** : Returns a new DataFrame sorted by the given expressions.
```scala
iris.sort($"itemNo".desc).show()
```

**orderBy()** : Returns a new DataFrame sorted by the specified column(s).
```scala
iris.orderBy(desc("sepal")).show()
```

**groupBy()** : Counting the number of cars who are of the same speed .
```scala
iris.groupBy("sepal").count().show()
```

**na()** : Returns a DataFrameNaFunctions for working with missing data.
```scala
iris.na.drop().show()
```

**as()** : Returns a new DataFrame with an alias set.
```scala
iris.select(avg($"sepal").as("avg_sepal")).show()
```

**alias()** : Returns a new DataFrame with an alias set. Same as as.
```scala
iris.select(avg($"weight").alias("avg_weight")).show()
```

**select()** : To fetch speed-column among all columns from the DataFrame.
```scala
iris.select("sepal").show()
```

**filter()** : filter the cars whose speed is greater than 30 (sepal > 30).
```scala
iris.filter(iris("sepal") > 30).show()
```

**where()** : Filters age using the given SQL expression.
```scala
iris.where($"sepal" > 30).show()
```

**agg()** : Aggregates on the entire DataFrame without groups. (1 row)
```scala
iris.agg(max($"sepal")).show()
```

**limit()** : Returns a new DataFrame by taking the first n rows.The difference between this function and head is that head returns an array while limit returns a new DataFrame.
```scala
iris.limit(3).show()
```

**unionAll()** : Returns a new DataFrame containing union of rows in this frame and another frame.
```scala
iris.unionAll(iris2).show()
```

**interserct()** : Returns a new DataFrame containing rows only in both this frame and another frame.
```scala
iris.intersect(iris2).show()
```

**except()** : Returns a new DataFrame containing rows in this frame but not in another frame.
```scala
iris.except(iris2).show()
```

**withColumn()** : Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
```scala
val coder: (Int => String) = (arg: Int) => {
      if (arg < 30) "down" else "up"
    }

    val sqlfunc = udf(coder)

    iris.withColumn("First", sqlfunc(col("sepal"))).show()
```

**withColumnRenamed()** : Returns a new DataFrame with a column renamed.
```scala
iris.withColumnRenamed("sepalo", "sepal").show()
```

**drop()** : Returns a new DataFrame with a column dropped.
```scala
iris.drop("sepal").show()
```

**dropDuplicates()** : Returns a new DataFrame that contains only the unique rows from this DataFrame. This is an alias for distinct.
```scala
iris.dropDuplicates().show()
```

**describe()** : Returns a DataFrame containing information such as number of non-null entries (count),mean, standard deviation, and minimum and maximum value for each numerical column.
```scala
iris.describe("sepal").show()
```
