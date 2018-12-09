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
**toDF()** : toDF() Returns a new DataFrame with columns renamed. It can be quite convenient in conversion from a RDD of tuples into a DataFrame with meaningful names.
```scala
val car = sc.textFile("src/main/resources/flowers.txt")
      .map(_.split(","))
      .map(f => Flower(f(0).trim.toInt, f(1), f(2).trim.toInt))
      .toDF().show()
```
