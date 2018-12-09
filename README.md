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
