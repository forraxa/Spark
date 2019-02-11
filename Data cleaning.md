## Data Cleaning
The steps are

- remove punctuation, by replace "[^A-Za-z0-9\s]+" with "", or not include numbers "[^A-Za-z\s]+"
- trim all spaces
- lower all words
- remove stop words

Code as follows

```scala
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions.split

// val reg = raw"[^A-Za-z0-9\s]+" // with numbers

val reg = raw"[^A-Za-z\s]+" // no numbers
val lines = sc.textFile("peter.txt").
    map(_.replaceAll(reg, "").trim.toLowerCase).toDF("line")
val words = lines.select(split($"line", " ").alias("words"))

val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")

val noStopWords = remover.transform(words)

val counts = noStopWords.select(explode($"filtered")).map(word =>(word, 1))
    .reduceByKey(_+_)

// from word -> num to num -> word
val mostCommon = counts.map(p => (p._2, p._1)).sortByKey(false, 1)

mostCommon.take(5)
```
___  
Otra posibilidad de aplicar un patron
```
val changeFlatMap = changeFile.flatMap("[a-z]+".r findAllIn _)
```
