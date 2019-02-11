## imports en spark

- import org.apache.spark.{SparkConf,SparkContext}  
- import org.apache.spark.storage.StorageLevel  
- import org.apache.spark.streaming.{Seconds, StreamingContext, Time}  
- import org.apache.spark.sql.SparkSession // Para abrir una sessión.  
- import org.apache.spark.sql.types.{StructType, StructField, LongType} // y otros  
- import spark.implicits._ // Después de crear un SparkSession se importa implicits para poder 
                              convertir objetos Scala en Dataframes  

