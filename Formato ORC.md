## Formato ORC 
Formato de archivo columnar optimizado para lecturas en Hadoop.  

```
// Save and Overwrite our new DataFrame to an ORC file
flightsWithDelays.write.format("orc").mode(SaveMode.Overwrite).save("flightsWithDelays.orc")
```


MODE (SCALA/JAVA) |	MEANING
-|-
SaveMode.ErrorIfExists (default)	| When saving a DataFrame to a data source, if data already exists, an exception is expected to be thrown.
SaveMode.Append	| When saving a DataFrame to a data source, if data/table already exists, contents of the DataFrame are expected to be appended to existing data.
SaveMode.Overwrite	| Overwrite mode means that when saving a DataFrame to a data source, if data/table already exists, existing data is expected to be overwritten by the contents of the DataFrame.
SaveMode.Ignore	| Ignore mode means that when saving a DataFrame to a data source, if data already exists, the save operation is expected to not save the contents of the DataFrame and to not change the existing data. This is similar to a CREATE TABLE IF NOT EXISTS in SQL.

