
#####Ir al directorio
```cd /home/hadoop/share/01-mapred-wordcount```

#####Construimos de trabajo directorio
```hadoop fs -mkdir /ejemplos/```

#####Construimos de trabajo directorio
```hadoop fs -mkdir /ejemplos/01-mapred-wordcount```
```hadoop fs -mkdir /ejemplos/01-mapred-wordcount/input```


#####Copiamos fichero
```hadoop fs -copyFromLocal ./data/El_Senor_de_los_Anillos.txt /ejemplos/01-mapred-wordcount/input```
```hadoop fs -ls /ejemplos/01-mapred-wordcount/input```

```hadoop jar ./target/01-mapred-wordcount-1.0-SNAPSHOT.jar houseware.learn.hadoop.mapred.wordcount.WordCounter2  /ejemplos/01-mapred-wordcount/input /ejemplos/01-mapred-wordcount/output ​```


##### Visulizar ficheros
```hadoop fs -cat /ejemplos/01-mapred-wordcount/output/_SUCCESS```
```hadoop fs -cat /ejemplos/01-mapred-wordcount/output/part-r-00000```