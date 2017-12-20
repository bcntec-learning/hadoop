Replica: copiar el contenido de la fila de una tabla en la celda de una columna de una tabla "maestra"
-----------------------------------------------------------------------------------
Para cada Put en la tabla A (con una clave de fila "\ <rowid \>"), una celda se coloca en una tabla maestra con la clave de fila "\ <Id. De fila \>", calificador "\ <Id. De fila \>" y valorar una concatenación de calificadores y valores de la fila en A. El nombre de la tabla maestra y la familia de columnas se pasan como argumentos.
Para cada Delete en la tabla A (con una clave de fila "\ <rowid \>"), solo si la fila en A no está vacía, funciona como el caso Put. De lo contrario, la columna en la tabla maestra se elimina.

### Cómo configurar el observador en una tabla llamada 'test_observer'

Primero cargue el contenedor en algún lugar de HDFS.
Inicie hbase-shell y cree una tabla 'test_replica_child' y una tabla 'test_replica_master', que es la tabla maestra para 'test_replica_child'.
    hbase(main):049:0> alter 'test_replica_child', METHOD => 'table_att', 'coprocessor' => 'hdfs:///user/test/hbase-observer-0.1.jar|houseware.learn.hadoop.mapred.hbase.observer.Replica||master=test_replica_master,family=child'

### Cómo desarmar al observador en una tabla llamada'test_observer'

    hbase(main):049:0> alter 'test_replica_child', METHOD => 'table_att_unset', NAME => 'coprocessor$1'
    
Tenga en cuenta que si realiza cambios en la implementación y necesita actualizar el jar, debe cambiar el nombre del jar para que HBase cargue la nueva versión. No es suficiente reemplazar el jar en HDFS y desarmar / configurar el mismo coprocesador. Aparentemente HBase guarda un caché del archivo jar del coprocesador.