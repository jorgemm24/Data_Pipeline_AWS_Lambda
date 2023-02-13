CREATE EXTERNAL TABLE IF NOT EXISTS `db_athena`.`llamadas`
(
`Nombre servicio` string,
`ID de llamada` string,
`Modo de llamada` string,
`Id Contacto` string,
`Base de datos` string,
`Callerid2` string,`Día` string,
`Hora` string,
`Agente` string,
`Descripción del agente` string,
`Número de intentos` string,
`Tipo de llamada` string,
`Causa de colgado` string,
`Estado llamada` string,
`Fecha completa llamada` string,
`Fecha respuesta llamada` string,
`Hora respuesta llamada` string,
`Fecha completa respuesta última llamada` string,
`Fecha colgado llamada` string,`Hora colgado llamada` string,
`Fecha completa colgado llamada` string,
`Tiempo en cola llamada` string,
`Tiempo total llamada` string,
`Argumentario` string,
`Resultado` string,
`MOTIVO` string,
`SUBMOTIVO` string,
`Fecha cola` string,
`Hora cola` string,
`Fecha de tono` string,
`Hora de tono` string,
`Duración de tono` string,
`Duración de conversación` string,
`ticketsid` string,
`Tipo de Emisor` string,
`Medio de Consulta` string,
`Tipo de Documento` string,
`Nro de Documento` string,
`Celular Afiliado` string,
`Operador` string,
`Tipo de Cliente` string,
`Correo electrónico` string,
`SamId` string,
`Observaciones` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ( 'field.delim' = ';',  'quoteChar' = '"', 'escapeChar' = '\\')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://bucket-ind-2023/raw/llamadas/2023/202302/'
TBLPROPERTIES ('classification' = 'csv', "skip.header.line.count"="1");

select * from llamadas limit 50;