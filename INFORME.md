# Informe - TP Coordinacion

> **Alumno:** Nicolas Chen
> **Padron:** 105907

En este informe se describen de forma breve los problemas encontrados y las soluciones aplicadas en cada punto del trabajo practico.

## Temas a cubrir:

- Coordinacion entre instancias de Sum y Aggregation
- Escalabilidad respecto a los Clientes
- Escalabilidad respecto a Grandes Volumenes de Datos
- Escalabilidad respecto a la Cantidad de Controles
- Cierre Ordenado del Sistema

## 1. Coordinacion entre instancias de Sum y Aggregation

Como varias instancias de Sum consumen de la misma cola `input_queue`, el mensaje de fin (EOF) enviado por el Gateway solo le llegaba a una de ellas. Las demas instancias quedaban esperando datos sin enviar sus acumuladores. A su vez, Aggregation no tenia forma de saber cuando habian terminado todos los Sum para poder calcular su top.

Para solucionarlo, el primer Sum que recibe el EOF lo retransmite a un exchange de control directo (`<SUM_PREFIX>_control`), notificando a todos los demas Sum para que envien sus datos y su propio EOF hacia Aggregation. En Aggregation se agrego un contador de EOFs: espera a recibir un EOF de cada Sum (`SUM_AMOUNT`) antes de calcular su top y reenviarlo. Join hace lo mismo esperando a todos los Aggregation (`AGGREGATION_AMOUNT`) para calcular el top final.

## 2. Escalabilidad respecto a los Clientes

Los nodos acumulaban las frutas de todos los clientes en una misma estructura y el Gateway no podia distinguir a que cliente pertenecia cada respuesta devuelta por Join.

Para resolver esto, el Gateway asigna un identificador numerico unico (`ClientID`) a cada cliente. Este ID viaja en cada mensaje en formato `[clientId, records]`. Los nodos Sum, Aggregation y Join guardan sus datos en memoria separados por cliente (`map[ClientID]...`). Al terminar de procesar un cliente, liberan esa memoria. El Gateway filtra los mensajes de la cola final por `ClientID` para responder por el socket correspondiente.

## 3. Escalabilidad respecto a Grandes Volumenes de Datos

Sum enviaba todas las frutas a todas las instancias de Aggregation (broadcast), saturando la red y duplicando el procesamiento en cada nodo.

Se aplico particionado por fruta usando una funcion de hash: `aggregator = hash(fruta) % AGGREGATION_AMOUNT`. Cada fruta se envia unicamente al Aggregator que le corresponde. Los unicos mensajes que reciben todos los Aggregators son los mensajes de fin (EOF).

## 4. Escalabilidad respecto a la Cantidad de Controles

Tener cantidades de nodos o nombres de colas fijos en el codigo impide adaptar el sistema a diferentes cargas de trabajo.

Para permitirlo, todas las cantidades de instancias (`SUM_AMOUNT`, `AGGREGATION_AMOUNT`, `TOP_SIZE`) y nombres de colas se configuran por variables de entorno. Esto permite escalar horizontalmente agregando mas instancias de Sum para procesar mas datos de entrada en paralelo, o mas instancias de Aggregation para repartir la carga de calcular tops parciales. A su vez, Join solo recibe a lo sumo `AGGREGATION_AMOUNT * TOP_SIZE` elementos, por lo que el top final siempre se calcula de forma rapida y liviana.

## 5. Cierre Ordenado del Sistema (Graceful Shutdown)

Los nodos no manejaban las senales de apagado del sistema operativo, forzando a Docker a esperar un timeout para luego matarlos con `SIGKILL`.

Para un cierre limpio, se capturan las senales `SIGTERM` y `SIGINT` en Sum, Aggregation y Join. Al recibirlas, cada nodo cierra sus colas, exchanges y conexiones con RabbitMQ, finalizando el proceso de forma limpia e inmediata.
