# Sistema para analizar datos de una empresa de mensajería

## Descripción del negocio

La empresa de mensajería "Fast and Safe" desea realizar un sistema de analítica de datos para apoyar el proceso de toma de decisiones en los servicios de mensajería que presta. La empresa presta servicios de mensajería a otras empresas en varias ciudades de Colombia. La empresa ya tiene un sistema de información operacional que le permite manejar los clientes y servicios.

## Estructura organizacional

La empresa "Fast and Safe" tiene varios clientes y cada cliente puede tener varias sedes físicas de donde se piden los servicios. Cuando un usuario pide un servicio usa la aplicación web donde solicita el servicio. Cada vez que se solicita un servicio, el cliente especifica la ciudad de origen y destino, la dirección de origen y destino del servicio. También se especifica el tiempo en que desea recibir el pedido, el cual puede ser urgente, con menos de una hora, entre 2 y 3 horas, o en el transcurso del día.

## Estados del servicio

El servicio tiene varios estados: iniciado, Con mensajero asignado, recogido en origen, Entregado en Destino, Cerrado. Cada vez que se solicita un servicio se toma por defecto el estado "Iniciado".

## Proceso operativo

Cuando se registra un nuevo servicio, la aplicación envía un mensaje a los diferentes mensajeros. Luego los mensajeros ven el servicio solicitado por medio de una aplicación móvil y lo toman. Cuando un mensajero toma el servicio este pasa a un estado de "Con mensajero asignado". Después de esto, el mensajero recoge el servicio y lo entrega en su destino. Si todo sale bien, el mensajero cierra el servicio con éxito.

## Gestión de novedades

Si ocurre algo diferente en el flujo del servicio, el mensajero puede reportar novedades en el sistema operacional. Una novedad es cuando le ocurre algo al mensajero y esto cambia la hora de entrega programada. Por ejemplo, la moto del mensajero puede sufrir un daño, o el cliente se demora al empacar lo que se desea enviar. En todo caso, la aplicación permite registrar este tipo de novedades. La aplicación operacional le permite al cliente consultar en qué estado se encuentra el envío.

## Problemática actual

Durante los últimos años el negocio de la mensajería ha crecido y la empresa ya tiene varios clientes y mensajeros. Sin embargo, esto ha creado nuevos retos ya que los clientes se están quejando por las demoras en las entregas de pedidos y los mensajeros también se quejan de ciertos clientes. Con el objetivo mejorar la prestación de servicios de mensajería, la empresa "Fast and Safe" quiere hacer un sistema de bodegas de datos que le permita analizar la información que generan los clientes y mensajeros.

## Preguntas de análisis

En general, se desea responder las siguientes preguntas:

1. En qué meses del año los clientes solicitan más servicios de mensajería

2. Cuáles son los días donde más solicitudes hay

3. A qué hora los mensajeros están más ocupados

4. Número de servicios solicitados por cliente y por mes

5. Mensajeros más eficientes (Los que más servicios prestan)

6. Cuáles son las sedes que más servicios solicitan por cada cliente

7. Cuál es el tiempo promedio de entrega desde que se solicita el servicio hasta que se cierra el caso

8. Mostrar los tiempos de espera por cada fase del servicio: Iniciado, Con mensajero asignado, recogido en origen, Entregado en Destino, Cerrado. En que fase del servicio hay más demoras?

9. Cuáles son las novedades que más se presentan durante la prestación del servicio?