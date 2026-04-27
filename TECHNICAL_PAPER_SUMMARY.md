# Resumen Técnico: Simulación de Degradación de Servicio para Migración Automatizada

## 1. Arquitectura del Sistema de Simulación

Se ha implementado un sistema de microservicios en un entorno de Kubernetes para simular y cuantificar la degradación del rendimiento bajo condiciones de contención de recursos de CPU. El sistema está compuesto por un único **Deployment** que encapsula una aplicación de prueba, la cual consta de dos contenedores co-ubicados en el mismo Pod, permitiendo una comunicación de baja latencia a través de la interfaz de red `localhost`.

1.  **Contenedor Servidor (`image-processor-container`):**
    *   **Función:** Emula un microservicio de cómputo intensivo, representativo de cargas de trabajo como el procesamiento de imágenes, análisis de video o inferencia de modelos de Machine Learning.
    *   **Implementación:** Un servidor HTTP que expone un endpoint `POST /process`. Al recibir una petición, ejecuta una carga de trabajo sintética diseñada para ser computacionalmente costosa (basada en operaciones de punto flotante como `math.sqrt` y `math.log` en un bucle extenso).
    *   **Instrumentación:** El servidor está instrumentado con la librería OpenTelemetry para medir y exportar métricas clave. La métrica principal es **`app_qos`**, un indicador de Calidad de Servicio (QoS) normalizado (0-100), que se calcula en función de la latencia de ejecución de la carga de trabajo.

2.  **Contenedor Cliente (`client-container`):**
    *   **Función:** Simula un cliente o un servicio dependiente que consume el microservicio de procesamiento.
    *   **Implementación:** Un script que envía peticiones `POST` periódicas al endpoint `/process` del contenedor servidor.
    *   **Monitorización:** Mide y registra en los logs el **Tiempo de Respuesta Total (Round-Trip Time, RTT)** de cada transacción. Este RTT es la métrica fundamental que representa la latencia percibida por el consumidor del servicio.

## 2. Metodología de Medición de la Calidad de Servicio (QoS)

Para obtener una señal procesable que represente la salud de la aplicación, se ha diseñado una métrica de **QoS basada en la latencia de ejecución (`app_qos`)**. Esta métrica es calculada por el propio servidor después de cada tarea.

La fórmula de QoS se basa en dos umbrales configurables:

*   **Umbral de Latencia Saludable (`LATENCY_HEALTHY_SECONDS`):** Un tiempo de ejecución por debajo del cual se considera que el servicio opera con un QoS del 100%.
*   **Umbral de Latencia Crítica (`LATENCY_CRITICAL_SECONDS`):** Un tiempo de ejecución por encima del cual el servicio se considera degradado, y el QoS es del 0%.

Entre estos dos umbrales, el valor de `app_qos` se interpola linealmente, proporcionando una medida cuantitativa y continua de la degradación del rendimiento.

## 3. Simulación del Evento de Contención de Recursos

Para inducir la degradación del servicio, se introduce un pod disruptivo (`cpu-wave`) en el mismo nodo que el pod de la aplicación. Este pod está configurado con una **`PriorityClass`** de Kubernetes más alta y una solicitud de recursos de CPU que satura el nodo.

Este diseño asegura que, en un escenario de contención, el planificador de Kubernetes (scheduler) priorice al pod `cpu-wave`, reduciendo forzosamente el tiempo de CPU asignado al pod de la aplicación. Este fenómeno, conocido como **CPU Throttling**, es la causa directa de la degradación del rendimiento.

## 4. Resultados y Observaciones

El experimento permite observar dos efectos correlacionados:

1.  **Aumento de la Latencia Percibida:** Los logs del `client-container` muestran un incremento drástico en el **RTT**, pasando de un estado base estable a tiempos de respuesta elevados e incluso timeouts.
2.  **Caída de la Métrica de QoS:** Simultáneamente, la métrica `app_qos` exportada por el servidor y visualizada en Grafana/Prometheus cae de 100 a 0, reflejando la incapacidad del servicio para cumplir con sus objetivos de nivel de servicio (SLOs) de latencia.

## 5. Conclusión y Aplicabilidad

Este sistema demuestra de manera efectiva que una métrica de QoS basada en la latencia de la aplicación es un indicador mucho más fiable de la salud del servicio que las métricas tradicionales de uso de CPU.

La señal generada por la caída de `app_qos` es un **disparador (trigger) ideal para un agente de control automatizado**. Dicho agente podría implementar políticas de resiliencia, como la **migración proactiva de la carga de trabajo** a un nodo computacional con mayor disponibilidad de recursos, restaurando así la Calidad de Servicio y asegurando la continuidad operativa de la aplicación.
