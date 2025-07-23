# Sistema ETL para Análisis de Datos de Mensajería - Fast and Safe

## Descripción General del Proyecto

Este proyecto implementa un **sistema de ETL (Extract, Transform, Load)** para el análisis de datos de la empresa de mensajería "Fast and Safe". El objetivo principal es crear un **Data Warehouse** que permita a la empresa analizar sus operaciones, identificar patrones en el comportamiento de clientes y mensajeros.

### Objetivos del Sistema

El sistema está diseñado para responder preguntas críticas del negocio como:
1. Patrones temporales de demanda (meses, días, horas pico)
2. Número de servicios por cliente y periodo
3. Eficiencia de mensajeros y sedes más activas
4. Tiempos promedio de entrega y cuellos de botella en el proceso
5. Análisis de novedades e incidentes más frecuentes

---

## Arquitectura del Sistema

### Arquitectura General

El sistema sigue una **arquitectura de Data Warehouse clásica** con las siguientes capas:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│                 │    │                 │    │                 │
│   SISTEMA OLTP  │───▶│   PROCESO ETL   │───▶│  DATA WAREHOUSE │
│   (PostgreSQL)  │    │   (Python)      │    │   (SQLite)      │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Componentes Principales

#### 1. **Sistema Fuente (OLTP)**
- **Base de Datos:** PostgreSQL
- **Esquema:** Sistema transaccional de la empresa de mensajería
- **Tablas Principales:** cliente, sede, mensajero, servicio, estados_servicio, novedades

#### 2. **Proceso ETL**
- **Lenguaje:** Python 3.x
- **Framework:** pandas + SQLAlchemy
- **Módulos:** 10 scripts especializados + utilitarios

#### 3. **Data Warehouse (Destino)**
- **Motor:** SQLite
- **Modelo:** Star Schema (Esquema Estrella)
- **Archivo:** `DW_FastAndSafe.db`

### Esquema y Arquitectura del Data Warehouse

#### **Diseño Físico del Schema**

El Data Warehouse `DW_FastAndSafe` implementa una **arquitectura de esquema estrella (Star Schema)** optimizada para consultas analíticas OLAP. La arquitectura física está diseñada siguiendo los principios de Kimball para máximo rendimiento en consultas de agregación y análisis multidimensional.

#### **Arquitectura de Capas del DW**

**1. Capa de Almacenamiento (Storage Layer)**
- **Motor de Base de Datos:** SQLite 3.x
- **Tipo de Almacenamiento:** Archivo único (`DW_FastAndSafe.db`)
- **Ventajas:** 
  - Cero configuración y mantenimiento
  - Portabilidad total del DW
  - Rendimiento óptimo para análisis de volumen medio
  - Transacciones ACID completas

**2. Capa de Esquema (Schema Layer)**
- **Modelo Lógico:** Star Schema multidimensional
- **Normalización:** Dimensiones desnormalizadas (optimizadas para consultas)
- **Claves:** Sistema de claves subrogadas (surrogate keys) numéricas
- **Integridad:** Restricciones de integridad referencial entre hechos y dimensiones

**3. Capa de Índices (Index Layer)**
- **Índices Primarios:** Claves subrogadas en todas las dimensiones
- **Índices Secundarios:** Claves naturales para lookups ETL
- **Índices Compuestos:** Optimizados para consultas frecuentes en tabla de hechos
- **Índices Dimensionales:** Foreign keys en tabla de hechos para JOINs eficientes

#### **Diseño de Almacenamiento Físico**

**Estrategia de Particionamiento:**
- **Tabla de Hechos:** Sin particionamiento (volumen manejable en SQLite)
- **Dimensiones:** Tablas compactas, carga completa en memoria durante consultas
- **Estrategia de Crecimiento:** Diseñado para escalar horizontalmente mediante archivos separados por periodo

**Optimizaciones de Almacenamiento:**
```sql
-- Ejemplo de estructura optimizada
CREATE TABLE Fact_Cambio_Estado_Servicio (
    Servicio_Estado_Key INTEGER PRIMARY KEY AUTOINCREMENT,
    Fecha_Key INTEGER NOT NULL,
    Hora_Key INTEGER NOT NULL,
    Cliente_Key INTEGER NOT NULL,
    -- [Foreign Keys adicionales]
    Timestamp_Estado DATETIME NOT NULL,
    Contador_Estados INTEGER DEFAULT 1,
    
    -- Índices optimizados para consultas analíticas
    INDEX idx_fecha_cliente (Fecha_Key, Cliente_Key),
    INDEX idx_servicio_timestamp (Servicio_ID_Operacional, Timestamp_Estado),
    INDEX idx_mensajero_estado (Mensajero_Key, Estado_Servicio_Key)
);
```

#### **Arquitectura de Metadatos**

**Gestión de Metadatos:**
- **Linaje de Datos:** Cada tabla incluye timestamps de ETL
- **Versionado:** Preparado para SCD con campos de validez temporal
- **Documentación:** Esquema DBML como documentación técnica
- **Calidad:** Validaciones de integridad durante proceso ETL

**Catálogo de Datos:**
```
DW_FastAndSafe.db
├── Dimensiones Conformadas (9 tablas)
│   ├── Dim_Fecha (1,096 registros estimados)
│   ├── Dim_Hora (1,440 registros)
│   ├── Dim_Cliente (variable según negocio)
│   ├── Dim_Geografia (ciudades colombianas)
│   ├── Dim_Sede (por cliente y versión)
│   ├── Dim_Mensajero (por versión)
│   ├── Dim_Urgencia_Servicio (~3-5 registros)
│   ├── Dim_Estado_Servicio (~5 registros)
│   └── Dim_Novedad (variable + 'Sin Novedad')
│
└── Tabla de Hechos (1 tabla)
    └── Fact_Cambio_Estado_Servicio (alta cardinalidad)
```

#### **Consideraciones Arquitectónicas**

**Escalabilidad:**
- **Vertical:** SQLite soporta bases de datos hasta 281 TB
- **Horizontal:** Preparado para migración a PostgreSQL/SQL Server
- **Temporal:** Particionamiento futuro por año/mes si es necesario

**Rendimiento:**
- **Consultas Analíticas:** Optimizado para agregaciones y GROUP BY
- **Memoria:** Dimensiones pequeñas cargables en memoria
- **Concurrencia:** Múltiples lectores simultáneos (típico en BI)

**Mantenibilidad:**
- **Backup:** Archivo único fácil de respaldar
- **Migración:** Schema portable entre diferentes motores SQL
- **Debugging:** Herramientas estándar de SQLite disponibles

**Seguridad:**
- **Acceso:** Control a nivel de sistema operativo
- **Cifrado:** Disponible mediante extensiones SQLite
- **Auditoría:** Logs de ETL para rastreo de cambios

### Modelo Dimensional (Star Schema)

El Data Warehouse implementa un **modelo estrella** optimizado para consultas analíticas con **granularidad transaccional** a nivel de cambio de estado de servicio.

#### **Dimensiones Conformadas:**

##### **Dimensiones Temporales (Conformed Dimensions)**
- **`Dim_Fecha`** - *Dimensión de Tiempo (Granularidad: Día)*
  - **Tipo:** Dimensión estática generada programáticamente
  - **Granularidad:** Diaria (2023-2025, 1,096 registros)
  - **Atributos jerárquicos:** Año → Trimestre → Mes → Día
  - **Características especiales:** Soporte para análisis de patrones estacionales y ciclos de negocio

- **`Dim_Hora`** - *Dimensión de Tiempo (Granularidad: Minuto)*
  - **Tipo:** Dimensión estática generada programáticamente
  - **Granularidad:** Por minuto (1,440 registros por día)
  - **Atributos calculados:** Franjas horarias de negocio (Madrugada, Mañana, Tarde, Noche)
  - **Uso:** Análisis de patrones de actividad intradiarios

##### **Dimensiones de Entidades de Negocio**
- **`Dim_Cliente`** - *Dimensión SCD Tipo 2*
  - **Granularidad:** Un registro por cliente por versión
  - **Slowly Changing Dimension:** Tipo 2 preparado (campos de versionado temporal)
  - **Clave natural:** `Cliente_ID_Operacional`
  - **Atributos de negocio:** Nombre, industria/sector

- **`Dim_Geografia`** - *Dimensión de Referencia*
  - **Tipo:** Dimensión de lookup geográfico
  - **Granularidad:** Ciudad como nivel más bajo
  - **Jerarquía:** País → Departamento → Ciudad
  - **Enriquecimiento:** País fijo ('Colombia') agregado durante ETL

- **`Dim_Sede`** - *Dimensión SCD Tipo 2 con Referencias*
  - **Granularidad:** Una sede por cliente por versión
  - **Dependencias:** FK a `Dim_Cliente`, FK a `Dim_Geografia`
  - **Tipo:** SCD Tipo 2 preparado
  - **Role-playing:** Actúa como origen en la tabla de hechos

- **`Dim_Mensajero`** - *Dimensión SCD Tipo 2 con Lógica de Negocio*
  - **Granularidad:** Un mensajero por versión
  - **Atributo calculado:** Vehículo más frecuente (derivado de historial de servicios)
  - **Lógica ETL compleja:** CTE para determinar vehículo predominante por mensajero
  - **Manejo de nulos:** 'No asignado' para casos sin vehículo definido

##### **Dimensiones de Proceso de Negocio**
- **`Dim_Urgencia_Servicio`** - *Dimensión de Clasificación*
  - **Granularidad:** Un registro por tipo de urgencia
  - **Enriquecimiento:** Categorización automática (Alta/Media/Baja) basada en reglas de negocio
  - **Mapeo semántico:** Descripción textual → Categoría estandarizada

- **`Dim_Estado_Servicio`** - *Dimensión de Flujo de Proceso*
  - **Granularidad:** Un estado por etapa del proceso
  - **Atributo clave:** `Orden_Estado` preserva secuencia del proceso
  - **Flujo:** Iniciado → Con mensajero asignado → Recogido → Entregado → Cerrado

- **`Dim_Novedad`** - *Dimensión de Incidentes con Miembro Especial*
  - **Granularidad:** Un tipo de novedad por registro
  - **Miembro especial:** 'Sin Novedad' (ID = -1) para servicios sin incidentes
  - **Propósito:** Evitar NULLs en tabla de hechos, simplifica consultas analíticas

#### **Tabla de Hechos Central:**

##### **`Fact_Cambio_Estado_Servicio`** - *Tabla de Hechos Transaccional*

**Características del Modelo:**
- **Tipo:** Accumulating Snapshot Fact Table
- **Granularidad:** Un registro por cada cambio de estado de un servicio específico
- **Cardinalidad esperada:** Alta (múltiples estados por servicio)

**Dimensiones Conformadas (Foreign Keys):**
- `Fecha_Key` → `Dim_Fecha` (cuándo ocurrió el cambio)
- `Hora_Key` → `Dim_Hora` (hora específica del cambio)
- `Cliente_Key` → `Dim_Cliente` (cliente propietario del servicio)
- `Sede_Origen_Key` → `Dim_Sede` (role-playing: sede de origen)
- `Geografia_Destino_Key` → `Dim_Geografia` (role-playing: destino)
- `Mensajero_Key` → `Dim_Mensajero` (NULL permitido para estado "Iniciado")
- `Estado_Servicio_Key` → `Dim_Estado_Servicio` (estado actual)
- `Urgencia_Servicio_Key` → `Dim_Urgencia_Servicio` (nivel de servicio)
- `Novedad_Key` → `Dim_Novedad` (incidente asociado, 'Sin Novedad' por defecto)

**Dimensiones Degeneradas:**
- `Servicio_ID_Operacional` - Agrupa estados del mismo servicio (clave natural)
- `Direccion_Destino` - Dirección específica de entrega (alta cardinalidad)

**Medidas y Métricas:**
- `Contador_Estados` - Medida aditiva (valor = 1) para contar transiciones
- `Timestamp_Estado` - Timestamp exacto para análisis de duración y secuencia

**Índices de Rendimiento:**
- Índice compuesto: (Servicio_ID_Operacional, Timestamp_Estado)
- Índices dimensionales: Fecha_Key, Cliente_Key, Mensajero_Key, Estado_Servicio_Key

**Casos de Uso Analíticos:**
- **Análisis de flujo:** Seguimiento de servicios a través de estados
- **Métricas de tiempo:** Duración entre estados, tiempo total de servicio
- **Análisis de eficiencia:** Servicios por mensajero, tiempo por estado
- **Detección de patrones:** Novedades más frecuentes, cuellos de botella operacionales

---

## Enfoque Técnico Utilizado

### Metodología ETL

#### **1. Extract (Extracción)**
- **Conexión directa** a la base de datos PostgreSQL del sistema operacional
- **Consultas SQL optimizadas** para extraer solo los datos necesarios
- **Manejo de relaciones complejas** mediante JOINs y CTEs (Common Table Expressions)

#### **2. Transform (Transformación)**
- **Generación de dimensiones temporales** programáticamente
- **Lookups de claves** para convertir IDs operacionales en claves subrogadas
- **Enriquecimiento de datos** con reglas de negocio
- **Normalización** y estandarización de datos
- **Creación de métricas calculadas**

#### **3. Load (Carga)**
- **Carga por lotes** (batch processing) usando pandas
- **Reemplazo completo** de tablas en cada ejecución
- **Establecimiento automático** de claves primarias e índices

### Patrones de Diseño Implementados

#### **1. Slowly Changing Dimensions (SCD)**
- Preparado para **SCD Tipo 2** en dimensiones como Cliente, Sede y Mensajero
- Campos de versionado temporal incluidos en el esquema

#### **2. Role-Playing Dimensions**
- La dimensión `Dim_Sede` actúa como origen (`Sede_Origen_Key`)
- La dimensión `Dim_Geografia` actúa como destino (`Geografia_Destino_Key`)

#### **3. Accumulated Snapshot**
- La tabla de hechos registra **cada cambio de estado** de un servicio
- Permite análisis de la evolución temporal de los servicios

#### **4. Surrogate Keys**
- **Claves subrogadas** numéricas autoincrementales en todas las dimensiones
- Independencia de los sistemas fuente y mejor rendimiento

---

## Información Técnica

### Stack Tecnológico

#### **Lenguajes y Frameworks**
- **Python 3.x** - Lenguaje principal del ETL
- **pandas** - Manipulación y análisis de datos
- **SQLAlchemy** - ORM y manejo de conexiones a bases de datos
- **NumPy** - Operaciones numéricas

#### **Bases de Datos**
- **PostgreSQL** - Sistema fuente (OLTP)
  - Host: localhost:5432
  - Base de datos: DW_FastAndSafe
- **SQLite** - Data Warehouse (OLAP)
  - Archivo: DW_FastAndSafe.db

#### **Herramientas de Visualización** (Opcional)
- **matplotlib** - Gráficos básicos
- **seaborn** - Visualizaciones estadísticas

### Estructura del Código

```
src/
├── etl/                           # Scripts ETL individuales
│   ├── 01_dim_fecha.py           # Dimensión de fechas
│   ├── 02_dim_hora.py            # Dimensión de horas
│   ├── 03_dim_cliente.py         # Dimensión de clientes
│   ├── 04_dim_geografia.py       # Dimensión geográfica
│   ├── 05_dim_sede.py            # Dimensión de sedes
│   ├── 06_dim_mensajero.py       # Dimensión de mensajeros
│   ├── 07_dim_urgencia_servicio.py # Urgencia de servicios
│   ├── 08_dim_estado_servicio.py # Estados de servicios
│   ├── 09_dim_novedad.py         # Tipos de novedades
│   └── 10_fact_cambio_estado_servicio.py # Tabla de hechos
├── utils/
│   └── db_connections.py         # Utilidades de conexión
└── run_etl.py                    # Orquestador principal
```

### Características Técnicas Clave

#### **1. Modularidad**
- Cada dimensión en un script independiente
- Reutilización de funciones utilitarias
- Separación clara de responsabilidades

#### **2. Robustez**
- **Manejo de errores** en cada paso del proceso
- **Validación de integridad** referencial
- **Logging detallado** del progreso

#### **3. Escalabilidad**
- **Carga por chunks** (1000 registros por lote)
- **Lookups en memoria** para optimizar rendimiento
- **Diseño extensible** para nuevas dimensiones

#### **4. Mantenibilidad**
- **Código autodocumentado** con comentarios extensivos
- **Convenciones consistentes** de nomenclatura
- **Configuración centralizada** de conexiones

### Consideraciones de Rendimiento

- **Índices automáticos** en claves primarias
- **Carga por lotes** para grandes volúmenes
- **Consultas optimizadas** con JOINs eficientes
- **Lookups en memoria** para transformaciones rápidas

### Seguridad y Configuración

- **Credenciales centralizadas** en el módulo de conexiones
- **Conexiones parametrizadas** para diferentes entornos
- **Validación de datos** antes de la carga

---

## Ejecución del Sistema

### Comando Principal
```bash
python src/run_etl.py
```

### Orden de Ejecución
1. **Dimensiones independientes** (Fecha, Hora)
2. **Dimensiones base** (Cliente, Geografía)
3. **Dimensiones con dependencias** (Sede, Mensajero, etc.)
4. **Tabla de hechos** (requiere todas las dimensiones)

### Resultados
- **Data Warehouse** completo en `DW_FastAndSafe.db`
- **10 tablas dimensionales** + **1 tabla de hechos**
- **Base para análisis** y reportes de business intelligence

---

## Casos de Uso y Análisis Posibles

El Data Warehouse resultante permite realizar análisis como:

- **Análisis temporal:** Patrones de demanda por época del año
- **Análisis geográfico:** Rutas más utilizadas y ciudades con mayor actividad
- **Análisis operacional:** Eficiencia de mensajeros y tiempos de entrega
- **Análisis de calidad:** Incidentes más frecuentes y áreas de mejora
- **Análisis de clientes:** Comportamiento y patrones de uso por segmento

Este sistema proporciona una base sólida para la toma de decisiones basada en datos y la optimización continua de las operaciones de mensajería.
