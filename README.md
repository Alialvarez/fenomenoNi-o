# Proyecto Fenómeno del Niño — Analítica del Alumbrado Público de Medellín

**Plataforma:** Databricks (Unity Catalog, Lakeflow Spark Declarative Pipelines, Lakeflow Jobs, AI/BI Dashboards)
**Arquitectura:** Medallón (Bronze → Silver → Gold)
**Dominio:** Energía — Alumbrado público de Medellín
**Fecha de última actualización:** Noviembre 2026
**Autor:** Alba Luz Preciado — Alina Maria Castañeda---

## 1. Resumen ejecutivo

Este proyecto de extremo a extremo analiza el impacto del **Fenómeno del Niño** sobre el sistema de alumbrado público de Medellín, a partir del archivo operativo `Consumo_Sep_2026_Reduccion_Horaria.xlsx`. El objetivo es cuantificar el **consumo energético**, el **costo facturado** y el **ahorro esperado** bajo distintos escenarios de **reducción horaria** (racionamiento), y proyectar el comportamiento para octubre–diciembre de 2026.

El proyecto está construido íntegramente en Databricks sobre una **arquitectura medallón** con gobierno de datos en Unity Catalog, transformaciones Silver en notebooks PySpark, modelo dimensional Gold publicado por un **pipeline SDP (Spark Declarative Pipeline)** con vistas materializadas, orquestación automática mediante un **job diario**, y visualización en un **dashboard AI/BI** de dos páginas.

**Cifras de control del modelo (septiembre 2026):**

| Métrica | Valor |
| --- | --- |
| Puntos luminosos analizados | 4,209 |
| Consumo base total | 6,886.5 kWh |
| Valor base facturado | $4,606,783.32 |
| Ahorro escenario −4h | $3,499,991.34 |
| Ajuste total vs. factura | −194,188.40 kWh / −$129,903,999.62 |
| Precio efectivo base | $668.96 / kWh |

---

## 2. Contexto de negocio

El Fenómeno del Niño reduce la capacidad hidrológica del sistema interconectado nacional, lo que obliga a la ciudad a aplicar **esquemas de reducción horaria del alumbrado público** (apagado programado de circuitos en franjas nocturnas). Cada escenario de reducción (−2h, −4h, −6h, etc.) tiene un efecto distinto sobre:

1. El **consumo mensual de energía** (kWh) por concepto de facturación.
2. El **valor facturado** ($ COP) sujeto a tarifa, IPP (Índice de Precios del Productor) y ajustes de factura.
3. El **ahorro operativo** que la ciudad obtiene al reducir horas de encendido.

La fuente (`Consumo_Sep_2026_Reduccion_Horaria.xlsx`) contiene dos hojas de cálculo:

* **Hoja 1 — "Septiembre 2026":** parámetros de facturación, ajustes de factura y consumos de referencia de puentes.
* **Hoja 2 — "Reducción horaria":** consumo, ahorro y valor por concepto para cada escenario de reducción horaria.

---

## 3. Arquitectura medallón

```
┌──────────────────────────────────────────────────────────────────────┐
│ FUENTE (Raw)                                                          │
│ Consumo_Sep_2026_Reduccion_Horaria.xlsx (Excel, 2 hojas)              │
└──────────────────────────┬───────────────────────────────────────────┘
                           │  %pip install openpyxl + pandas (parse por slices)
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│ BRONZE — workspace.fenomeno_nino_bronze                               │
│ Ingesta cruda tal cual (raw), una tabla por hoja + celdas crudas      │
│ consumo_raw_celdas · septiembre_2026_raw · reduccion_horaria_raw      │
└──────────────────────────┬───────────────────────────────────────────┘
                           │  Notebook 02_Silver: limpieza, tipado,
                           │  imputación y segmentación por slices verificados
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│ SILVER — workspace.fenomeno_nino_silver                               │
│ Datos limpios, tipados y validados (11 controles de calidad OK)       │
│ parametros · ajustes_factura · consumo_referencia_puentes ·          │
│ consumo_escenarios · ahorro_escenarios · valor_escenarios ·          │
│ supuestos_imputacion                                                  │
└──────────────────────────┬───────────────────────────────────────────┘
                           │  Pipeline SDP fenomeno_nino_gold
                           │  (vistas materializadas, serverless + Photon)
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│ GOLD — workspace.fenomeno_nino_gold                                   │
│ Modelo estrella listo para BI (10 controles de validación OK)         │
│ dim_escenario · dim_concepto ·                                        │
│ fact_consumo_escenarios · fact_ajustes_factura · fact_proyeccion_3m  │
└──────────────────────────┬───────────────────────────────────────────┘
                           │  Consulta SQL / metric views locales
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│ CONSUMO ANALÍTICO                                                     │
│ Dashboard AI/BI "fenomeno_nino_gold" (2 páginas) · Power BI (opc.)   │
└──────────────────────────────────────────────────────────────────────┘
```

**Principios aplicados en cada capa:**

* **Bronze:** ingestión inmutable y sin transformaciones lógicas — el dato llega tal como está en el Excel, con trazabilidad de fila/columna (`fila_excel`) para auditoría. Se conservan también las celdas crudas completas para re-parseo forense.
* **Silver:** tipado correcto (doubles para kWh y $, strings para conceptos), exclusión de filas de encabezado/subtotales, imputación documentada de valores faltantes y segmentación por slices de filas/columnas **verificados contra el archivo original**.
* **Gold:** modelo dimensional estrella gobernado por el pipeline (no por notebooks), con claves surrogate simples, flags `es_total`/`es_subtotal` para filtrar agregados, y hechos a granularidad concepto × escenario × periodo.

---

## 4. Estructura de carpetas del proyecto

```
fenomenoNi-o/
├── README.md                                  # Este documento
├── Consumo_Sep_2026_Reduccion_Horaria.xlsx    # Fuente de datos (raw)
├── 00_Setup/
│   ├── 00_catalog_schemas                     # Creación del catálogo y esquemas UC
│   └── 01_config                              # Variables de configuración del proyecto
├── 01_Bronze/
│   └── 01_Bronze_EDA_Consumo_Sep_2026         # Ingesta cruda + EDA de la fuente
├── 02_Silver/
│   └── 02_Silver_Limpieza_Segmentacion        # Limpieza, tipado, imputación, validación
├── 03_Gold/
│   └── 03_Gold_Modelo_Dimensional            # Definición del modelo estrella (referencia;
│                                              #  la publicación la hace el pipeline SDP)
└── .git/                                      # Repositorio Git (versionado local)
```

### 4.1 Notebooks

| Notebook | Capa | Rol | ID |
| --- | --- | --- | --- |
| `00_catalog_schemas` | Setup | Crea el catálogo `workspace` y los esquemas `fenomeno_nino_bronze`, `fenomeno_nino_silver`, `fenomeno_nino_gold` | 3350373647053504 |
| `01_config` | Setup | Centraliza rutas, nombres de tablas y parámetros del proyecto | 3350373647053505 |
| `01_Bronze_EDA_Consumo_Sep_2026` | Bronze | Lee el Excel con openpyxl/pandas, explora estructura y persiste las 3 tablas crudas | 3350373647053502 |
| `02_Silver_Limpieza_Segmentacion` | Silver | Segmenta por slices, limpia, tipa, imputa y valida con 11 controles | 3350373647053503 |
| `03_Gold_Modelo_Dimensional` | Gold | Documenta y define el modelo estrella; el pipeline SDP es el dueño de las tablas publicadas | 3350373647053506 |

> **Nota técnica:** en compute serverless, `openpyxl` **no viene preinstalado** — todo notebook que lea el Excel debe ejecutar primero `%pip install openpyxl`.

---

## 5. Catálogo y esquemas en Unity Catalog

Todo el dato está gobernado en el catálogo **`workspace`** bajo tres esquemas, uno por capa del medallón:

| Esquema | Capa | Objeto | Tipo | Filas |
| --- | --- | --- | --- | --- |
| `workspace.fenomeno_nino_bronze` | Bronze | `consumo_raw_celdas` | Tabla MANAGED | 794 |
| `workspace.fenomeno_nino_bronze` | Bronze | `septiembre_2026_raw` | Tabla MANAGED | 32 |
| `workspace.fenomeno_nino_bronze` | Bronze | `reduccion_horaria_raw` | Tabla MANAGED | 46 |
| `workspace.fenomeno_nino_silver` | Silver | `parametros` | Tabla MANAGED | 3 |
| `workspace.fenomeno_nino_silver` | Silver | `ajustes_factura` | Tabla MANAGED | 5 |
| `workspace.fenomeno_nino_silver` | Silver | `consumo_referencia_puentes` | Tabla MANAGED | 5 |
| `workspace.fenomeno_nino_silver` | Silver | `consumo_escenarios` | Tabla MANAGED | 7 |
| `workspace.fenomeno_nino_silver` | Silver | `ahorro_escenarios` | Tabla MANAGED | 7 |
| `workspace.fenomeno_nino_silver` | Silver | `valor_escenarios` | Tabla MANAGED | 7 |
| `workspace.fenomeno_nino_silver` | Silver | `supuestos_imputacion` | Tabla MANAGED | 4 |
| `workspace.fenomeno_nino_gold` | Gold | `dim_escenario` | VISTA MATERIALIZADA | 5 |
| `workspace.fenomeno_nino_gold` | Gold | `dim_concepto` | VISTA MATERIALIZADA | 7 |
| `workspace.fenomeno_nino_gold` | Gold | `fact_consumo_escenarios` | VISTA MATERIALIZADA | 35 |
| `workspace.fenomeno_nino_gold` | Gold | `fact_ajustes_factura` | VISTA MATERIALIZADA | 5 |
| `workspace.fenomeno_nino_gold` | Gold | `fact_proyeccion_3m` | VISTA MATERIALIZADA | 45 |

El esquema Gold contiene además las tablas internas del pipeline (`__materialization_*` y `event_log_*`), que son admin/archivo internas de SDP y **no deben consumirse directamente**.

---

## 6. Capa Bronze — ingesta cruda y EDA

**Notebook:** `01_Bronze_EDA_Consumo_Sep_2026`

* Carga el Excel con openpyxl (instalado on-demand) y hace un **EDA completo** de ambas hojas: dimensiones, encabezados, celdas combinadas, tipos inferidos y anomalías de formato.
* Persiste tres tablas crudas:
  * `consumo_raw_celdas` — todas las celdas crudas (794) con su coordenada (hoja, fila, columna) para trazabilidad forense y re-parseo sin reabrir el Excel.
  * `septiembre_2026_raw` — volcado de la hoja 1 (32 filas).
  * `reduccion_horaria_raw` — volcado de la hoja 2 (46 filas).
* No aplica ninguna transformación lógica: la Bronze es una **fotografía inmutable** de la fuente.

### 6.1 Slices de parseo verificados (usados en Silver)

La segmentación del Excel se validó fila por fila contra el archivo original. Se trabaja con dos grupos: `g1` = hoja "Septiembre 2026", `g2` = hoja "Reducción horaria", con índice `fila_excel`:

| Bloque de datos | Grupo | Slice (filas, columnas) | Nota |
| --- | --- | --- | --- |
| Parámetros de facturación | g1 | filas 5–7, cols c2–c4 | |
| Ajustes de factura | g1 | filas 11–15, cols c2–c8 | |
| Consumos de referencia puentes | g1 | filas 19–23, cols c2–c9 | encabezado en fila 18 — **no incluir** |
| Consumo por escenario | g2 | filas 12–18, cols c2–c11 | |
| Ahorro por escenario | g2 | filas 22–28, cols c2–c10 | |
| Valor por escenario | g2 | filas 32–38, cols c2–c11 | |

---

## 7. Capa Silver — limpieza, imputación y segmentación

**Notebook:** `02_Silver_Limpieza_Segmentacion`

Transformaciones clave:

1. **Tipado:** valores numéricos (kWh, $, %) a `double`; conceptos y descripciones a `string` normalizado (trim, sin filas vacías).
2. **Exclusión de ruido:** se separan filas de encabezado, totales y subtotales de los datos atómicos; los agregados se conservan con flags (`es_total`, `es_subtotal`) para no perder trazabilidad.
3. **Imputación documentada:** los valores faltantes o atípicos se imputan siguiendo reglas explícitas, registradas en la tabla `supuestos_imputacion` (4 filas) — cada supuesto es auditable.
4. **Persistencia:** 7 tablas limpias en `workspace.fenomeno_nino_silver`.

**Calidad de datos:** el notebook ejecuta **11 controles de validación** (tipos, nulos, rangos, coherencia de totales contra el Excel), todos en estado **OK**.

**Cifras validadas en Silver:** consumo base total **6,886.5 kWh**; valor base total **$4,606,783.32**; ahorro escenario −4h **$3,499,991.34**; ajuste TOTAL de factura **−194,188.40 kWh / −$129,903,999.62**; **4,209** puntos luminosos.

---

## 8. Capa Gold — modelo dimensional (estrella)

**Dueño de las tablas:** el pipeline SDP `fenomeno_nino_gold` (ver sección 9). El notebook `03_Gold_Modelo_Dimensional` contiene la definición de referencia de las transformaciones.

### 8.1 Diagrama estrella

```
                        ┌──────────────────┐         ┌──────────────────┐
                        │  dim_escenario   │         │   dim_concepto   │
                        │  (5 filas)       │         │   (7 filas)      │
                        │  escenario_id PK │         │  concepto (PK/nk)│
                        │  escenario       │         │  es_total        │
                        │  horas_reduccion │         │  es_subtotal     │
                        └────────┬─────────┘         └────────┬─────────┘
                                 │ 1                          │ 1
                                 │ *                          │ *
              ┌──────────────────┴────────────────────────────┴──────────────┐
              │                                                               │
              │   fact_consumo_escenarios (35)     fact_ajustes_factura (5)   │
              │   concepto · escenario_id                                   │
              │   periodo ('2026-09')                                       │
              │   consumo_kwh · valor · ahorro                              │
              │   es_total · es_subtotal                                    │
              └───────────────────────────────────────────────────────────┘
                                 │ *
              ┌──────────────────┴───────────────────────────────┐
              │  fact_proyeccion_3m (45)                          │
              │  3 meses (oct–dic 2026) × 3 escenarios de precio │
              │  × 5 escenarios de reducción                     │
              └──────────────────────────────────────────────────┘
```

### 8.2 Tablas de hechos y dimensiones

| Objeto | Granularidad | Descripción |
| --- | --- | --- |
| `dim_escenario` | 1 fila por escenario | Escenarios de reducción horaria (base, −2h, −4h, −6h, …) con `escenario_id` |
| `dim_concepto` | 1 fila por concepto | Conceptos de facturación del alumbrado público (7), con flags `es_total`/`es_subtotal` |
| `fact_consumo_escenarios` | concepto × escenario × periodo | Consumo (kWh), valor ($) y ahorro por escenario; periodo `2026-09` |
| `fact_ajustes_factura` | 1 fila por concepto de ajuste | Ajustes de factura (energía y pesos) vs. la factura de septiembre |
| `fact_proyeccion_3m` | mes × escenario precio × escenario reducción | Proyección octubre–diciembre 2026 (ver sección 11) |

**Controles de validación Gold:** 10 controles (integridad de claves, cuadre de hechos contra Silver, unicidad de dimensiones, coherencia de flags), todos **OK**.

### 8.3 Reglas de consumo para BI

* Filtrar siempre `es_total = false AND es_subtotal = false` en visuales atómicos; usar las filas de total solo para cuadres de control.
* Relaciones del modelo: `dim_escenario.escenario_id → fact_consumo_escenarios.escenario_id` y `dim_concepto.concepto → fact_consumo_escenarios.concepto`, ambas 1:*.
* Los hechos de septiembre usan `periodo = '2026-09'`.

---

## 9. Pipeline SDP — `fenomeno_nino_gold`

| Propiedad | Valor |
| --- | --- |
| **ID** | `68227fb0-10c6-414f-acfb-ec98df5d6ec7` |
| **Tipo** | WORKSPACE (Lakeflow Spark Declarative Pipeline) |
| **Fuente** | `/Workspace/Users/alba.preciado2860@unaula.edu.co/fenomeno/transformations/my_transformation.py` (glob `transformations/**`) |
| **Publicación** | Catálogo `workspace`, esquema `fenomeno_nino_gold` |
| **Compute** | Serverless, **Photon** activado, canal `CURRENT` |
| **Modo** | Production (no development), triggered (no continuous) |
| **Estado** | Última actualización `8ff26c98` **COMPLETED** (2026-11-26) |

El pipeline define **5 vistas materializadas** (`dim_escenario`, `dim_concepto`, `fact_consumo_escenarios`, `fact_ajustes_factura`, `fact_proyeccion_3m`) adaptadas del notebook 03_Gold y derivadas de la capa Silver. Cada ejecución reconstruye el modelo Gold con controles de calidad embebidos; el pipeline es el **único dueño** de las tablas Gold (las tablas creadas previamente desde el notebook fueron retiradas al consolidar la propiedad en el pipeline).

Historial relevante de actualizaciones: `626f3f78` (primera publicación completa validada, 25-nov) y `8ff26c98` (re-ejecución diaria OK, 26-nov); un intento previo `70ff1879` falló y fue corregido.

> Existe un pipeline vacío residual `New Pipeline 2026-11-25 20:05` sin uso — candidato a limpieza.

---

## 10. Job — orquestación diaria

| Propiedad | Valor |
| --- | --- |
| **ID** | `256413843831368` |
| **Nombre** | `fenomeno_nino_gold` |
| **Trigger** | Periódico, cada **1 día** (diario), **UNPAUSED** |
| **Tarea** | `run_pipeline_gold` → ejecuta el pipeline `68227fb0-10c6-414f-acfb-ec98df5d6ec7` |
| **Concurrencia** | 1 (nunca se solapan ejecuciones) · cola habilitada |

**Descripción del job:** construye el modelo estrella Gold (dim_escenario, dim_concepto, fact_consumo_escenarios, fact_ajustes_factura, y la proyección fact_proyeccion_3m) en `workspace.fenomeno_nino_gold` a partir de la capa Silver, todos los días automáticamente.

**Flujo orquestado completo:**

```
Excel (raw) ─▶ Bronze (notebook 01, manual/on-demand)
               ─▶ Silver (notebook 02, manual/on-demand)
               ─▶ [JOB diario] ─▶ Pipeline SDP ─▶ Gold (MVs)
               ─▶ Dashboard AI/BI (consulta Gold en vivo)
```

Bronze y Silver se ejecutan bajo demanda (la fuente es un archivo mensual); el job diario garantiza que Gold y el dashboard estén siempre sincronizados con el último dato cargado.

---

## 11. Proyección Octubre–Diciembre 2026

La vista materializada `fact_proyeccion_3m` (45 filas = 3 meses × 3 escenarios de precio × 5 escenarios de reducción) proyecta valor y ahorro para el último trimestre de 2026.

### 11.1 Metodología

* **Consumo (kWh) constante** entre meses (sin estacionalidad modelada — supuesto explícito).
* **Valores escalados** por mes según: `valor_mes = valor_base × ((1 + IPP_mensual) × (1 + variación_kWh))^n_meses`.
* **Precio efectivo base:** $599.32/kWh (parámetro `Medidores(4)!F109`, tabla Silver `parametros`) × factor `stn2 = 1.116196` → **$668.96/kWh**, validado contra `valor_base / consumo_base` en los datos.

### 11.2 Escenarios de precio (supuestos de análisis)

| Escenario | IPP mensual | Variación kWh mensual |
| --- | --- | --- |
| Conservador | 0.25% | 0.5% |
| Base | 0.45% | 2.0% |
| Pesimista | 0.70% | 4.0% |

> Los escenarios de precio son **supuestos de análisis**, no datos observados: el workspace no cuenta con series de IPP o precios de mercado. Deben interpretarse como análisis de sensibilidad.

**Resultados clave (diciembre 2026):** valor base proyectado **≈ $4.96M**; ahorro −4h proyectado **≈ $3.76M**.

---

## 12. Dashboard AI/BI — `fenomeno_nino_gold`

| Propiedad | Valor |
| --- | --- |
| **ID** | `01f1e9396db81945b3ae55736856081f` |
| **Ruta** | `/Users/alba.preciado2860@unaula.edu.co/fenomeno/fenomeno_nino_gold.lvdash.json` |
| **Estado** | DRAFT (pendiente de publicar) |
| **Validación** | Widgets verificados: página 1 7/7 SUCCESS · página 2 5/5 SUCCESS (screenshots OK) |

### Página 1 — Consumo y ahorro (septiembre 2026)

* **KPIs:** consumo base 6,886.5 kWh · valor base $4.6M · ahorro −4h $3.5M.
* **Barras:** consumo por concepto · ahorro por escenario.
* **Barras agrupadas:** consumo por concepto × escenario.
* **Tabla:** ajustes de factura (energía y pesos).
* **Datasets:** metric view local sobre `fact_consumo_escenarios` JOIN `dim_escenario` (filtrando `es_total/es_subtotal = false`) + dataset SQL de ajustes.

### Página 2 — Proyección Oct–Dic 2026

* **KPIs:** valor base diciembre $4.96M · ahorro −4h diciembre $3.76M.
* **2 gráficos de líneas:** evolución mensual de valor y de ahorro por escenario de precio.
* **1 pivot:** valor proyectado mes × escenario de precio × escenario de reducción.

---

## 13. Validación y calidad — evidencia

| Capa / Componente | Controles | Resultado |
| --- | --- | --- |
| Silver | 11 controles (tipos, nulos, rangos, cuadres con Excel) | ✅ 11/11 OK |
| Gold (pipeline) | 10 controles (claves, cuadres con Silver, flags) | ✅ 10/10 OK |
| Dashboard P1 | 7 widgets | ✅ 7/7 SUCCESS |
| Dashboard P2 | 5 widgets | ✅ 5/5 SUCCESS |
| Precio efectivo | $599.32 × 1.116196 = $668.96/kWh | ✅ Cuadra con valor/kWh observado |

**Cifras de control cruzadas fuente ↔ Silver ↔ Gold:** consumo base 6,886.5 kWh; valor base $4,606,783.32; ahorro −4h $3,499,991.34; ajuste total −194,188.40 kWh / −$129,903,999.62.

---

## 14. Guía de ejecución

Ejecutar en este orden (por única vez o al llegar un nuevo archivo mensual):

1. `00_Setup/00_catalog_schemas` — crea catálogo/esquemas (idempotente).
2. `00_Setup/01_config` — define variables del proyecto.
3. Cargar el Excel `Consumo_Sep_2026_Reduccion_Horaria.xlsx` en la raíz del proyecto (reemplazar por el mes correspondiente y ajustar `01_config`).
4. `01_Bronze/01_Bronze_EDA_Consumo_Sep_2026` — ingesta cruda (instala openpyxl automáticamente).
5. `02_Silver/02_Silver_Limpieza_Segmentacion` — limpieza + 11 controles.
6. **Automático:** el job diario `fenomeno_nino_gold` ejecuta el pipeline SDP que reconstruye Gold. Para refrescar de inmediato, lanzar el job manualmente o el update del pipeline.
7. Consultar el dashboard AI/BI `fenomeno_nino_gold` (refresca al ejecutarse el pipeline).

---

## 15. Consumo desde Power BI (opcional)

Para explotar el modelo Gold desde Power BI vía el conector de Databricks:

* Importar las 4 tablas del modelo estrella: `dim_escenario`, `dim_concepto`, `fact_consumo_escenarios`, `fact_ajustes_factura` (y opcionalmente `fact_proyeccion_3m`).
* Crear las relaciones 1:*: `dim_escenario[escenario_id] → fact_consumo_escenarios[escenario_id]` y `dim_concepto[concepto] → fact_consumo_escenarios[concepto]`.
* En todos los visuales atómicos filtrar `es_total = false AND es_subtotal = false`.
* Los hechos de septiembre responden a `periodo = '2026-09'`.

---

## 16. Consideraciones técnicas y aprendizajes

* **openpyxl no preinstalado en serverless:** todo notebook que lea Excel debe `%pip install openpyxl` al inicio.
* **Parseo por slices verificados:** la posición de los bloques en el Excel se validó fila por fila (ver 6.1); cualquier cambio de formato del archivo mensual requiere reverificar los slices.
* **Propiedad de las tablas Gold consolidada en el pipeline:** no crear tablas Gold desde notebooks para evitar duplicidad de dueños en UC.
* **Supuestos de proyección explícitos:** los escenarios de precio (IPP/variación kWh) son supuestos de sensibilidad, no datos de mercado.
* **Escenarios de reducción como dimensión:** el diseño permite comparar cualquier escenario de apagado programado contra la línea base en un solo visual.

---

## 17. Roadmap sugerido

* Publicar el dashboard (pasar de DRAFT a publicado) y definir audiencia.
* Automatizar la ingesta Bronze/Silver (hoy manual) — p. ej. Auto Loader sobre un volume o Lakeflow Connect si la fuente pasa a ser un sistema externo.
* Incorporar series reales de IPP/tarifa para sustituir los supuestos de la proyección.
* Añadir alertas de calidad de datos (frescura, completitud) sobre las tablas Silver/Gold.