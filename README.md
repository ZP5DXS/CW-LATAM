# 📡 CW LATAM

### Radar CW en tiempo real para Latinoamérica

**CW LATAM** es una plataforma web gratuita y open source diseñada para visualizar en tiempo real la actividad CW detectada por estaciones receptoras de la red en Sudamérica.

El objetivo es ofrecer una interfaz simple, visual y rápida para descubrir qué estaciones están siendo escuchadas regionalmente en la banda de **40 metros**, facilitando encuentros, actividad CW y operación QRS entre radioaficionados.

🌐 **Web:**  
https://zp5dxs.github.io/CW-LATAM/

---

## 📡 ¿Qué muestra?

CW LATAM procesa spots CW de la red y muestra únicamente actividad relevante para nuestra región.

### Filtros principales

- 📻 Banda de **40 metros**
- ⚡ Modo **CW**
- 📢 Estaciones detectadas llamando **CQ**
- 🌎 Spots recibidos por estaciones receptoras ubicadas en **Sudamérica**
- ⏱️ Actividad visible durante un máximo de **10 minutos**

La estación transmitiendo puede encontrarse en cualquier parte del mundo.

---

## 🗺️ Radar continental

Los spots son representados visualmente sobre un mapa.

Cada detección muestra la relación:

**Receptor → Estación transmitiendo**

Las trazas permiten visualizar desde qué puntos de Sudamérica está siendo escuchada una estación.

Las señales desaparecen progresivamente mediante un efecto de *fade* a medida que envejecen y son eliminadas después de 10 minutos sin nueva actividad.

---

## 📻 Frecuencia LXCW QRS — 7.033 MHz

CW LATAM utiliza:

### **7.033 MHz**

como frecuencia de encuentro CW/QRS.

**CALL / LISTEN — 7.033 MHz**

Las detecciones entre:

**7032.9 – 7033.1 kHz**

son normalizadas visualmente a **7033.0 kHz**, contemplando pequeñas diferencias de frecuencia entre equipos.

Las estaciones detectadas en esta frecuencia reciben **prioridad absoluta y una identificación visual especial** dentro del radar.

---

## 🔊 Alerta CW 7033

La interfaz puede generar una alerta sonora cuando aparece nueva actividad en la frecuencia de encuentro.

La alerta utiliza tonos Morse reales:

**CQ CQ CQ**

El usuario puede activar o silenciar esta función desde la propia interfaz.

---

## 📊 Prioridad regional

Una estación puede ser detectada varias veces por diferentes receptores.

CW LATAM prioriza especialmente la **cantidad de receptores únicos** que escuchan una misma estación.

Esto permite distinguir entre:

- una estación detectada muchas veces por un único receptor;
- una estación escuchada simultáneamente desde diferentes lugares de Sudamérica.

La segunda representa una señal con mayor cobertura regional.

---

## 🎯 Foco regional

El radar permite utilizar un modo de **FOCO** alrededor del centro seleccionado en el mapa.

Esto permite dar mayor relevancia a los receptores cercanos a una determinada región y observar la actividad CW desde una perspectiva más local.

---

## ⚡ Tiempo real

La plataforma utiliza una arquitectura de actualización continua.

Los nuevos spots son enviados automáticamente al navegador y aparecen sin necesidad de actualizar manualmente la página.

La interfaz mantiene únicamente actividad reciente para conservar la naturaleza de **radar en tiempo real** del proyecto.

---

## 🧭 Filosofía del proyecto

CW LATAM nace como una herramienta comunitaria para fomentar la actividad CW en Latinoamérica.

El proyecto busca ser:

**GRATIS · SIN ANUNCIOS · OPEN SOURCE**

Sin cuentas obligatorias, sin instalaciones y accesible directamente desde cualquier navegador moderno.

---

## 🛠️ Tecnología

- HTML / CSS / JavaScript
- Leaflet / OpenStreetMap
- Node.js
- WebSocket
- Streaming de spots en tiempo real
- GitHub Pages
- Backend relay independiente

---

## 📜 Licencia

Este proyecto se distribuye bajo licencia **MIT**.

Podés estudiar, modificar, reutilizar y contribuir al código respetando los términos de la licencia.

---

## 📻 Autor

**Mathias Maidana — ZP5DXS**

Proyecto desarrollado para la comunidad CW latinoamericana.

**73!**
