# 🐧 Crear Lambda Layer desde Mac (con binarios Linux)

## 🚨 El Problema

Cuando instalas paquetes en Mac, los binarios son para **macOS**, no para **Linux**. Lambda corre en Linux, por lo que los binarios no funcionan.

**Error típico**: `No module named 'pydantic_core._pydantic_core'`

---

## ✅ Solución: Usar GitHub Actions (Recomendado)

GitHub Actions corre en **Linux**, así que los binarios serán correctos.

### Paso 1: Ejecutar Workflow en GitHub

1. **GitHub** → Tu repo → **Actions**
2. **Buscar**: "Create Lambda Layer"
3. **Click en "Run workflow"** (botón a la derecha)
4. **Branch**: `main`
5. **Run workflow** (botón verde)
6. **Espera** 2-3 minutos a que termine

### Paso 2: Descargar el Layer

1. **GitHub Actions** → Click en el workflow ejecutado
2. **Scroll hacia abajo** → Verás "Artifacts"
3. **Click en "lambda-layer"** para descargar
4. **Descomprime** el archivo
5. **Tendrás**: `layer.zip` listo para subir

### Paso 3: Subir a AWS

1. **AWS Console** → **Lambda** → **Layers** → **Create layer**
2. **Upload**: Seleccionar el `layer.zip` descargado
3. **Compatible runtimes**: Python 3.11, Python 3.12
4. **Create**

---

## 🔧 Alternativa: Instalar Docker

Si prefieres crear el layer localmente:

### Instalar Docker Desktop

1. **Descargar**: https://www.docker.com/products/docker-desktop/
2. **Instalar** Docker Desktop para Mac
3. **Abrir** Docker Desktop
4. **Ejecutar**:
   ```bash
   ./create_layer_linux.sh
   ```

Este script usa Docker para crear el layer con binarios Linux.

---

## 📋 Comparación de Métodos

| Método | Ventajas | Desventajas |
|--------|----------|-------------|
| **GitHub Actions** | ✅ No requiere instalación<br>✅ Binarios correctos<br>✅ Automático | ⚠️ Requiere descargar artifact |
| **Docker** | ✅ Local<br>✅ Binarios correctos | ⚠️ Requiere instalar Docker |
| **pip con flags** | ✅ Rápido | ❌ No funciona para todos los paquetes |

---

## 🚀 Recomendación

**Usa GitHub Actions** - Es la forma más fácil y confiable desde Mac.

1. Ejecuta el workflow
2. Descarga el artifact
3. Súbelo a AWS

---

## 💡 Nota

El workflow se ejecuta automáticamente cuando cambias `requirements_lambda.txt`, pero también puedes ejecutarlo manualmente desde GitHub Actions.

