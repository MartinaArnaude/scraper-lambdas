#!/bin/bash

# Script para crear Lambda Layer con binarios para Linux (desde Mac)
# Usa Docker o pip con flags específicos para Linux
# Uso: ./create_layer_linux.sh

set -e

echo "🐧 Creando Lambda Layer con binarios para Linux (desde Mac)..."
echo ""

# Verificar si Docker está disponible
if command -v docker &> /dev/null; then
    USE_DOCKER=true
    echo "✅ Docker encontrado - Usando Docker para crear layer con binarios Linux"
else
    USE_DOCKER=false
    echo "⚠️  Docker no encontrado - Usando pip con flags Linux (puede no funcionar para todos los paquetes)"
fi

# 1. Limpiar
if [ -d "lambda-layer" ]; then
    echo "🧹 Limpiando layer anterior..."
    rm -rf lambda-layer
fi

if [ "$USE_DOCKER" = true ]; then
    # Usar Docker (MEJOR OPCIÓN)
    echo ""
    echo "🐳 Usando Docker para crear layer con binarios Linux..."
    
    # Crear Dockerfile temporal
    cat > /tmp/Dockerfile.layer << 'EOF'
FROM public.ecr.aws/lambda/python:3.11

WORKDIR /layer

# Instalar dependencias
COPY requirements_lambda.txt .
RUN pip install -r requirements_lambda.txt -t python/ --no-cache-dir

# Crear zip
RUN cd python && zip -r ../layer.zip . && cd ..
EOF

    # Crear requirements en ubicación temporal
    cp requirements_lambda.txt /tmp/requirements_lambda.txt

    # Construir imagen y extraer layer
    echo "   Construyendo imagen Docker..."
    docker build -f /tmp/Dockerfile.layer -t lambda-layer-builder /tmp/ > /dev/null 2>&1 || {
        echo "❌ Error construyendo imagen Docker"
        echo "   Asegúrate de que Docker está corriendo"
        exit 1
    }

    # Crear contenedor y copiar layer
    echo "   Extrayendo layer..."
    CONTAINER_ID=$(docker create lambda-layer-builder)
    docker cp $CONTAINER_ID:/layer/layer.zip ./lambda-layer/layer.zip
    docker rm $CONTAINER_ID > /dev/null

    # Limpiar
    rm /tmp/Dockerfile.layer /tmp/requirements_lambda.txt

    echo "✅ Layer creado con Docker"
else
    # Usar pip con flags Linux (FALLBACK)
    echo ""
    echo "📦 Usando pip con flags Linux..."
    echo "   (Algunos paquetes pueden no tener binarios precompilados)"
    
    mkdir -p lambda-layer/python
    cd lambda-layer/python

    # Instalar con flags para Linux
    echo "   Instalando dependencias..."
    
    # pydantic-core primero (crítico)
    echo "   Instalando pydantic-core para Linux..."
    pip install pydantic-core>=2.0.0 \
        --platform manylinux2014_x86_64 \
        --only-binary=:all: \
        --target . \
        --no-cache-dir 2>&1 | tail -5 || {
        echo "⚠️  No se pudieron instalar binarios precompilados de pydantic-core"
        echo "   Intentando instalación normal (puede fallar en Lambda)..."
        pip install pydantic-core>=2.0.0 -t . --no-cache-dir
    }

    # Otras dependencias
    pip install pydantic>=2.0.0 -t . --no-cache-dir
    pip install beautifulsoup4>=4.12.0 requests>=2.31.0 -t . --no-cache-dir
    pip install httpx>=0.24.0 --platform manylinux2014_x86_64 --only-binary=:all: -t . --no-cache-dir 2>&1 | tail -3 || \
        pip install httpx>=0.24.0 -t . --no-cache-dir
    pip install psycopg2-binary>=2.9.9 -t . --no-cache-dir
    pip install boto3>=1.28.0 -t . --no-cache-dir
    pip install python-dateutil>=2.8.2 -t . --no-cache-dir
    pip install crawl4ai>=0.7.0 -t . --no-cache-dir 2>&1 | tail -5

    cd ../..

    # Crear ZIP
    echo ""
    echo "📦 Creando archivo zip..."
    cd lambda-layer
    zip -r layer.zip python/ -q
    cd ..
    
    echo "✅ Layer creado con pip"
fi

# Verificar tamaño
ZIP_SIZE=$(du -h lambda-layer/layer.zip | cut -f1)
ZIP_SIZE_BYTES=$(stat -f%z lambda-layer/layer.zip 2>/dev/null || stat -c%s lambda-layer/layer.zip 2>/dev/null)
ZIP_SIZE_MB=$((ZIP_SIZE_BYTES / 1024 / 1024))

echo ""
echo "✅ Layer creado: lambda-layer/layer.zip"
echo "📦 Tamaño: $ZIP_SIZE ($ZIP_SIZE_MB MB)"

# Verificar pydantic-core
echo ""
echo "🔍 Verificando pydantic-core..."
if unzip -l lambda-layer/layer.zip | grep -q "_pydantic_core.*\.so"; then
    echo "✅ Binario de pydantic-core encontrado (Linux)"
    unzip -l lambda-layer/layer.zip | grep "_pydantic_core.*\.so" | head -3
else
    echo "⚠️  ADVERTENCIA: No se encontró binario de pydantic-core para Linux"
    echo "   El layer puede no funcionar en Lambda"
    echo "   Recomendación: Usa Docker para crear el layer"
fi

if [ $ZIP_SIZE_MB -gt 50 ]; then
    echo ""
    echo "⚠️  ADVERTENCIA: El layer es mayor a 50MB ($ZIP_SIZE_MB MB)"
else
    echo "✅ Tamaño OK (límite: 50MB)"
fi

echo ""
echo "📋 Próximo paso: Subir lambda-layer/layer.zip a AWS"
echo "   Ver: SUBIR_LAYER_MANUAL.md"

