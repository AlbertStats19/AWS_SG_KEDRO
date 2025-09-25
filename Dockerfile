# Imagen base ligera de Python
FROM python:3.11-slim as runtime-environment

# Actualizar pip e instalar uv (gestor rápido de pip)
RUN python -m pip install -U "pip>=21.2"
RUN pip install uv

# Copiar requirements de tu repo y instalarlos
COPY requirements.txt ./requirements.txt
RUN uv pip install --system --no-cache-dir -r requirements.txt \
    && uv pip install --system --no-cache-dir scikit-learn==1.4.0

# Crear usuario kedro para no correr como root
ARG KEDRO_UID=999
ARG KEDRO_GID=0
RUN groupadd -f -g ${KEDRO_GID} kedro_group && \
    useradd -m -d /home/kedro_docker -s /bin/bash -g ${KEDRO_GID} -u ${KEDRO_UID} kedro_docker

WORKDIR /home/kedro_docker
USER kedro_docker

# Copiar TODO tu proyecto dentro del contenedor (respetando .dockerignore)
COPY --chown=${KEDRO_UID}:${KEDRO_GID} . .

# Puerto para Jupyter o debugging (opcional)
EXPOSE 8888

# Comando por defecto: correr tu pipeline Kedro
CMD ["kedro", "run"]
