# Imagen base
FROM public.ecr.aws/docker/library/python:3.11-slim as runtime-environment

# Pip y uv
RUN python -m pip install -U "pip>=21.2"
RUN pip install uv

# Reqs del proyecto
COPY requirements.txt ./requirements.txt
RUN uv pip install --system --no-cache-dir -r requirements.txt \
    && uv pip install --system --no-cache-dir scikit-learn==1.4.0

# Copiamos el repo y lo instalamos como paquete con root (evita Permission denied en egg-info)
WORKDIR /opt/project
COPY . .
RUN pip install -e .

# Creamos usuario no-root
ARG KEDRO_UID=999
ARG KEDRO_GID=0
RUN groupadd -f -g ${KEDRO_GID} kedro_group && \
    useradd -m -d /home/kedro_docker -s /bin/bash -g ${KEDRO_GID} -u ${KEDRO_UID} kedro_docker

# Privilegios sobre /opt/project (por si en runtime lees algo de ahí)
RUN chown -R ${KEDRO_UID}:${KEDRO_GID} /opt/project

# Entraremos como usuario seguro
USER kedro_docker
WORKDIR /opt/project

# (Opcional) puerto
EXPOSE 8888

# Ejecuta tu script
CMD ["python", "src/processing/run_kedro.py"]


## Imagen base ligera de Python desde AWS Public ECR
#FROM public.ecr.aws/docker/library/python:3.11-slim as runtime-environment

## Actualizar pip e instalar uv (gestor rápido de pip)
#RUN python -m pip install -U "pip>=21.2"
#RUN pip install uv

## Copiar requirements de tu repo e instalarlos
#COPY requirements.txt ./requirements.txt
#RUN uv pip install --system --no-cache-dir -r requirements.txt \
#    && uv pip install --system --no-cache-dir scikit-learn==1.4.0

## Crear usuario kedro para no correr como root
#ARG KEDRO_UID=999
#ARG KEDRO_GID=0
#RUN groupadd -f -g ${KEDRO_GID} kedro_group && \
#    useradd -m -d /home/kedro_docker -s /bin/bash -g ${KEDRO_GID} -u ${KEDRO_UID} kedro_docker

#WORKDIR /home/kedro_docker
#USER kedro_docker

## Copiar TODO tu proyecto dentro del contenedor (respetando .dockerignore)
#COPY --chown=${KEDRO_UID}:${KEDRO_GID} . .

## Puerto para Jupyter o debugging (opcional)
#EXPOSE 8888

## Comando por defecto: correr tu pipeline Kedro
## CMD ["kedro", "run"]
#CMD ["python", "src/processing/run_kedro.py"]
