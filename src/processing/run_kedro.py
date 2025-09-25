import os
from pathlib import Path
from kedro.framework.session import KedroSession
from kedro.framework.project import configure_project

# --- Parámetros (se pueden sobreescribir por ENV) ---
PRODUCT = os.getenv("PARAM_PRODUCT", "CDT")
FECHA = os.getenv("PARAM_FECHA_EJECUCION", "2025-07-10")
VAR_APERTURA = os.getenv("PARAM_VARIABLE_APERTURA", "cdt_cant_aper_mes")
TARGET = os.getenv("PARAM_TARGET", "cdt_cant_ap_group3")

# --- Ubicación de la configuración montada por SageMaker Processing ---
SAFE_CONF_DIR = os.getenv("KEDRO_CONF_DIR", "/opt/ml/processing/conf_mlops")
KEDRO_ENV = os.getenv("KEDRO_ENV", "base")  # usar "base"

def main():
    # /opt/ml/processing/input/code  -> .../../../ = /opt/ml/processing
    project_path = Path(__file__).resolve().parents[2]

    # Obliga a Kedro a leer 'conf_mlops' que montamos en el step
    os.environ["KEDRO_CONFIG_SOURCE"] = SAFE_CONF_DIR

    # Inicializa el proyecto por package_name (instalado con pip -e .)
    configure_project("processing")

    params = {
        "product": PRODUCT,
        "fecha_ejecucion": FECHA,
        "variable_apertura": VAR_APERTURA,
        "target": TARGET,
    }

    # Crea la sesión apuntando al env "base"
    with KedroSession.create(package_name="processing", project_path=project_path, env=KEDRO_ENV) as session:
        context = session.load_context()

        print(f"[INFO] KEDRO_CONFIG_SOURCE={SAFE_CONF_DIR}  KEDRO_ENV={KEDRO_ENV}")
        print("[DEBUG] Datasets en catalog.yml:")
        for ds in context.catalog.list():
            print(f" - {ds}")

        print(f"[INFO] Ejecutando pipeline 'backtesting' con parámetros: {params}")
        session.run(pipeline_name="backtesting", extra_params=params)

if __name__ == "__main__":
    main()


#import os
#import subprocess
#import sys

## Valores por defecto para pruebas
#PRODUCT = os.getenv("PARAM_PRODUCT", "CDT")
#FECHA = os.getenv("PARAM_FECHA_EJECUCION", "2025-07-10")
#VAR_APERTURA = os.getenv("PARAM_VARIABLE_APERTURA", "cdt_cant_aper_mes")
#TARGET = os.getenv("PARAM_TARGET", "cdt_cant_ap_group3")

#def main():
#    # Ejecuta el pipeline de Kedro usando el conf_mlops/ (como en tu local)
#    cmd = [
#        sys.executable, "-m", "kedro", "run",
#        "--conf-source=./conf_mlops/",
#        "--pipeline=backtesting",  # tu lógica backtesting
#        f"--params=product={PRODUCT},fecha_ejecucion={FECHA},variable_apertura={VAR_APERTURA},target={TARGET}"
#    ]
#    print("Running:", " ".join(cmd), flush=True)
#    res = subprocess.run(cmd, check=False)
#    if res.returncode != 0:
#        raise SystemExit(res.returncode)

#if __name__ == "__main__":
#    main()
