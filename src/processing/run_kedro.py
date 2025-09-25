import os
from pathlib import Path
from kedro.framework.session import KedroSession

# Parámetros por defecto (se sobreescriben con ENV en SageMaker)
PRODUCT = os.getenv("PARAM_PRODUCT", "CDT")
FECHA = os.getenv("PARAM_FECHA_EJECUCION", "2025-07-10")
VAR_APERTURA = os.getenv("PARAM_VARIABLE_APERTURA", "cdt_cant_aper_mes")
TARGET = os.getenv("PARAM_TARGET", "cdt_cant_ap_group3")

def main():
    project_path = Path(__file__).resolve().parents[2]

    # 👇 Forzamos a Kedro a usar conf_mlops como origen de configuraciones
    os.environ["KEDRO_CONFIG_SOURCE"] = str(project_path / "conf_mlops")

    params = {
        "product": PRODUCT,
        "fecha_ejecucion": FECHA,
        "variable_apertura": VAR_APERTURA,
        "target": TARGET,
    }

    # Crear la sesión Kedro
    with KedroSession.create(package_name=None, project_path=project_path) as session:
        context = session.load_context()

        # 🚀 Depuración: imprimir el catalogo de datos
        print("[DEBUG] Datasets configurados en catalog.yml:")
        for ds_name in context.catalog.list():
            print(f" - {ds_name}")

        # Ejecutar el pipeline
        print(f"[INFO] Ejecutando pipeline backtesting con parámetros: {params}")
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
