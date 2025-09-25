import os
from pathlib import Path
from kedro.framework.session import KedroSession
from kedro.framework.project import configure_project

# Parámetros por defecto
PRODUCT = os.getenv("PARAM_PRODUCT", "CDT")
FECHA = os.getenv("PARAM_FECHA_EJECUCION", "2025-07-10")
VAR_APERTURA = os.getenv("PARAM_VARIABLE_APERTURA", "cdt_cant_aper_mes")
TARGET = os.getenv("PARAM_TARGET", "cdt_cant_ap_group3")

def main():
    project_path = Path(__file__).resolve().parents[2]

    # 👇 Forzamos el package_name (tu carpeta en src/)
    configure_project("processing")

    params = {
        "product": PRODUCT,
        "fecha_ejecucion": FECHA,
        "variable_apertura": VAR_APERTURA,
        "target": TARGET
    }

    # 👇 Usamos ese mismo nombre aquí
    with KedroSession.create("processing", project_path=project_path) as session:
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
