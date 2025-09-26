from pathlib import Path
from kedro.framework.session import KedroSession

def main():
    # 🔹 Parámetros de ejecución (quemados aquí para simplicidad)
    params = {
        "product": "CDT",
        "fecha_ejecucion": "2025-07-10",
        "variable_apertura": "cdt_cant_aper_mes",
        "target": "cdt_cant_ap_group3",
    }

    print(f"[INFO] Parámetros de ejecución: {params}")

    # 🔹 Crear sesión de Kedro usando package_name explícito
    with KedroSession.create(
        package_name="processing",   # 👈 tomado de pyproject.toml
        project_path=Path(__file__).resolve().parents[1],  # 👈 apunta a src/processing
        env="base"
    ) as session:
        context = session.load_context()
        print("[INFO] Contexto Kedro cargado correctamente")

        print(f"[INFO] Ejecutando pipeline 'backtesting' con parámetros...")
        session.run(pipeline_name="backtesting", extra_params=params)
        print("[INFO] Pipeline 'backtesting' ejecutado exitosamente ✅")

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
