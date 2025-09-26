import subprocess

def main():
    params = {
        "product": "CDT",
        "fecha_ejecucion": "2025-07-10",
        "variable_apertura": "cdt_cant_aper_mes",
        "target": "cdt_cant_ap_group3",
    }

    print(f"[INFO] Parámetros de ejecución: {params}")

    # 🔹 Convertir dict en formato CLI
    params_str = ",".join([f"{k}:{v}" for k, v in params.items()])

    print("[INFO] Ejecutando pipeline 'backtesting' vía CLI Kedro...")
    result = subprocess.run(
        [
            "kedro",
            "run",
            "--pipeline=backtesting",
            f"--params={params_str}",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    print("[STDOUT]", result.stdout)
    print("[STDERR]", result.stderr)

    if result.returncode != 0:
        raise RuntimeError("❌ Error al ejecutar el pipeline Kedro")

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
