from kedro.config import TemplatedConfigLoader
from kedro.framework.context import KedroContext

# Define cómo cargar la configuración (conf_mlops en tu caso)
CONFIG_LOADER_CLASS = TemplatedConfigLoader
CONTEXT_CLASS = KedroContext

# 👇 Opcionalmente, le dices a Kedro que mire conf_mlops en vez de conf/
CONF_SOURCE = "conf_mlops"
