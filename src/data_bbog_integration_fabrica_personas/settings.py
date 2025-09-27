"""Project settings. There is no need to edit this file unless you want to change values
from the Kedro defaults. For further information, including these default values, see
https://kedro.readthedocs.io/en/stable/kedro_project_setup/settings.html."""

# Instantiated project hooks.
# For example, after creating a hooks.py and defining a ProjectHooks class there, do
# from data_bbog_integration_fabrica_personas.hooks import ProjectHooks
# Hooks are executed in a Last-In-First-Out (LIFO) order.
# HOOKS = (ProjectHooks(),)
# Installed plugins for which to disable hook auto-registration.
# DISABLE_HOOKS_FOR_PLUGINS = ("kedro-viz",)

# Class that manages storing KedroSession data.
# from kedro.framework.session.store import BaseSessionStore
# SESSION_STORE_CLASS = BaseSessionStore
# Keyword arguments to pass to the `SESSION_STORE_CLASS` constructor.
# SESSION_STORE_ARGS = {
#     "path": "./sessions"
# }

# Directory that holds configuration.
# CONF_SOURCE = "conf"

# Class that manages how configuration is loaded.
from kedro.config import OmegaConfigLoader  # noqa: import-outside-toplevel
from datetime import datetime
from dateutil.relativedelta import relativedelta
from data_bbog_integration_fabrica_personas.hooks import MemoryProfilingHooks

HOOKS = (MemoryProfilingHooks(),)


def get_previous_month(period: str):
    date = datetime.strptime(period, "%Y-%m-%d")
    previous = date - relativedelta(months=1)
    return previous.strftime("%Y%m")


def get_execution_date(fecha_ejecucion: str):
    date = datetime.strptime(fecha_ejecucion, "%Y-%m-%d")
    return date.strftime("%Y%m")


def get_current_month():
    date = datetime.now()
    return date.strftime("%Y%m")


CONFIG_LOADER_CLASS = OmegaConfigLoader

CONFIG_LOADER_ARGS = {
    "custom_resolvers": {
        "previous_month": get_previous_month,
        "current_month": get_current_month,
        "format_execution_date": get_execution_date,
    }
}
# Keyword arguments to pass to the `CONFIG_LOADER_CLASS` constructor.
# CONFIG_LOADER_ARGS = {
#       "config_patterns": {
#           "spark" : ["spark*/"],
#           "parameters": ["parameters*", "parameters*/**", "**/parameters*"],
#       }
# }

# Class that manages Kedro's library components.
# from kedro.framework.context import KedroContext
# CONTEXT_CLASS = KedroContext

# Class that manages the Data Catalog.
# from kedro.io import DataCatalog
# DATA_CATALOG_CLASS = DataCatalog
