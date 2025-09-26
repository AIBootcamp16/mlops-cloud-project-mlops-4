import pkgutil
import importlib
from pathlib import Path

routers = []

package_dir = Path(__file__).resolve().parent

for _, module_name, _ in pkgutil.iter_modules([str(package_dir)]):
    if module_name.startswith("routes_"):
        module = importlib.import_module(f"api.{module_name}")
        if hasattr(module, "router"):
            prefix = "/" + module_name.replace("routes_", "")
            tag = module_name.replace("routes_", "").capitalize()
            routers.append((module.router, prefix, [tag]))