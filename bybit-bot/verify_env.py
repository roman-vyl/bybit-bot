# verify_env.py

import sys
import subprocess
from pathlib import Path

print("\n🔍 Проверка текущего окружения:")

# Путь к текущему python
print(f"✅ Python интерпретатор: {sys.executable}")

# Проверка, активен ли venv
venv_path = Path(sys.executable).parts
if "venv" in [p.lower() for p in venv_path]:
    print("✅ Виртуальное окружение активно (venv)")
else:
    print("⚠️ Внимание: venv НЕ активирован!")

# Версия Python
print(f"🐍 Версия Python: {sys.version.split()[0]}")

# Установленные пакеты
print("\n📦 Установленные пакеты:")
subprocess.run([sys.executable, "-m", "pip", "list"])
