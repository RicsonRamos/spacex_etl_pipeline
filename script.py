import subprocess
import platform
import shutil
from pathlib import Path
import sys

# ==============================
# CONFIGURAÇÕES DE CAMINHO
# ==============================

SRC_DIR = Path("docs/diagrams_src")
OUT_DIR = Path("docs/diagrams")

OUT_DIR.mkdir(parents=True, exist_ok=True)

# Detecta executável correto
MMDC = "mmdc.cmd" if platform.system() == "Windows" else "mmdc"

# Verifica se mmdc está no PATH
if not shutil.which(MMDC):
    print("❌ Mermaid CLI (mmdc) não encontrado.")
    print("Instale com: npm install -g @mermaid-js/mermaid-cli")
    sys.exit(1)

# Lista de arquivos Mermaid
diagrams = [
    "etl_flow",
    "dependencies",
    "er_model",
    "prefect_flow"
]

# ==============================
# FUNÇÃO PARA GERAR PNG
# ==============================

def generate_png(name):
    src_file = SRC_DIR / f"{name}.mmd"
    out_file = OUT_DIR / f"{name}.png"

    if not src_file.exists():
        print(f"⚠️ Arquivo não encontrado: {src_file}")
        return

    cmd = [
        MMDC,
        "-i", str(src_file),
        "-o", str(out_file),
        "-t", "default",
        "-w", "1280",
        "-H", "720"
    ]

    print(f"🚀 Generating {out_file}...")

    try:
        subprocess.run(cmd, check=True)
        print(f"✅ Successfully generated: {out_file}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error generating {name}: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# ==============================
# EXECUÇÃO
# ==============================

print("🔎 Starting Mermaid diagram generation...\n")

for diagram in diagrams:
    generate_png(diagram)

print("\n🎉 Process completed!")
