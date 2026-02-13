import pandas as pd
import json
from pathlib import Path
from src.config.config import settings

def generate_markdown_report(endpoint_name, df):
    """Gera um dicionário de dados em Markdown com visualização profissional."""
    report_dir = settings.ROOT_DIR / "docs" / "schemas"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{endpoint_name}_schema.md"

    # Cálculo de metadados
    completeness = (df.notnull().mean() * 100).round(2)
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"#  Dicionário de Dados: `{endpoint_name.upper()}`\n\n")
        f.write(f"- **Registros analisados:** {len(df)}\n")
        f.write(f"- **Total de colunas:** {len(df.columns)}\n")
        f.write(f"- **Data da análise:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        
        f.write("## Mapeamento de Campos\n\n")
        f.write("| Campo | Tipo Python/Pandas | Preenchimento (%) | Status |\n")
        f.write("| :--- | :--- | :--- | :--- |\n")
        
        for col in df.columns:
            dtype = str(df[col].dtype)
            perc = completeness[col]
            # Lógica de status para o analista
            status = "OK" if perc > 80 else "Baixo Preenchimento"
            if "." in col:
                status += " (Nested)"
            
            f.write(f"| `{col}` | `{dtype}` | {perc}% | {status} |\n")
            
    print(f"Documentação Sênior gerada: {report_path}")

def run_inspection():
    # Itera sobre os arquivos JSON que acabamos de extrair
    for file_path in settings.RAW_DATA_DIR.glob("*.json"):
        # Extrai o nome do endpoint do nome do arquivo (ex: rockets_2026... -> rockets)
        endpoint_name = file_path.name.split('_')[0]
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # json_normalize é o padrão ouro para achatar JSON de APIs
            df = pd.json_normalize(data)
            generate_markdown_report(endpoint_name, df)
        except Exception as e:
            print(f"Falha ao processar {file_path.name}: {e}")

if __name__ == "__main__":
    run_inspection()