from dataclasses import dataclass


@dataclass(frozen=True)
class EntityConfig:
    """
    Configuração imutável da entidade.
    Responsável apenas por derivar nomes físicos das tabelas.
    (Single Responsibility Principle)
    """
    name: str

    @property
    def bronze_table(self) -> str:
        return f"bronze_{self.name}"

    @property
    def silver_table(self) -> str:
        return f"silver_{self.name}"