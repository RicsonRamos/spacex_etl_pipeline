#  Dicionário de Dados: `ROCKETS`

- **Registros analisados:** 4
- **Total de colunas:** 56
- **Data da análise:** 2026-02-13 01:18

## Mapeamento de Campos

| Campo | Tipo Python/Pandas | Preenchimento (%) | Status |
| :--- | :--- | :--- | :--- |
| `payload_weights` | `object` | 100.0% | OK |
| `flickr_images` | `object` | 100.0% | OK |
| `name` | `str` | 100.0% | OK |
| `type` | `str` | 100.0% | OK |
| `active` | `bool` | 100.0% | OK |
| `stages` | `int64` | 100.0% | OK |
| `boosters` | `int64` | 100.0% | OK |
| `cost_per_launch` | `int64` | 100.0% | OK |
| `success_rate_pct` | `int64` | 100.0% | OK |
| `first_flight` | `str` | 100.0% | OK |
| `country` | `str` | 100.0% | OK |
| `company` | `str` | 100.0% | OK |
| `wikipedia` | `str` | 100.0% | OK |
| `description` | `str` | 100.0% | OK |
| `id` | `str` | 100.0% | OK |
| `height.meters` | `float64` | 100.0% | OK (Nested) |
| `height.feet` | `float64` | 100.0% | OK (Nested) |
| `diameter.meters` | `float64` | 100.0% | OK (Nested) |
| `diameter.feet` | `float64` | 100.0% | OK (Nested) |
| `mass.kg` | `int64` | 100.0% | OK (Nested) |
| `mass.lb` | `int64` | 100.0% | OK (Nested) |
| `first_stage.thrust_sea_level.kN` | `int64` | 100.0% | OK (Nested) |
| `first_stage.thrust_sea_level.lbf` | `int64` | 100.0% | OK (Nested) |
| `first_stage.thrust_vacuum.kN` | `int64` | 100.0% | OK (Nested) |
| `first_stage.thrust_vacuum.lbf` | `int64` | 100.0% | OK (Nested) |
| `first_stage.reusable` | `bool` | 100.0% | OK (Nested) |
| `first_stage.engines` | `int64` | 100.0% | OK (Nested) |
| `first_stage.fuel_amount_tons` | `float64` | 100.0% | OK (Nested) |
| `first_stage.burn_time_sec` | `float64` | 75.0% | Baixo Preenchimento (Nested) |
| `second_stage.thrust.kN` | `int64` | 100.0% | OK (Nested) |
| `second_stage.thrust.lbf` | `int64` | 100.0% | OK (Nested) |
| `second_stage.payloads.composite_fairing.height.meters` | `float64` | 75.0% | Baixo Preenchimento (Nested) |
| `second_stage.payloads.composite_fairing.height.feet` | `float64` | 75.0% | Baixo Preenchimento (Nested) |
| `second_stage.payloads.composite_fairing.diameter.meters` | `float64` | 75.0% | Baixo Preenchimento (Nested) |
| `second_stage.payloads.composite_fairing.diameter.feet` | `float64` | 75.0% | Baixo Preenchimento (Nested) |
| `second_stage.payloads.option_1` | `str` | 100.0% | OK (Nested) |
| `second_stage.reusable` | `bool` | 100.0% | OK (Nested) |
| `second_stage.engines` | `int64` | 100.0% | OK (Nested) |
| `second_stage.fuel_amount_tons` | `float64` | 100.0% | OK (Nested) |
| `second_stage.burn_time_sec` | `float64` | 75.0% | Baixo Preenchimento (Nested) |
| `engines.isp.sea_level` | `int64` | 100.0% | OK (Nested) |
| `engines.isp.vacuum` | `int64` | 100.0% | OK (Nested) |
| `engines.thrust_sea_level.kN` | `int64` | 100.0% | OK (Nested) |
| `engines.thrust_sea_level.lbf` | `int64` | 100.0% | OK (Nested) |
| `engines.thrust_vacuum.kN` | `int64` | 100.0% | OK (Nested) |
| `engines.thrust_vacuum.lbf` | `int64` | 100.0% | OK (Nested) |
| `engines.number` | `int64` | 100.0% | OK (Nested) |
| `engines.type` | `str` | 100.0% | OK (Nested) |
| `engines.version` | `str` | 100.0% | OK (Nested) |
| `engines.layout` | `str` | 75.0% | Baixo Preenchimento (Nested) |
| `engines.engine_loss_max` | `float64` | 75.0% | Baixo Preenchimento (Nested) |
| `engines.propellant_1` | `str` | 100.0% | OK (Nested) |
| `engines.propellant_2` | `str` | 100.0% | OK (Nested) |
| `engines.thrust_to_weight` | `float64` | 100.0% | OK (Nested) |
| `landing_legs.number` | `int64` | 100.0% | OK (Nested) |
| `landing_legs.material` | `str` | 75.0% | Baixo Preenchimento (Nested) |
