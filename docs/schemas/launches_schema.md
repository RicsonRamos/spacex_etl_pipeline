#  Dicionário de Dados: `LAUNCHES`

- **Registros analisados:** 205
- **Total de colunas:** 43
- **Data da análise:** 2026-02-13 01:18

## Mapeamento de Campos

| Campo | Tipo Python/Pandas | Preenchimento (%) | Status |
| :--- | :--- | :--- | :--- |
| `static_fire_date_utc` | `str` | 59.02% | Baixo Preenchimento |
| `static_fire_date_unix` | `float64` | 59.02% | Baixo Preenchimento |
| `net` | `bool` | 100.0% | OK |
| `window` | `float64` | 57.07% | Baixo Preenchimento |
| `rocket` | `str` | 100.0% | OK |
| `success` | `object` | 90.73% | OK |
| `failures` | `object` | 100.0% | OK |
| `details` | `str` | 65.37% | Baixo Preenchimento |
| `crew` | `object` | 100.0% | OK |
| `ships` | `object` | 100.0% | OK |
| `capsules` | `object` | 100.0% | OK |
| `payloads` | `object` | 100.0% | OK |
| `launchpad` | `str` | 100.0% | OK |
| `flight_number` | `int64` | 100.0% | OK |
| `name` | `str` | 100.0% | OK |
| `date_utc` | `str` | 100.0% | OK |
| `date_unix` | `int64` | 100.0% | OK |
| `date_local` | `str` | 100.0% | OK |
| `date_precision` | `str` | 100.0% | OK |
| `upcoming` | `bool` | 100.0% | OK |
| `cores` | `object` | 100.0% | OK |
| `auto_update` | `bool` | 100.0% | OK |
| `tbd` | `bool` | 100.0% | OK |
| `launch_library_id` | `str` | 35.12% | Baixo Preenchimento |
| `id` | `str` | 100.0% | OK |
| `fairings.reused` | `object` | 45.37% | Baixo Preenchimento (Nested) |
| `fairings.recovery_attempt` | `object` | 52.2% | Baixo Preenchimento (Nested) |
| `fairings.recovered` | `object` | 41.46% | Baixo Preenchimento (Nested) |
| `fairings.ships` | `object` | 82.44% | OK (Nested) |
| `links.patch.small` | `str` | 92.2% | OK (Nested) |
| `links.patch.large` | `str` | 92.2% | OK (Nested) |
| `links.reddit.campaign` | `str` | 73.17% | Baixo Preenchimento (Nested) |
| `links.reddit.launch` | `str` | 80.0% | Baixo Preenchimento (Nested) |
| `links.reddit.media` | `str` | 42.93% | Baixo Preenchimento (Nested) |
| `links.reddit.recovery` | `str` | 46.34% | Baixo Preenchimento (Nested) |
| `links.flickr.small` | `object` | 100.0% | OK (Nested) |
| `links.flickr.original` | `object` | 100.0% | OK (Nested) |
| `links.presskit` | `str` | 44.39% | Baixo Preenchimento (Nested) |
| `links.webcast` | `str` | 91.71% | OK (Nested) |
| `links.youtube_id` | `str` | 91.71% | OK (Nested) |
| `links.article` | `str` | 69.27% | Baixo Preenchimento (Nested) |
| `links.wikipedia` | `str` | 75.61% | Baixo Preenchimento (Nested) |
| `fairings` | `float64` | 0.0% | Baixo Preenchimento |
