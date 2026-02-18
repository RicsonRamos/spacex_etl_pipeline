import streamlit as st
import polars as pl
from sqlalchemy import create_engine
import plotly.express as px
from src.config.settings import settings
from src.utils.logging import logger

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="SpaceX Strategic Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

class SpaceXDashboard:
    def __init__(self):
        # Rigor: Conexão centralizada via Pydantic Settings
        self.engine = create_engine(settings.DATABASE_URL)
        self.colors = {"success": "#2ECC71", "failure": "#E74C3C", "main": "#3498DB"}

    @st.cache_data(ttl=600)
    def fetch_data(_self, query: str):
        """Executa queries e retorna Polars DataFrame com tratamento de erro."""
        try:
            return pl.read_database(query, _self.engine)
        except Exception as e:
            logger.error("Falha na consulta SQL", query=query, error=str(e))
            return pl.DataFrame()

    def render_header(self):
        st.title("🚀 SpaceX Mission Control & Efficiency Lab")
        st.markdown(
            "Análise rigorosa de performance operacional e viabilidade econômica dos vetores SpaceX."
        )
        st.write("---")

    def render_sidebar(self, df_gold):
        st.sidebar.header("🎛️ Filtros Estratégicos")
        
        # Filtro de Foguetes
        rockets = df_gold["rocket_name"].to_list() if not df_gold.is_empty() else []
        selected = st.sidebar.multiselect("Selecionar Modelos:", rockets, default=rockets)
        
        # Filtro de Status de Missão (Lógica defensiva)
        status_filter = st.sidebar.radio("Status dos Dados:", ["Todos", "Apenas Ativos", "Protótipos/Aposentados"])
        
        st.sidebar.markdown("---")
        st.sidebar.info(f"**Log Level:** {settings.LOG_LEVEL}")
        return selected, status_filter

    def run(self):
        # 1. Extração de Dados
        df_gold = self.fetch_data("SELECT * FROM gold_cost_efficiency_metrics")
        df_launches = self.fetch_data("SELECT * FROM silver_launches")
        
        if df_gold.is_empty():
            st.error("❌ Camada Gold não encontrada. Execute o pipeline de dados primeiro.")
            st.stop()

        self.render_header()
        selected_rockets, status_filter = self.render_sidebar(df_gold)

        # 2. Filtragem e Tratamento (Rigor Analítico)
        df_filtered = df_gold.filter(pl.col("rocket_name").is_in(selected_rockets))
        
        # Coerção de Nulos para evitar erros de renderização
        df_filtered = df_filtered.with_columns([
            pl.col("success_rate").fill_null(0.0),
            pl.col("avg_cost_per_kg").fill_null(0.0),
            pl.col("total_kg_delivered").fill_null(0.0)
        ])

        # 3. KPIs de Performance (Cards)
        kpi_cols = st.columns(len(df_filtered))
        for i, row in enumerate(df_filtered.to_dicts()):
            with kpi_cols[i]:
                # Insight: Delta indica saúde operacional (Confiabilidade)
                rate = float(row['success_rate'])
                st.metric(
                    label=row['rocket_name'],
                    value=f"{row['total_launches']} Missões",
                    delta=f"{rate:.1f}% Sucesso",
                    delta_color="normal" if rate > 85 else "inverse"
                )

        st.write("###")

        # 4. Visualizações de Profundidade
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("🎯 Matriz de Eficiência Econômica")
            # Gráfico de Dispersão: Relação Risco (X) vs Custo (Y)
            fig_efficiency = px.scatter(
                df_filtered.to_pandas(),
                x="success_rate",
                y="avg_cost_per_kg",
                size="total_launches",
                color="rocket_name",
                hover_name="rocket_name",
                labels={"success_rate": "Confiabilidade (%)", "avg_cost_per_kg": "USD / Kg"},
                title="Custo-Benefício por Payload"
            )
            st.plotly_chart(fig_efficiency, width='stretch')
            st.caption("Insight: Modelos no canto inferior direito são os mais estáveis financeiramente.")

        with col_right:
            st.subheader("📊 Histórico de Dominância Operacional")
            if not df_launches.is_empty():
                # Agregação temporal para Insight de Escalabilidade
                trend = df_launches.group_by(["launch_year", "success"]).count().sort("launch_year")
                fig_trend = px.bar(
                    trend.to_pandas(),
                    x="launch_year",
                    y="count",
                    color="success",
                    title="Crescimento Anual de Lançamentos",
                    color_discrete_map={True: self.colors["success"], False: self.colors["failure"]}
                )
                st.plotly_chart(fig_trend, width='stretch')

        st.write("---")
        
        # 5. Auditoria de Dados (Rigor de Engenheiro)
        with st.expander("🔍 Auditoria de Dados Brutos (Camada Gold)"):
            st.dataframe(
                df_filtered.to_pandas(),
                column_config={
                    "avg_cost_per_kg": st.column_config.NumberColumn("Custo/Kg", format="$ %.2f"),
                    "success_rate": st.column_config.NumberColumn("Sucesso", format="%.1f%%"),
                },
                hide_index=True,
                width='stretch'
            )

if __name__ == "__main__":
    SpaceXDashboard().run()