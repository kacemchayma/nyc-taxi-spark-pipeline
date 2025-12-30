import streamlit as st
import pandas as pd
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

# =====================================================
# CONFIG
# =====================================================
st.set_page_config(
    page_title="NYC Taxi – Dashboard Avancé",
    layout="wide",
    initial_sidebar_state="expanded"
)

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"

st.title("🚕 NYC Taxi – Analyse Avancée & Feature Engineering")
st.markdown(
    """
    **Technologies** : Apache Spark (Scala) • Streamlit • Big Data  
    **Objectif** : Explorer les tendances, anomalies et dynamiques des trajets taxis
    """
)

# =====================================================
# CHARGEMENT DES DONNÉES
# =====================================================
txt_file = OUTPUT_DIR / "phase6_extension_results.txt"
heatmap_file = OUTPUT_DIR / "heatmap_pickup_hour.csv"

# =====================================================
# ONGLET PRINCIPAL
# =====================================================
tab1, tab2, tab3 = st.tabs([
    "📄 Rapport Spark",
    "📊 Tendances & Heatmap",
    "📍 Analyse Spatiale"
])

# =====================================================
# ONGLET 1 – RAPPORT TXT
# =====================================================
with tab1:
    st.subheader("📄 Résultats analytiques (générés par Spark)")

    if txt_file.exists():
        with open(txt_file, "r", encoding="utf-8") as f:
            st.code(f.read(), language="text")
    else:
        st.error("❌ Fichier phase6_extension_results.txt introuvable")

# =====================================================
# ONGLET 2 – TENDANCES & HEATMAP
# =====================================================
with tab2:
    st.subheader("📊 Tendances temporelles")

    if heatmap_file.exists():
        df = pd.read_csv(heatmap_file)

        # ---------------- KPI ----------------
        col1, col2, col3 = st.columns(3)
        col1.metric("🚕 Trajets analysés", f"{df['count'].sum():,}")
        col2.metric("⏰ Heures couvertes", df["hour"].nunique())
        col3.metric("📍 Zones couvertes", df["PULocationID"].nunique())

        # ---------------- COURBE ----------------
        hourly = (
            df.groupby("hour")["count"]
            .sum()
            .reset_index()
            .sort_values("hour")
        )

        fig_line = px.line(
            hourly,
            x="hour",
            y="count",
            title="Évolution du nombre de trajets par heure",
            markers=True
        )
        fig_line.update_layout(
            xaxis_title="Heure de la journée",
            yaxis_title="Nombre de trajets"
        )
        st.plotly_chart(fig_line, use_container_width=True)

        # ---------------- HEATMAP ----------------
        st.subheader("🔥 Heatmap des zones de pickup par heure")

        pivot = df.pivot_table(
            index="hour",
            columns="PULocationID",
            values="count",
            aggfunc="sum"
        ).fillna(0)

        fig, ax = plt.subplots(figsize=(18, 6))
        sns.heatmap(
            pivot,
            cmap="YlOrRd",
            ax=ax,
            cbar_kws={"label": "Nombre de trajets"}
        )

        ax.set_xlabel("Zone de Pickup (PULocationID)")
        ax.set_ylabel("Heure")
        st.pyplot(fig)

    else:
        st.warning("⚠️ heatmap_pickup_hour.csv non trouvé")

# =====================================================
# ONGLET 3 – ANALYSE SPATIALE
# =====================================================
with tab3:
    st.subheader("📍 Zones les plus actives")

    if heatmap_file.exists():
        top_zones = (
            df.groupby("PULocationID")["count"]
            .sum()
            .reset_index()
            .sort_values("count", ascending=False)
            .head(15)
        )

        fig_bar = px.bar(
            top_zones,
            x="PULocationID",
            y="count",
            title="Top 15 zones de pickup",
            labels={"count": "Nombre de trajets", "PULocationID": "Zone"}
        )

        st.plotly_chart(fig_bar, use_container_width=True)

    st.info(
        "💡 Cette analyse met en évidence les zones les plus sollicitées, "
        "utile pour l’optimisation de la flotte et le ride-sharing."
    )

# =====================================================
# FOOTER
# =====================================================
st.success(
    "✔ Dashboard interactif basé sur des résultats Spark\n"
    "✔ Visualisation claire, moderne et exploitable\n"
    "✔ Extension Big Data validée"
)
