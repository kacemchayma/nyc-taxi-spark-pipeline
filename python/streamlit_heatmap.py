import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

st.set_page_config(
    page_title="NYC Taxi – Heatmap des pickups",
    layout="wide"
)

st.title("🚕 NYC Taxi – Activité des zones de pickup")
st.markdown("Heatmap des trajets par zone et par heure (données Spark)")

# ================================
# Chargement des données Spark
# ================================
@st.cache_data
def load_data():
    return pd.read_csv("output/heatmap_pickup_hour.csv")

df = load_data()

st.success(f"{len(df)} lignes chargées depuis Spark")

# ================================
# FILTRES UTILISATEUR
# ================================
st.sidebar.header("Filtres")

top_n = st.sidebar.slider(
    "Nombre de zones affichées",
    min_value=5,
    max_value=30,
    value=15
)

# Top zones les plus actives
top_zones = (
    df.groupby("PULocationID")["count"]
    .sum()
    .sort_values(ascending=False)
    .head(top_n)
    .index
)

df_filtered = df[df["PULocationID"].isin(top_zones)]

# ================================
# HEATMAP
# ================================
st.header("🔥 Heatmap : Zone × Heure")

pivot = df_filtered.pivot_table(
    index="PULocationID",
    columns="hour",
    values="count",
    aggfunc="sum",
    fill_value=0
)

fig, ax = plt.subplots(figsize=(16, 7))
sns.heatmap(
    pivot,
    cmap="YlOrRd",
    linewidths=0.2,
    ax=ax
)

ax.set_xlabel("Heure de la journée")
ax.set_ylabel("Zone de pickup")

st.pyplot(fig)

# ================================
# INTERPRÉTATION
# ================================
st.markdown("""
### 🧠 Interprétation
- Les zones les plus actives apparaissent avec des couleurs plus foncées.
- Les pics d’activité sont visibles aux heures de pointe (matin et fin de journée).
- Cette visualisation permet d’identifier des **hotspots spatio-temporels**.
""")
