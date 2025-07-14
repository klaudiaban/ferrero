import pathlib
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px

# ------------------------------------------------------------------
# 1 – Load & preprocess data
# ------------------------------------------------------------------
FAILURE_FILE = "raport_awarii.csv"
PROD_FILE    = "test.csv"


def load_and_prepare() -> pd.DataFrame:
    # -------- Failure / downtime ----------
    fail = pd.read_csv(FAILURE_FILE, sep=None, engine="python")

    # robust 5-digit line code extractor: …-010703 or …-010703-003 → 10703
    pattern = r"(\d{5})(?:-\d{3})?$"
    fail["Linia"] = (
        fail["Lokalizacja funkc."]
        .astype(str)
        .str.extract(pattern)[0]
        .astype(float)
        .astype("Int64")
    )

    fail["Data"] = pd.to_datetime(
        fail["Utworzono dnia"], format="%d.%m.%Y", errors="coerce"
    )
    fail["Tydzień"] = "W" + fail["Data"].dt.isocalendar().week.astype(str).str.zfill(2)

    fail["Downtime_min"] = (
        fail["Czas przestoju"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .str.replace(" ", "", regex=False)
        .astype(float)
        .fillna(0.0)
    )

    downtime = (
        fail.groupby(["Tydzień", "Linia"], as_index=False)["Downtime_min"]
        .sum()
        .rename(columns={"Downtime_min": "Total_downtime_min"})
    )

    # -------- Production ----------
    prod = pd.read_csv(PROD_FILE, sep=None, engine="python")
    prod["Linia"] = pd.to_numeric(prod["Linia"], errors="coerce").astype("Int64")
    prod["Tydzień"] = prod["﻿Tydzień"].astype(str).str.strip()

    # find the “Eff LINIA …” column automatically
    eff_col = next(c for c in prod.columns if "Eff LINIA" in c)

    for col in ["QL (TOT) Akt", "QL (TOT) Pln", eff_col]:
        prod[col] = (
            prod[col]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .str.replace(" ", "", regex=False)
            .astype(float)
        )

    prod["QL_diff"]            = prod["QL (TOT) Akt"] - prod["QL (TOT) Pln"]
    prod["Line_efficiency_pct"] = prod[eff_col]

    prod_clean = prod[["Tydzień", "Linia", "Line_efficiency_pct", "QL_diff"]]

    # -------- Merge ----------
    merged = pd.merge(
        downtime, prod_clean, on=["Tydzień", "Linia"], how="left"
    ).dropna()

    return merged


def scatter_with_trend(df, x, y, title):
    fig = px.scatter(
        df, x=x, y=y, trendline="ols",
        hover_data=["Tydzień"], template="simple_white"
    )
    fig.update_layout(title=title, height=400, margin=dict(l=40, r=20, t=60, b=40))
    return fig


df_all   = load_and_prepare()
all_lines = sorted(df_all["Linia"].unique())

# ------------------------------------------------------------------
# 2 – Dash layout
# ------------------------------------------------------------------
app = dash.Dash(__name__)
app.layout = html.Div(
    className="p-6 font-sans space-y-6",
    children=[
        html.H1("Downtime vs. Production KPIs", className="text-3xl font-bold"),
        html.Div(
            [
                html.Label("Select production line:", className="mr-2"),
                dcc.Dropdown(
                    id="line-dropdown",
                    options=[{"label": str(l), "value": l} for l in all_lines],
                    value=all_lines[0],
                    clearable=False,
                    style={"width": "200px"},
                ),
            ]
        ),
        html.Div(id="kpi-cards", className="flex flex-wrap gap-4"),
        dcc.Graph(id="eff-scatter"),
        dcc.Graph(id="ql-scatter"),
        dash_table.DataTable(
            id="weekly-table",
            page_size=20,
            style_table={"overflowX": "auto"},
            style_cell={"padding": "6px"},
        ),
    ],
)

# ------------------------------------------------------------------
# 3 – Callbacks
# ------------------------------------------------------------------
@app.callback(
    [
        Output("eff-scatter", "figure"),
        Output("ql-scatter", "figure"),
        Output("weekly-table", "data"),
        Output("weekly-table", "columns"),
        Output("kpi-cards", "children"),
    ],
    [Input("line-dropdown", "value")],
)
def update(line):
    dfl = df_all[df_all["Linia"] == line]

    fig_eff = scatter_with_trend(
        dfl, "Total_downtime_min", "Line_efficiency_pct",
        "Downtime vs Line Efficiency (%)"
    )
    fig_ql = scatter_with_trend(
        dfl, "Total_downtime_min", "QL_diff",
        "Downtime vs QL Difference (Akt − Pln)"
    )

    # table
    cols = [
        {"name": "Week", "id": "Tydzień"},
        {"name": "Downtime (min)", "id": "Total_downtime_min"},
        {"name": "Efficiency %", "id": "Line_efficiency_pct"},
        {"name": "QL Diff", "id": "QL_diff"},
    ]
    data = dfl.sort_values("Tydzień")[["Tydzień", "Total_downtime_min",
                                       "Line_efficiency_pct", "QL_diff"]].to_dict("records")

    # KPI cards
    card = lambda title, value: html.Div(
        className="bg-gray-50 shadow rounded-xl p-4 w-40 text-center",
        children=[html.Div(title, className="text-sm text-gray-500"),
                  html.Div(value, className="text-xl font-semibold")],
    )
    kpis = [
        card("Avg Efficiency", f"{dfl['Line_efficiency_pct'].mean():.1f}%"),
        card("Avg Downtime",   f"{dfl['Total_downtime_min'].mean():.0f} min"),
        card("Avg QL Diff",    f"{dfl['QL_diff'].mean():.0f}"),
    ]

    return fig_eff, fig_ql, data, cols, kpis


# ------------------------------------------------------------------
# 4 – Main
# ------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=False)