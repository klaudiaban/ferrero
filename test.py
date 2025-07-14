import pandas as pd
import dash
from dash import dcc, html, Output, Input
import plotly.express as px
import datetime as dt

# Load CSV
prod = pd.read_csv("test.csv", sep=";", encoding="utf-8-sig")
prod = prod.drop_duplicates()

# Fix headers with BOM characters
prod.columns = prod.columns.str.strip().str.replace('\uFEFF', '')

# Ensure numeric columns are cleaned
def clean_numeric(col):
    return (
        prod[col].astype(str)
        .str.replace(',', '.', regex=False)
        .str.replace(' ', '', regex=False)
        .astype(float)
    )

# Columns of interest
kpi_pairs = {
    "QL (TOT)": ["QL (TOT) Akt", "QL (TOT) Pln"],
    "Q/Zm.": ["Q/Zm. Akt", "Q/Zm. Standard"],
    "Q/CPK": ["Q/CPK Akt", "Q/CPK Docel"],
    "G/QL": ["G/QL Akt", "G/QL Docel"],
    "Godz./Prac.": ["Akt Godz./Prac.", "Std. Godz. Prac."],
    "% Scrap": ["% SF/SP Eff", "% SF/SP Std."],
    "Oszczędności Godz./Prac.": ["Suma Oszczędności Godz./Prac."]
}

# Clean necessary columns
for pair in kpi_pairs.values():
    for col in pair:
        if col in prod.columns:
            prod[col] = clean_numeric(col)

# Convert line and week
prod['Linia'] = pd.to_numeric(prod['Linia'], errors='coerce').astype('Int64')
prod['Tydzień'] = prod['Tydzień'].astype(str).str.strip()

# App init
app = dash.Dash(__name__)
app.title = "Ferrero KPI Dashboard"

# Layout
app.layout = html.Div(className='p-6 font-sans space-y-4', children=[
    html.H2("KPI Dashboard by Linia", className='text-2xl font-bold'),
    html.Div(className='flex gap-4', children=[                        
        html.Div(children=[
            html.Label("Select Line:"),
            dcc.Dropdown(
                id='line-select',
                options=[{"label": str(l), "value": l}
                         for l in sorted(prod['Linia'].dropna().unique())]
                         + [{"label": "All Lines", "value": "ALL"}],
                value="ALL",
                clearable=False,
                style={"width": "220px"}
            )
        ]),
        html.Div(children=[
            html.Label("Period:"),
            dcc.Dropdown(
                id='period-select',
                options=[
                    {"label": "Week",  "value": "WEEK"},
                    {"label": "Month", "value": "MONTH"}
                ],
                value="WEEK",
                clearable=False,
                style={"width": "120px"}
            )
        ])
    ]),
    html.Div(id='plots', className='grid grid-cols-2 gap-4')
])


# ------------------------------------------------------------------
# Callbacks – targets rendered as line, actuals as bars
# ------------------------------------------------------------------
import plotly.graph_objects as go

# ------------------------------------------------------------------
# Callbacks – target dashed line, period switch (week / month)
# ------------------------------------------------------------------
@app.callback(
    Output('plots', 'children'),
    [Input('line-select', 'value'),
     Input('period-select', 'value')]                    
)
def update_kpis(selected_line, period):
    # ---------- filter by line ----------
    if selected_line == "ALL":
        df = prod.copy()
        title_suffix = " (All Lines)"
    else:
        df = prod[prod['Linia'] == selected_line]
        title_suffix = f" (Linia {selected_line})"

    if period == "MONTH":
        # Convert ISO week code to first day of that week, then month label “YYYY-MM”
        #   e.g. “W05” → 2025-02-03 → “2025-02”
        def week_to_month(iso_week):
            week_num = int(iso_week.lstrip('W'))
            # assume data from 2025; change if multi-year
            date = dt.date(2025, 1, 4) + dt.timedelta(days=(week_num-1)*7)
            first_monday = date - dt.timedelta(days=date.weekday())
            return first_monday.strftime("%Y-%m")
        df['PERIOD'] = df['Tydzień'].apply(week_to_month)
        x_title = "Month"
    else:
        df['PERIOD'] = df['Tydzień']
        x_title = "Week"

    graphs = []

    for title, cols in list(kpi_pairs.items())[:6]:
        if len(cols) != 2 or any(col not in df.columns for col in cols):
            continue

        actual_col, target_col = cols
        dff = (
            df[['PERIOD', actual_col, target_col]]
            .dropna()
            .groupby('PERIOD', as_index=False)
            .sum()
            .sort_values('PERIOD')
        )

        import plotly.graph_objects as go
        fig = go.Figure()
        fig.add_trace(go.Bar(x=dff['PERIOD'], y=dff[actual_col],
                             name='Actual', opacity=0.7))
        fig.add_trace(go.Scatter(x=dff['PERIOD'], y=dff[target_col],
                                 name='Target',
                                 line=dict(width=3)))
        fig.update_layout(
            title=f"{title} – Actual vs Target{title_suffix}",
            template="simple_white", height=350,
            xaxis_title=x_title, yaxis_title=title,
            hovermode="x unified", margin=dict(l=40, r=20, t=60, b=40)
        )
        graphs.append(dcc.Graph(figure=fig))

    return graphs

# Run app
if __name__ == '__main__':
    app.run(debug=True)
