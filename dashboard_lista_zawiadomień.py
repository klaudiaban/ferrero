import pandas as pd
import numpy as np
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder

df = pd.read_csv("cleaned_lista_zawiadomień.csv")
df['Data zawiadom.'] = pd.to_datetime(df['Data zawiadom.'])
df = df.sort_values(by=['Typ maszyny', 'Data zawiadom.'])

features = ['Typ maszyny', 'Dzień tygodnia', 'Priorytet', 'Rodzaj zawiad.']
encoders = {}
for col in features:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    encoders[col] = le

def train_model(window_days):
    df_sorted = df.sort_values(by=['Typ maszyny', 'Data zawiadom.']).copy()
    df_sorted['Następna awaria'] = df_sorted.groupby('Typ maszyny')['Data zawiadom.'].shift(-1)
    df_sorted['dni_do_następnej_awarii'] = (df_sorted['Następna awaria'] - df_sorted['Data zawiadom.']).dt.days
    df_sorted['Czy_awaria'] = (df_sorted['dni_do_następnej_awarii'] <= window_days).astype(int)
    df_sorted = df_sorted.dropna(subset=['Czy_awaria'])

    X = df_sorted[features]
    y = df_sorted['Czy_awaria']
    model = RandomForestClassifier(n_estimators=100, class_weight='balanced', random_state=42)
    model.fit(X, y)
    return model

# Pretrain models for 1, 2, 3 day windows
models = {d: train_model(d) for d in [1, 2, 3]}

app = Dash(__name__)
app.title = "Predykcja Awarii - Dashboard"

app.layout = html.Div([
    html.H1("Dashboard predykcji awarii", style={"textAlign": "center"}),
    html.Div([
        html.Label("Typ maszyny"),
            dcc.Dropdown(
                options=[{"label": encoders['Typ maszyny'].inverse_transform([val])[0], "value": val}
                        for val in sorted(df['Typ maszyny'].unique())],
                value=[df['Typ maszyny'].unique()[0]],
                multi=True,
                id="machine-select"
            )
    ], style={"width": "30%", "display": "inline-block"}),

    html.Div([
        html.Label("Rodzaj dnia"),
        dcc.Dropdown(
            id="daytype-select",
            options=[
                {"label": "Dni robocze (pon–pt)", "value": "workday"},
                {"label": "Weekend (sob–ndz)", "value": "weekend"}
            ],
            value="workday"
        )
    ], style={"width": "30%", "display": "inline-block", "marginLeft": "20px"}),

    html.Div([
        html.Label("Okno predykcji (dni)"),
        dcc.Dropdown(
            options=[{"label": f"{d} dzień" if d == 1 else f"{d} dni", "value": d} for d in [1, 2, 3]],
            value=1,
            id="window-select"
        )
    ], style={"width": "30%", "display": "inline-block", "marginLeft": "20px"}),

    html.Br(), html.Hr(),

    html.Div(id="output-probability"),

    html.Br(),

    dcc.Graph(id="bar-chart"),

    html.Br(),

    dcc.Graph(id="heatmap"),

    html.Br(),
    html.Label("Rodzaj zawiadomienia"),
    dcc.Dropdown(
        id="rodzaj-select",
        options=[{"label": encoders['Rodzaj zawiad.'].inverse_transform([val])[0], "value": val}
                for val in sorted(df['Rodzaj zawiad.'].unique())] + [{"label": "Wszystkie", "value": "all"}],
        value="all"
    ),
    dcc.Graph(id="monthly-scatter")
])

@app.callback(
    Output("output-probability", "children"),
    Output("bar-chart", "figure"),
    Output("heatmap", "figure"),
    Output("monthly-scatter", "figure"),
    Input("machine-select", "value"),
    Input("window-select", "value"),
    Input("rodzaj-select", "value"),
    Input("daytype-select", "value")
)
def update_output(machine_ids, window_days, rodzaj_select, daytype_select):
    # Ensure input is a list
    if not isinstance(machine_ids, list):
        machine_ids = [machine_ids]

    # Prawdopodobieństwo dla wszystkich wybranych maszyn
    prob_texts = []
    day_id = 2 if daytype_select == "workday" else 6 
    for m in machine_ids:
        input_row = pd.DataFrame([{
            'Typ maszyny': m,
            'Dzień tygodnia': day_id,
            'Priorytet': df['Priorytet'].mode()[0],
            'Rodzaj zawiad.': df['Rodzaj zawiad.'].mode()[0]
        }])
        p = models[window_days].predict_proba(input_row)[0][1]
        machine_label = encoders['Typ maszyny'].inverse_transform([m])[0]
        day_name_map = {
            0: "poniedziałek", 1: "wtorek", 2: "środa",
            3: "czwartek", 4: "piątek", 5: "sobota", 6: "niedziela"
        }
        weekday_label = day_name_map[day_id]
        prob_texts.append(f"{machine_label} → {p:.2%} (dzień: {weekday_label})")

    text = html.Ul([html.Li(t) for t in prob_texts])

    inputs = pd.DataFrame([{
        'Typ maszyny': m,
        'Dzień tygodnia': day_id,
        'Priorytet': df['Priorytet'].mode()[0],
        'Rodzaj zawiad.': df['Rodzaj zawiad.'].mode()[0]
    } for m in machine_ids])
    for col in inputs.columns:
        inputs[col] = inputs[col].astype(int)

    # Bar Chart
    probs = models[window_days].predict_proba(inputs)[:, 1]
    machine_labels = encoders['Typ maszyny'].inverse_transform(machine_ids)
    bar_fig = px.bar(
        y=machine_labels, x=probs, orientation='h',
        labels={'y': 'Typ maszyny', 'x': 'Prawdopodobieństwo awarii'},
        title=f"Prawdopodobieństwo awarii ({window_days} dni)",
        height=400
    )

    # Heatmap
    heat_data = []
    for w in [1, 2, 3]:
        model = models[w]
        probs_w = model.predict_proba(inputs)[:, 1]
        heat_data.append(probs_w)

    heatmap_df = pd.DataFrame(
        heat_data,
        index=[f"{w} dni" for w in [1, 2, 3]],
        columns=encoders['Typ maszyny'].inverse_transform(machine_ids)
    ).T

    heat_fig = px.imshow(
        heatmap_df, text_auto=".2f", aspect="auto",
        color_continuous_scale="Blues",
        labels=dict(x="Okno predykcji", y="Typ maszyny", color="Prawdopodobieństwo"),
        title="Heatmapa ryzyka awarii"
    )

    df['month'] = df['Data zawiadom.'].dt.to_period('M').astype(str)
    df['Rodzaj text'] = encoders['Rodzaj zawiad.'].inverse_transform(df['Rodzaj zawiad.'])

    df_filtered = df[df['Typ maszyny'].isin(machine_ids)]

    if rodzaj_select != "all":
        rodzaj_label = encoders['Rodzaj zawiad.'].inverse_transform([rodzaj_select])[0]
        df_filtered = df_filtered[df_filtered['Rodzaj text'] == rodzaj_label]

    scatter_data = (
        df_filtered.groupby(['month', 'Rodzaj text'])
        .size()
        .reset_index(name='Liczba zgłoszeń')
    )

    # Scatter plot
    scatter_fig = px.line(
        scatter_data,
        x='month',
        y='Liczba zgłoszeń',
        color='Rodzaj text',
        markers=True,
        title="Liczba zgłoszeń miesięcznie (maszyna + rodzaj)",
        labels={'month': 'Miesiąc'},
    )

    return text, bar_fig, heat_fig, scatter_fig


if __name__ == "__main__":
    app.run(debug=True)
