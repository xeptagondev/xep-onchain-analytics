from dash import html, dcc
from dash.dependencies import Input, Output
from home import create_page_home
from charts import create_charts
from charts_ethereum import create_charts_ethereum
from anomaly_models import create_anomaly_models
from anomaly_models_eth import create_anomaly_models_eth
from anomaly import create_anomaly
from forecasting import create_forecasting
from app import app
import dash

application = app.server
app.config.suppress_callback_exceptions = True

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])

def display_page(pathname):
    if pathname == '/analytics':
        return create_charts()
    if pathname == '/analytics/ethereum':
        return create_charts_ethereum()
    if pathname == '/anomaly':
        return create_anomaly()
    if pathname == '/anomaly/models':
        return create_anomaly_models()
    if pathname == '/anomaly/models/eth':
        return create_anomaly_models_eth()
    if pathname == "/forecasting":
        return create_forecasting()
    else:
        return create_page_home()


if __name__ == '__main__':
    app.run_server(debug=False)
