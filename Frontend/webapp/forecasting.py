import dash
from dash import html, dcc, ALL, ctx
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from app import app
from navbar import create_navbar
from footer import create_footer
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, datetime
import re
import duckdb as ddb
import psycopg2
import json
import dash_mantine_components as dmc
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import pmdarima as pm
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from pmdarima.arima import auto_arima
from datetime import datetime, timedelta, date
from dash.exceptions import PreventUpdate

dash.register_page(__name__, path='/forecasting', name="Forecasting")
nav = create_navbar()
footer = create_footer()

df = pd.read_csv("assets/eth_usd.tsv", sep = '\t').rename(columns = {'Price (ETH/USD) – Ethereum': 'price'})
df['date'] = df['Time'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y'))
df.set_index('date', inplace=True)
df.drop(columns=['Time'], inplace=True)
df = df[df.index >= '2022-01-01']

forecast_metrics = ['Price (ETH/USD)']

# Function to split data into train and test
def train_test_split(data):
    train_size = int(len(data)-7)
    train, test = data.iloc[:train_size], data.iloc[train_size:]
    return train, test

def run_forecasting_model(data, model = 'SMA'):
    train, test = train_test_split(data)

    num_forecast_steps = len(test)
    start_date = test.index[0]

    if model == 'sma':
        window = 10
        date_range = pd.date_range(start=start_date, periods=num_forecast_steps, freq='D')
        forecast = pd.Series(index=date_range)

        for i in range(num_forecast_steps):
            # Use only the data up to the current point for the moving average
            current_window = train.iloc[-window:]
            # Calculate the moving average
            moving_avg = current_window.mean()
            # Append the forecast value to the list
            forecast.iloc[i] = moving_avg
            new_series = pd.Series([moving_avg], index=[forecast.index[i]])

            train = train.append(new_series)

        return train

    elif model == 'arima':
        date_range = pd.date_range(start=start_date, periods=len(test), freq='D')
        forecast = pd.Series(index=date_range)

        arima_model = ARIMA(train, order = (1,1,0))
        model_fit = arima_model.fit()
        forecast_values = model_fit.forecast(len(test))
        forecast = forecast.fillna(forecast_values)

        train = train.append(forecast)

        return train
    
    elif model == 'exp':
        date_range = pd.date_range(start=start_date, periods=len(test), freq='D')
        forecast = pd.Series(index=date_range)

        model = ExponentialSmoothing(train, trend='mul')
        model_fit = model.fit()
        forecast_values = model_fit.forecast(num_forecast_steps)
        forecast = forecast.fillna(forecast_values)

        train = train.append(forecast)

        return train

content = html.Div([
    dbc.Row([
            # Control panel column
            dbc.Col([
                
                # Select Cryptocurrency 
                dbc.Row([
                    html.P(" Select Cryptocurrency", className = 'bi bi-coin', style={'color':'black', 'text-align':'center', 'font-size':'15px', 'font-family':'Open Sans', 'font-weight':'bold'}),
                    dbc.DropdownMenu(
                        [dbc.DropdownMenuItem("Ethereum (ETH)", id="Ethereum-5"),
                        dbc.DropdownMenuItem(divider=True),
                        html.Div([
                            html.Span("to be implemented in future", className='disabled-info'),
                            dbc.DropdownMenuItem("Bitcoin (BTC)", id="Bitcoin-5", disabled=True),
                        ], className='disabled-coin'),
                        dbc.DropdownMenuItem(divider=True),
                        html.Div([
                            html.Span("to be implemented in future", className='disabled-info'),
                            dbc.DropdownMenuItem("Tether (USDT)", id="Tether-5", disabled=True),
                        ], className='disabled-coin-2'),
                        ],
                        id = 'cryptocurrency-select-5',
                        label = 'Ethereum (ETH)',
                        color = '#0d1e26',
                        toggle_style = {'text-align':'center', 'font-size':'13px', 'width':'160px', 'height':'35px', 'color':'white', 'font-family': 'Open Sans'}
                    )
                ], style={'text-align':'center', 'padding-bottom':'15px'}),
                
                # Model selection menu
                dbc.Row(html.Div(id = 'menu-forecast'), justify = 'center', style = {'padding':'25px', 'border-top': '2px solid grey'})

            ], width = 3, style = {'background-color':'#E8EBEE99',  'border-right':'2px solid grey', 'padding-top': '20px'}),

            # Display column
            dbc.Col([
                html.Div([
                    html.H5(id='graph-title-forecast', style = {'display': 'inline-block', 'vertical-align': 'middle', 'margin': '10px 0', 'color':'#0a275c'}),
                    html.Div([
                        html.Span("Select your preferred date range.", className='date-picker-text', style = {'font-size':'12px'}),
                            dmc.DateRangePicker(
                                id='date-range-picker',
                                placeholder='DD/MM/YYYY - DD/MM/YYYY',
                                inputFormat="DD/MM/YYYY",
                                clearable = True,
                                minDate=date(2022, 1, 1),
                                maxDate=datetime.now(),
                                style = {'width': 280}
                            ),
                    ], className='date-picker-div', style = {'display':'inline-block', 'position': 'relative', 'float':'right', 'margin-top':'13px'})
                ]),
        
                # area for metric description
                html.Div([
                    html.P("Model Description", style={'font-weight':'bold', 'textDecoration':'underline'}),
                    html.P(id='model-desc', style={'width':'48vw'}),
                ]),

                # area to display selected metric's graph
                dcc.Loading(
                    dcc.Graph(id="forecast-graph", style={'height': '80vh'}),
                    color='#0a275c'
                )

            ], width = 9, style = {'padding-right':'40px', 'padding-left':'30px', 'padding-top': '20px'})

    ], justify = 'evenly', style={'border-top': '2px solid grey', 'border-bottom': '1px solid grey'})
], style = {'padding-bottom':'60px'})

def create_forecasting():
    layout = html.Div([
        nav,
        content,
        footer
    ], style={'min-height':'100%', 'position':'relative', 'overflow-x':'hidden'})
    return layout

################## Callbacks ##############################
# Update dropdown label
@app.callback(
    Output('cryptocurrency-select-5', "label"),
    [Input("Bitcoin-5", "n_clicks"), Input("Ethereum-5", "n_clicks"), Input("Tether-5", "n_clicks")]
)

def update_dropdown(n1, n2, n3):
    label_id = {"Bitcoin-5": "Bitcoin (BTC)", "Ethereum-5": "Ethereum (ETH)", "Tether-5": "Tether (USDT)"}
    if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:
        return "Ethereum (ETH)"
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    return label_id[button_id]


# Update forecasting model menu
@app.callback(
    Output('menu-forecast', 'children'),
    Input('cryptocurrency-select-5', 'label')
)

def update_menu(selected_cryptocurrency):
    if selected_cryptocurrency == 'Ethereum (ETH)':
        return [
                    dbc.Accordion(
                    dbc.AccordionItem(
                        dbc.ListGroup([
                            dbc.ListGroupItem("Simple Moving Average", action=True, id={"type":'forecast-sma', "index": "myindex"}, color='#E8EBEE00', style={'cursor': 'pointer'}),
                            dbc.ListGroupItem("ARIMA", action=True, id={"type":'forecast-arima', "index": "myindex"}, color='#E8EBEE00', style={'cursor': 'pointer'}),
                            dbc.ListGroupItem("Exponential Smoothing", action=True, id={"type":'forecast-exp', "index": "myindex"}, color='#E8EBEE00', style={'cursor': 'pointer'}),
                        ], flush=True, style={'font-size': '14px'}),
                        
                        title="Price Forecasting"
                    ), 
                    flush=True, start_collapsed=True, style = {'width':'300px', 'margin-top':'15px', 'margin-left': 'auto', 'margin-right': 'auto'}
                )
                ]

# Update line graph data
@app.callback(
    Output("forecast-graph", "figure"), 
    Input('date-range-picker', 'value'),
    Input('graph-title-forecast', 'children'),
    [Input({"type": "forecast-sma", "index": ALL}, "n_clicks"), Input({"type": "forecast-arima", "index": ALL}, "n_clicks"), Input({"type": "forecast-exp", "index": ALL}, "n_clicks")]
)

def update_line_chart(dates, curr_model, sma_model, arima_model, exponential_model):
    # provides default range selectors
    default = dict(rangeselector=dict(buttons=list([
                dict(count=1,
                    label="1M",
                    step="month",
                    stepmode="backward"),
                dict(count=6,
                    label="6M",
                    step="month",
                    stepmode="backward"),
                dict(count=1,
                    label="YTD",
                    step="year",
                    stepmode="todate"),
                dict(count=1,
                    label="1Y",
                    step="year",
                    stepmode="backward"),
                dict(label="ALL", step="all")
            ])
        ),
        rangeslider=dict(
            visible=True,
            bgcolor="#d0e0e5",
            thickness=0.1 
        ),
        type="date"
    )

    if curr_model == 'Simple Moving Average':
        model = 'sma'

    elif curr_model == 'ARIMA':
        model = 'arima'

    elif curr_model == 'Exponential Smoothing':
        model = 'exp'
            
    fig = go.Figure()

    # Run the forecasting model
    data = df['price']
    train = run_forecasting_model(data, model)

    data = data[data.index <= '2023-11-06']
    train = train[train.index >= '2023-11-06']

    # Plotting the actual data and forecast data on the same graph
    fig = go.Figure()

    if dates:
        start, end = dates
        data = data[(data.index >= start) & (data.index <= end)]
        train = train[(train.index >= start) & (train.index <= end)]
    
        # Add points
        fig.add_trace(go.Scatter(x=train.index, y=train, mode='markers+lines', name='Forecast Points', marker=dict(color='red', size=8)))
        fig.add_trace(go.Scatter(x=data.index, y=data, mode='markers+lines', name='Actual Points', marker=dict(color='blue', size=8)))

    
    else:
        # Add points
        fig.add_trace(go.Scatter(x=train.index[-50:], y=train.iloc[-50:], mode='markers+lines', name='Forecast Points', marker=dict(color='red', size=8)))
        fig.add_trace(go.Scatter(x=data.index[-50:], y=data.iloc[-50:], mode='markers+lines', name='Actual Points', marker=dict(color='blue', size=8)))

    fig.update_traces(mode="markers+lines", hovertemplate='%{y}')

    fig.update_xaxes(title_text = "Date")
    fig.update_layout(plot_bgcolor='white', hovermode='x unified', hoverlabel = dict(namelength = -1))
    fig.update_xaxes(rangeselector_font_size = 15)

    return fig

# Update graph title and description
@app.callback(
    Output('graph-title-forecast', 'children'),
    Output('model-desc', "children"),
    [Input({"type": "forecast-sma", "index": ALL}, "n_clicks"), Input({"type": "forecast-arima", "index": ALL}, "n_clicks"), Input({"type": "forecast-exp", "index": ALL}, "n_clicks")]
)

def update_title_desc(sma_model, arima_model, exponential_model):
    selected = ctx.triggered[0]["prop_id"].split(".")[0]

    model_desc = {'sma': 'A simple moving average (SMA) calculates the average of a selected range of prices, usually closing prices, by the number of periods in that range.',
                  'arima': 'An autoregressive integrated moving average model is a form of regression analysis that gauges the strength of one dependent variable relative to other changing variables.',
                  'exp': 'Exponential Smoothing methods use weighted averages of past observations to forecast new values. The idea is to give more importance to recent values in the series. Thus, as observations get older in time, the importance of these values get exponentially smaller.'}
    
    if not selected:
        return "Simple Moving Average", model_desc['sma']
    

    mytype = json.loads(selected)["type"]
    model = mytype.split('-')[1]

    if model == 'sma':
        return "Simple Moving Average", model_desc['sma']
    
    elif model == 'arima':
        return "ARIMA", model_desc['arima']
    
    elif model == 'exp':
        return "Exponential Smoothing", model_desc['exp']
