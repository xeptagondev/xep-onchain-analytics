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

dash.register_page(__name__, path='/analytics', name="Analytics")
nav = create_navbar()
footer = create_footer()

# Database configurations
with open("config.json") as config_file:
    config = json.load(config_file)

# Connecting to PostgreSQL database
psqlconn = psycopg2.connect(database = config['postgre']['database'],
                            host = config['postgre']['host'],
                            user = config['postgre']['user'],
                            password = config['postgre']['password'],
                            port = config['postgre']['port'])



psqlcursor = psqlconn.cursor()

# Dataframe for our metrics and indicators
basic_metrics = pd.read_sql("SELECT * FROM basic_metrics", psqlconn)
computed_metrics = pd.read_sql("SELECT * FROM computed_metrics", psqlconn)

# Metrics and their descriptions
metrics_desc = pd.read_csv("assets/metrics_desc.csv")

content = html.Div([
    dbc.Row([
            # Control panel column
            dbc.Col([
                
                # Select Cryptocurrency 
                dbc.Row([
                    html.P(" Select Cryptocurrency", className = 'bi bi-coin', style={'color':'black', 'text-align':'center', 'font-size':'15px', 'font-family':'Open Sans', 'font-weight':'bold'}),
                    dbc.DropdownMenu(
                        [dbc.DropdownMenuItem("Bitcoin (BTC)", id="Bitcoin"),
                        dbc.DropdownMenuItem(divider=True),
                        dbc.DropdownMenuItem("Ethereum (ETH)", id="Ethereum", href="/analytics/ethereum"),
                        dbc.DropdownMenuItem(divider=True),
                        html.Div([
                            html.Span("to be implemented in future", className='disabled-info'),
                            dbc.DropdownMenuItem("Tether (USDT)", id="Tether", disabled=True),
                        ], className='disabled-coin-2'),
                        ],
                        id = 'cryptocurrency-select',
                        label = 'Bitcoin (BTC)',
                        color = '#0d1e26',
                        toggle_style = {'text-align':'center', 'font-size':'13px', 'width':'160px', 'height':'35px', 'color':'white', 'font-family': 'Open Sans'}
                    )
                ], style={'text-align':'center', 'padding-bottom':'15px'}),
                
                # Search Metrics
                dbc.Row([
                    dbc.Input(
                        id = 'searchbar',
                        placeholder = 'Search Metric',
                        type = 'search',
                        style = {'text-align':'center', 'width': '200px', 'height':'35px', 'border':'1px solid black','font-size':'13px', 'font-family': 'Open Sans'}
                    ),

                    dbc.ListGroup(
                        [dbc.ListGroupItem(x, action=True, id={"type": "list-group-item", "index": x}, color = '#E8EBEE00', style = {'font-size': '13px'}) for x in sorted(metrics_desc['metric_name'].tolist())], 
                        id='list-group', 
                        flush = True,
                        style={'margin-top':'15px', 'overflow-y':'scroll', 'width':'350px', 'height': '450px'})
                    
                ], justify = 'center', style = {'padding':'25px', 'border-top': '2px solid grey'}),

            ], width = 3, style = {'background-color':'#E8EBEE99',  'border-right':'2px solid grey', 'padding-top': '20px'}),

            # Display column
            dbc.Col([
                html.Div([
                    html.H5(id='graph-title', style = {'display': 'inline-block', 'vertical-align': 'middle', 'margin': '10px 0', 'color':'#0a275c'}),
                    dbc.Button(id='toast-toggle', n_clicks=0, className="bi bi-question-circle rounded-circle", color='white', style={'display': 'inline-block', 'vertical-align': 'middle', 'margin': '10px 0'}),
                    html.Div([
                        html.Span("Select your preferred date range.", className='date-picker-text', style = {'font-size':'12px'}),
                            dcc.DatePickerRange(
                            id='my-date-picker-range',
                            clearable = True,
                            min_date_allowed=date(2009, 1, 3),
                            max_date_allowed=datetime.now(),
                            start_date_placeholder_text='MM/DD/YYYY',
                            end_date_placeholder_text='MM/DD/YYYY',
                        ),
                    ], className='date-picker-div', style = {'display':'inline-block', 'position': 'relative', 'float':'right', 'margin-top':'13px'})
                ]),
                # area for toggable toast to display metric's formula
                html.Div(
                    dbc.Toast(
                            [dcc.Markdown(mathjax=True, id='metric-formula')],
                            id="toast",
                            header="How is it Calculated?",
                            dismissable=True,
                            is_open=False,
                            style = {'width':'40vw'}
                    ), style = {'padding-top': '15px', 'padding-bottom':'15px'}
                ),
                # area for metric description
                html.Div([
                    html.P("Metric Description", style={'font-weight':'bold', 'textDecoration':'underline'}),
                    html.P(id='metric-desc', style={'width':'48vw'}),
                ]),

                # area to display selected metric's graph
                dcc.Loading(
                    dcc.Graph(id="analytics-graph", style={'height': '80vh'}),
                    color='#0a275c'
                )

            ], width = 9, style = {'padding-right':'40px', 'padding-left':'30px', 'padding-top': '20px'})

    ], justify = 'evenly', style={'border-top': '2px solid grey', 'border-bottom': '1px solid grey'})
], style = {'padding-bottom':'60px'})



def create_charts():
    layout = html.Div([
        nav,
        content,
        footer
    ], style={'min-height':'100%', 'position':'relative', 'overflow-x':'hidden'})
    return layout

################## Callbacks ##############################

# Update dropdown label 
@app.callback(
    Output('cryptocurrency-select', "label"),
    [Input("Bitcoin", "n_clicks"), Input("Ethereum", "n_clicks"), Input("Tether", "n_clicks")]
)

def update_dropdown(n1, n2, n3):
    label_id = {"Bitcoin": "Bitcoin (BTC)", "Ethereum": "Ethereum (ETH)", "Tether": "Tether (USDT)"}
    # if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:
    #     return "Bitcoin (BTC)"
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    return label_id[button_id]

# Update listed metrics based on search term
@app.callback(
   Output("list-group", "children"),
   Input("searchbar", "value"),
   prevent_initial_call=True  
)
def update_metrics(searchterm):
    if searchterm == "": # when search bar is cleared
        return [dbc.ListGroupItem(x, action=True, id={"type": "list-group-item", "index": x}, color = '#E8EBEE00', style = {'font-size': '13px'}) for x in sorted(metrics_desc['metric_name'].tolist())]
    # When there is non-empty input to search bar
    result = metrics_desc['metric_name'].tolist().copy()
    for word in searchterm.split(" "):
        count=0
        while count < len(result):
            if word.lower() not in result[count].lower():
                result.remove(result[count])
            else:
                count+=1
    return [dbc.ListGroupItem(i, action=True, id={"type": "list-group-item", "index": i}, color = '#E8EBEE00', style = {'font-size': '13px'}) for i in result]

# Method to plot basic metrics
def plot_basic_metrics(fig, df, metric):
    if (metric != "Difficulty Ribbon"):
        fig.add_trace(go.Scatter(x=df['Date'], y=df[metric], mode='lines', line = dict(color = "#0a275c")))
    else:
        fig.add_trace(go.Scatter(x=df['Date'], y=df["Difficulty"], mode='lines', line = dict(color = "#0a275c"), name = "Market"))

        rolling_window = [14, 25, 40, 60, 90, 128, 200]
        for i in rolling_window:
            rolling_mean = df["Difficulty"].rolling(window=i).mean()
            trace = go.Scatter(x = df['Date'], y=rolling_mean, mode='lines', line = dict(color = "rgba(255, 0, 0, 0.5)"), name = "D{}".format(i))
            fig.add_trace(trace)

    return fig

# Update line graph data
@app.callback(
    Output("analytics-graph", "figure"), 
    Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
    Input('my-date-picker-range', 'start_date'),
    Input('my-date-picker-range', 'end_date'),
    Input('graph-title', 'children'),
    Input({'type': 'list-group-item', 'index': ALL}, 'id')
)
def update_line_chart(n_clicks_list, start, end, curr_metric, id_list):
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
    fig = go.Figure()
    if ctx.triggered_id is None or 1 not in n_clicks_list:
        fig.add_trace(go.Scatter(x=basic_metrics['Date'], y=basic_metrics['Price ($)'],
                    mode='lines', line = dict(color = "#0a275c")))
        
    elif ctx.triggered_id is not None or 1 in n_clicks_list:
        clicked = id_list[n_clicks_list.index(1)]['index']
        if metrics_desc[metrics_desc['metric_name'] == clicked]['is_computed'].values[0]:
            fig.add_trace(go.Scatter(x=computed_metrics['Date'], y=computed_metrics[clicked],
                    mode='lines', line = dict(color = "#0a275c")))
        else:
            plot_basic_metrics(fig, basic_metrics, clicked)

    # Update graph based on date range selected by user
    if start is not None and end is not None:
        if metrics_desc[metrics_desc['metric_name'] == curr_metric]['is_computed'].values[0]:
            filtered_df = computed_metrics[computed_metrics['Date'].between(start, end)]
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=filtered_df['Date'], y=filtered_df[curr_metric],
                    mode='lines', line = dict(color = "#0a275c")))
        else:
            filtered_df = basic_metrics[basic_metrics['Date'].between(start, end)]
            fig = go.Figure()
            plot_basic_metrics(fig, filtered_df, curr_metric)
        fig.update_xaxes(rangeslider=dict(
                            visible=True,
                            bgcolor="#d0e0e5",
                            thickness=0.1))
        fig.update_yaxes(title_text = curr_metric)

    # Return default range when datepicker is empty/cleared
    elif start is None and end is None:
        if metrics_desc[metrics_desc['metric_name'] == curr_metric]['is_computed'].values[0]:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=computed_metrics['Date'], y=computed_metrics[curr_metric],
                    mode='lines', line = dict(color = "#0a275c")))
        else:
            fig = go.Figure()
            plot_basic_metrics(fig, basic_metrics, curr_metric)
        fig.update_layout(
            xaxis=default
        )
        fig.update_yaxes(title_text = curr_metric)
    fig.update_traces(hovertemplate='Date: %{x} <br>Value: %{y}')
    fig.update_xaxes(title_text = "Date")
    fig.update_layout(plot_bgcolor='white')
    fig.update_xaxes(rangeselector_font_size = 15)
    
    return fig

# Update graph title and description
@app.callback(
    Output('graph-title', 'children'),
    Output('metric-desc', "children"),
    Output('metric-formula', 'children'),
    Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
)
def update_title_desc(n_clicks_list):
    if not ctx.triggered or 1 not in n_clicks_list:
        return "Price ($)", metrics_desc[metrics_desc['metric_name'] == 'Price ($)']['description'], metrics_desc[metrics_desc['metric_name'] == 'Price ($)']['formula']
    clicked_id = ctx.triggered_id.index
    return clicked_id, metrics_desc[metrics_desc['metric_name'] == clicked_id]['description'], metrics_desc[metrics_desc['metric_name'] == clicked_id]['formula']

# Toggling metric's description to be shown or not
@app.callback(
    Output("toast", "is_open"),
    Input("toast-toggle", "n_clicks"),
)
def open_toast(n):
    if n == 0:
        return False
    return True