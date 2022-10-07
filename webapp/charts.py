from dash import html, dcc, ALL, ctx
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from app import app
from navbar import create_navbar
import duckdb as ddb
import pandas as pd
import plotly.express as px
from datetime import date, datetime

nav = create_navbar()

# Connecting to database
conn = ddb.connect("/home/ec2-user/bitcoin-basic-metrics/bitcoin.duckdb", read_only=True)
c = conn.cursor()
df_price = pd.read_sql("SELECT * FROM bitcoin_price", conn)
df_metrics = pd.read_sql("SELECT * FROM bitcoin_metrics", conn)

# Metrics and their descriptions
metrics_desc = pd.read_sql("SELECT * FROM metric_desc", conn)


content = html.Div([
    dbc.Row([
            # Control panel column
            dbc.Col([
                
                # Select Cryptocurrency 
                dbc.Row([
                    html.P(" Select Cryptocurrency", className = 'bi bi-coin', style={'color':'black', 'text-align':'center', 'font-size':'15px', 'font-family':'Open Sans', 'font-weight':'bold'}),
                    dbc.DropdownMenu(
                        [ dbc.DropdownMenuItem("Bitcoin (BTC)", id="Bitcoin"),
                        dbc.DropdownMenuItem(divider=True),
                        dbc.DropdownMenuItem("Ethereum (ETH)", id="Ethereum", disabled = True),
                        dbc.DropdownMenuItem(divider=True),
                        dbc.DropdownMenuItem("Tether (USDT)", id="Tether", disabled = True),
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

                dbc.Row([
                    html.P("Select a date range", style = {'align':'left'}),
                    html.Div([
                        dcc.DatePickerRange(
                            id='my-date-picker-range',
                            clearable = True,
                            min_date_allowed=date(2009, 1, 3),
                            max_date_allowed=datetime.now(),
                            start_date_placeholder_text='MM/DD/YY',
                            end_date_placeholder_text='MM/DD/YY',
                        ),
                    ]),
                ], justify = 'center', style = {'text-align':'left', 'padding': '0px 25px 25px 25px'})

            ], width = 3, style = {'background-color':'#E8EBEE99',  'border-right':'2px solid grey', 'padding-top': '20px'}),

            # Area to display selected indicator's graph
            dbc.Col([
                html.Div([
                    html.H5('Bitcoin Price (USD)', id='graph-title', style = {'display': 'inline-block', 'vertical-align': 'middle', 'margin': '10px 0'}),
                    dbc.Button(id='toast-toggle', n_clicks=0, className="bi bi-question-circle rounded-circle", color='white', style={'display': 'inline-block', 'vertical-align': 'middle', 'margin': '10px 0'}),
                ]),
                
                html.Div(
                    dbc.Toast(
                            [html.P('Current price per unit of Bitcoin in USD', id='metric-desc')],
                            id="toast",
                            header="Metric Description",
                            dismissable=True,
                            is_open=False,
                            style = {'width':'500px'}
                    ), style = {'padding-top': '15px', 'padding-bottom':'15px'}
                ),
                dcc.Graph(id="analytics-graph"),
                #html.P('Metric Description', style={'textDecoration': 'underline'}),
                #html.P('Current price per unit of Bitcoin in USD', id='metric-desc')

            ], width = 9, style = {'padding-right':'40px', 'padding-left':'30px', 'padding-top': '20px'})

    ], justify = 'evenly', style={'height': '100vh', 'border-top': '2px solid grey'})

])


def create_charts():
    layout = html.Div([
        nav,
        content
    ])
    return layout


################## Callbacks ##############################

# Update dropdown label 
@app.callback(
    Output('cryptocurrency-select', "label"),
    [Input("Bitcoin", "n_clicks"), Input("Ethereum", "n_clicks"), Input("Tether", "n_clicks")]
)

def update_dropdown(n1, n2, n3):
    label_id = {"Bitcoin": "Bitcoin (BTC)", "Ethereum": "Ethereum (ETH)", "Tether": "Tether (USDT)"}
    if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:
        return "Bitcoin (BTC)"
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    return label_id[button_id]

# Update listed metrics based on search term
@app.callback(
   Output("metric-list", "children"),
   Input("searchbar", "value"),
   prevent_initial_call=True  
)
def update_metrics(searchterm):
    if searchterm == "": #when search bar is cleared
        return [dbc.ListGroupItem(x, action=True, id={"type": "list-group-item", "index": x}, color = '#E8EBEE00', style = {'font-size': '13px'}) for x in sorted(metrics_desc['metric_name'].tolist())]
    #when there is non-empty input to search bar
    result = metrics_desc['metric_name'].tolist().copy()
    for word in searchterm.split(" "):
        count=0
        while count < len(result):
            if word.lower() not in result[count].lower():
                result.remove(result[count])
            else:
                count+=1
    return [dbc.ListGroupItem(i, action=True, id={"type": "list-group-item", "index": i}, color = '#E8EBEE00', style = {'font-size': '13px'}) for i in result]

# Update line graph data
@app.callback(
    Output("analytics-graph", "figure"), 
    Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks')
)
def update_line_chart(n_clicks_list):
    if ctx.triggered_id is None:
        fig = px.line(df_metrics, x="time", y="Price ($)")
    else:
        clicked_id = ctx.triggered_id.index
        print(clicked_id)
        fig = px.line(df_metrics, x="time", y=clicked_id)
    fig.update_layout(plot_bgcolor='white')
    return fig

# Update graph title and description
@app.callback(
    Output('graph-title', 'children'),
    Output('metric-desc', "children"),
    Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
    prevent_initial_call=True
)
def update_title_desc(n_clicks_list):
    clicked_id = ctx.triggered_id.index
    return clicked_id, metrics_desc[metrics_desc['metric_name'] == clicked_id]['description']  

# 
@app.callback(
    Output("toast", "is_open"),
    Input("toast-toggle", "n_clicks"),
)
def open_toast(n):
    if n == 0:
        return False
    return True

# Update time period of graph displayed