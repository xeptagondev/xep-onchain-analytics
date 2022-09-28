import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from app import app
from navbar import create_navbar
import duckdb as ddb
import pandas as pd
import plotly.express as px


nav = create_navbar()

# Connecting to database
conn = ddb.connect("/home/ec2-user/bitcoin-basic-metrics/bitcoin.duckdb")
c = conn.cursor()
df_price = pd.read_sql("SELECT * FROM bitcoin_price", conn)


# Full list of indicators
metrics_list = ['Price ($)', 'Transaction Volume', 'Circulating Supply', 'Maximum Supply', 'Market Capitalisation',
                'Realised Capitalisation', 'No. of Unique Addresses', 'Exchange Reserve', 'Tokens Transferred (24h)',
                'Transactions (24h)', 'No. of Active Addresses', 'Fully Diluted Market Cap',
                'Market Value to Realised Value Ratio (MVRV)', 'Relative Unrealised Profit', 'Net Unrealised Profit and Loss (NUPL)',
                'Net Realised Profit/Loss', 'Spent Output Profit Ratio (SOPR)', 'Network Value to Transaction (NPV)',
                'Velocity', 'Market Cap to Thermocap Ratio', 'Coin-Days-Destroyed (CDD)', 'Balanced Price',
                'Average Coin Dormancy', 'Stablecoin Supply Ratio (SSR)', 'Average Transaction Value (ATV)', 'Difficulty Ribbon']

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
                        [dbc.ListGroupItem(x, id = x, color = '#E8EBEE99', style = {'font-size': '13px'}) for x in sorted(metrics_list)], 
                        id='metric-list', 
                        flush = True,
                        style={'margin-top':'15px', 'overflow-y':'scroll', 'width':'350px', 'height': '415px'})
                    
                ], justify = 'center', style = {'padding-top':'15px', 'padding-left':'25px', 'border-top': '2px solid grey'})  
                
            ], width = 3, style = {'background-color':'#E8EBEE99',  'border-right':'2px solid grey', 'padding-top': '20px'}),

            # Area to display selected indicator's graph
            dbc.Col([
                html.H5('Bitcoin Price (USD)', id='graph-title'),
                dcc.Graph(id="price-graph"),
                html.P('Chart Description', id='metric-description'),
            ], width = 9, style = {'padding-right':'40px', 'padding-left':'30px', 'padding-top': '20px'})

    ], justify = 'evenly', style={'position':'fixed', 'height': '100vh', 'border-top': '2px solid grey'})

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
    ctx = dash.callback_context
    label_id = {"Bitcoin": "Bitcoin (BTC)", "Ethereum": "Ethereum (ETH)", "Tether": "Tether (USDT)"}
    if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:
        return "Bitcoin (BTC)"
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    return label_id[button_id]

# Update listed metrics based on search term
@app.callback(
   Output("metric-list", "children"),
   Input("searchbar", "value")  
)
def update_metrics(searchterm):
    if searchterm == "":
        return [dbc.ListGroupItem(x, id = x, color = '#E8EBEE99', style = {'font-size': '13px'}) for x in sorted(metrics_list)]
    result = metrics_list.copy()
    for word in searchterm.split(" "):
        count=0
        while count < len(result):
            if word.lower() not in result[count].lower():
                result.remove(result[count])
            else:
                count+=1
    return [dbc.ListGroupItem(i, id = i, color = '#E8EBEE99', style = {'font-size': '13px'}) for i in result]

# Update line graph data
@app.callback(
    Output("price-graph", "figure"), 
    Input("cryptocurrency-select", "value")
)
def update_line_chart(value):
    fig = px.line(df_price, x="time", y="price_usd")
    fig.update_layout(plot_bgcolor='white')
    return fig

# Update graph title

# Update graph descriptor