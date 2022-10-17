from dash import html, dcc, ALL, ctx
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from app import app
from navbar import create_navbar
import duckdb as ddb
import pandas as pd
import plotly.express as px
from datetime import date, datetime
import psycopg2
from sqlalchemy import create_engine

nav = create_navbar()

# Connecting to PostgreSQL Database
engine = create_engine('postgresql://ec2-user:password@localhost:5432/bitcoin')
psqlconn = psycopg2.connect(database="bitcoin",
                            host="localhost",
                            user="ec2-user",
                            password="password",
                            port="5432")
psqlcursor = psqlconn.cursor()

# Dataframe for output of Anomaly Detection
df_isoForest = pd.read_sql('SELECT * FROM "isoForest_outliers"', psqlconn)
df_kmeans = pd.read_sql('SELECT * FROM "kmeans_outliers"', psqlconn)

content = html.Div([
    dbc.Row([
            # Control panel column
            dbc.Col([
                
                # Select Cryptocurrency 
                dbc.Row([
                    html.P(" Select Cryptocurrency", className = 'bi bi-coin', style={'color':'black', 'text-align':'center', 'font-size':'15px', 'font-family':'Open Sans', 'font-weight':'bold'}),
                    dbc.DropdownMenu(
                        [ dbc.DropdownMenuItem("Bitcoin (BTC)", id="Bitcoin-2"),
                        dbc.DropdownMenuItem(divider=True),
                        dbc.DropdownMenuItem("Ethereum (ETH)", id="Ethereum-2", disabled = True),
                        dbc.DropdownMenuItem(divider=True),
                        dbc.DropdownMenuItem("Tether (USDT)", id="Tether-2", disabled = True),
                        ],
                        id = 'cryptocurrency-select-2',
                        label = 'Bitcoin (BTC)',
                        color = '#0d1e26',
                        toggle_style = {'text-align':'center', 'font-size':'13px', 'width':'160px', 'height':'35px', 'color':'white', 'font-family': 'Open Sans'}
                    )
                ], style={'text-align':'center', 'padding-bottom':'15px'}),

                # Detection type
                dbc.Row([
                    dbc.Accordion(
                        dbc.AccordionItem(
                            dbc.ListGroup([
                                dbc.ListGroupItem("Table Summary", action=True, id='anomaly-table', color='#E8EBEE00'),
                                dbc.ListGroupItem("Blocks", action=True, id='anomaly-blocks', color='#E8EBEE00'),
                                dbc.ListGroupItem("Transactions", action=True, id='anomaly-trans', color='#E8EBEE00')
                            ], flush=True, style={'font-size':'14px'}),
                            
                            title="Address Detection"
                        ), 
                        flush=True, start_collapsed=True, style = {'width':'300px', 'margin-top':'15px'}
                    ),
                    dbc.Accordion(
                        dbc.AccordionItem(
                            dbc.ListGroup([
                                dbc.ListGroupItem("Isolation Forest", action=True, id='outlier-isoForest', color='#E8EBEE00'),
                                dbc.ListGroupItem("K-Means Clustering", action=True, id='outlier-kmeans', color='#E8EBEE00')
                            ], flush=True, style={'font-size':'14px'}),
                            
                            title="Outlier Detection"
                        ), 
                        flush=True, start_collapsed=True, style = {'width':'300px', 'margin-top':'15px'}
                    )
                    
                ], justify = 'center', style = {'padding':'25px', 'border-top': '2px solid grey'}),

            ], width = 3, style = {'background-color':'#E8EBEE99',  'border-right':'2px solid grey', 'padding-top': '20px'}),

            # Table / Graph display segment
            dbc.Col([
                html.H4(id="anomaly-title"),

                dcc.Graph(id="anomaly-graph")

            ], width = 9, style = {'padding-right':'40px', 'padding-left':'30px', 'padding-top': '20px'})
    
    ], justify = 'evenly', style={'height': '100vh', 'border-top': '2px solid grey'})

])


def create_anomaly():
    layout = html.Div([
        nav,
        content,
    ])
    return layout


################## Callbacks ##############################

# Update dropdown label 
@app.callback(
    Output('cryptocurrency-select-2', "label"),
    [Input("Bitcoin-2", "n_clicks"), Input("Ethereum-2", "n_clicks"), Input("Tether-2", "n_clicks")]
)

def update_dropdown(n1, n2, n3):
    label_id = {"Bitcoin-2": "Bitcoin (BTC)", "Ethereum-2": "Ethereum (ETH)", "Tether-2": "Tether (USDT)"}
    if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:
        return "Bitcoin (BTC)"
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    return label_id[button_id]

# Update graph display title
@app.callback(
    Output('anomaly-title', "children"),
    [Input("anomaly-table", "n_clicks"), Input("anomaly-blocks", "n_clicks"), Input("anomaly-trans", "n_clicks"), 
    Input("outlier-isoForest", "n_clicks"), Input("outlier-kmeans", "n_clicks")]
)

def update_title(n1,n2,n3,n4,n5):
    titles_dict = {"anomaly-table": "Table of Anomalous Transactions", "anomaly-blocks": "Anomalous Blocks Across Time", "anomaly-trans": "Anomalous Transactions Across Time",
                   "outlier-isoForest": "Outliers Detected Using Isolation Forest", "outlier-kmeans": "Outliers Detected Using K-Means Clustering"}
    if not ctx.triggered: #default
        return "Overview"
    selected = ctx.triggered[0]["prop_id"].split(".")[0]
    return titles_dict[selected]

