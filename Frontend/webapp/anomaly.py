import dash
from dash import html, dcc, ALL, ctx, dash_table
from dash.dependencies import Input, Output, State, ALL
import dash_bootstrap_components as dbc
from app import app
from navbar import create_navbar
from footer import create_footer
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, datetime
from dateutil.parser import parse
from dash.exceptions import PreventUpdate

import psycopg2
from sqlalchemy import create_engine
import json

dash.register_page(__name__, path='/anomaly', name="Anomaly Detection Overview")
nav = create_navbar()
footer = create_footer()

content = html.Div([
    dbc.Row([
            # Control panel column
            dbc.Col([
                
                # Select Cryptocurrency 
                dbc.Row([
                    html.P(" Select Cryptocurrency", className = 'bi bi-coin', style={'color':'black', 'text-align':'center', 'font-size':'15px', 'font-family':'Open Sans', 'font-weight':'bold'}),
                    dbc.DropdownMenu(
                        [dbc.DropdownMenuItem("Bitcoin (BTC)", id = "Bitcoin-2", href = '/anomaly/models'),
                        dbc.DropdownMenuItem(divider=True),
                        dbc.DropdownMenuItem("Ethereum (ETH)", id = "Ethereum-2", href = '/anomaly/models'),
                        dbc.DropdownMenuItem(divider=True),
                        html.Div([
                            html.Span("to be implemented in future", className='disabled-info'),
                            dbc.DropdownMenuItem("Tether (USDT)", id="Tether-2", disabled=True),
                        ], className='disabled-coin-2'),
                        ],
                        id = 'cryptocurrency-select-2',
                        label = 'Bitcoin (BTC)',
                        color = '#0d1e26',
                        align_end = True,
                        toggle_style = {'text-align':'center', 'font-size':'13px', 'width':'160px', 'height':'35px', 'color':'white', 'font-family': 'Open Sans'}
                    )
                ], style={'text-align':'center', 'padding-bottom':'15px'})
                
                # # Detection type
                # , dbc.Button("View Anomaly Detection Models", id = 'visit-models', href='/anomaly/models',
                #        style= {'background-color': '#0a275c', 'color':'white', 'text-transform':'none', 'font-weight': 900, 'border-radius':'15px', 'margin-top':'30px'})


            ], width = 3, style = {'background-color':'#E8EBEE99',  'border-right':'2px solid grey', 'padding-top': '20px', 'text-align':'center', 'horizontalAlign': 'middle'}),

            # Table / Graph display segment
            dbc.Col([
                html.Div(
                    html.H4(id="anomaly-overview", style = {'display':'inline-block', 'vertical-align':'center'}),
                    ),
                  
                html.Div(
                    dcc.Loading(
                        html.Div(id='anomaly-description'),
                        color='#0a275c',
                        style = {'position':'fixed', 'top': '50%'}
                    )

                , style= {'margin-top':'30px', 'width':'72vw', 'height':'70vh', 'overflow-y':'scroll'})

            ], width = 9, style = {'padding-right':'40px', 'padding-left':'30px', 'padding-top': '20px'})
    
    ], justify = 'evenly', style={'height': '100vh', 'border-top': '2px solid grey'})

], style = {'padding-bottom':'60px'})


def create_anomaly():
    layout = html.Div([
        nav,
        content,
        footer
    ], style={'min-height':'100%', 'position':'relative', 'overflow-x':'hidden'})
    return layout


############################## Callbacks ##############################

# Update dropdown label
@app.callback(
    Output('cryptocurrency-select-2', 'label'),
    [Input("Bitcoin-2", "n_clicks"), Input("Ethereum-2", "n_clicks"), Input("Tether-2", "n_clicks")]
)

def update_dropdown(n1, n2, n3):
    label_id = {"Bitcoin-2": "Bitcoin (BTC)", "Ethereum-2": "Ethereum (ETH)", "Tether-2": "Tether (USDT)"}
    if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:
        return "Bitcoin (BTC)"
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    
    return label_id[button_id]

# Update overview
@app.callback(
    Output('anomaly-overview', "children"),
    Output('anomaly-description', "children"),
    [Input("Bitcoin-2", "n_clicks"), Input("Ethereum-2", "n_clicks")]
)

def update_overview(n1, n2):
    ov_btc = """
        Due to the nature of a Blockchain network, cryptocurrencies are becoming the preferred option 
        for cybercriminal activities. Criminals are turning towards cryptocurrency as it is anonymous, 
        quick, requires no former relationship between parties, and can facilitate seamless international trades. 
        With the number of cybercriminal activities on the rise, members of the network would want to detect 
        these criminals as soon as possible to prevent them from harming the network’s community and integrity. 
        In financial networks, thieves and illegal activities are often anomalous in nature. Hence, we will be 
        implementing an anomaly detection framework which aims to reveal the identities of illicit transaction 
        makers as well as spot outliers in the network.
    """

    ov_eth = """
        Because of the unique characteristics of blockchain networks, cryptocurrencies have become the preferred choice 
        for cybercriminals. Criminals are increasingly using cryptocurrency due to its anonymity, speed, lack of prior 
        relationships required between parties, and its ability to facilitate seamless international transactions. As the 
        number of cybercriminal activities continues to rise, network members are eager to detect these criminals promptly 
        in order to protect the network's community and its integrity. In financial networks, illegal activities and thieves 
        often exhibit unusual patterns, so we intend to implement an anomaly detection framework that seeks to identify those 
        responsible for illicit transactions and pinpoint unusual behavior within the network.
    """

    address_btc = """
        The dataset is an input graph collected from Blockahir's bitcoin blockchain. A row in the dataset 
        represents an input along with its transactions data which is joined by its transaction hash. 
        Each node has 27 features used along with its hash which contains the transactions hash and its account 
        which contains the address of the recipient. Each input is marked as illicit if the recipient has 
        been reported to have used its account for illicit activites such as sextortion, blackmail, darknet market, 
        ransomeware etc. Illicit address are collected from bitcoinabuse.com which has the cryptocurrency community 
        reporting addresses where they have encountered activity from hackers and criminals to make the internet a safer place. 
        Additional address are also collected from the BABD-13 dataset used in the Basic models are created and trained for 
        demonstration purposes on 2022-07-01 data, parameters are available in our GitHub source code.
    """

    address_eth = """
        The dataset utilized for training our models is "A Labeled Transactions-Based Dataset on the Ethereum Network," 
        also referred to as BLTE (Benchmark Labeled Transactions of Ethereum Network). Each row in the dataset corresponds to
        an input and includes its associated transaction data. The dataset initially consists of 18 features, but through 
        preprocessing and feature engineering, we incorporated a total of 22 features, which include essential information 
        such as the transaction hash, sender address, and recipient address. Transactions are labeled as fraudulent 
        if either the sender address or recipient address, or both, have been reported as associated with a scam.
    """

    outlier_btc = """
        An Outlier is a rare chance of occurrence within a given data set and is an observation point that is distant from 
        other observations. Outliers act as signals that an anomalous transaction or activity might be taking place in the Blockchain network. 
        In this scenario, we used a combination of metrices such as Transaction Volume, Transaction Count etc as features for our 
        outlier detection models.
    """

    selected = ctx.triggered[0]["prop_id"].split(".")[0]
    
    if selected == "Bitcoin-2" or not ctx.triggered:
        card = []
        card.append(html.Div([html.P("Background of Anomaly Detection Framework", style={'font-weight':'bold', 'textDecoration':'underline', 'color': '#0a275c'}), html.P(ov_btc)]))
        card.append(html.Div([html.P("Address Detection", style={'font-weight':'bold', 'textDecoration':'underline', 'color': '#0a275c'}), html.P(address_btc)]))
        card.append(html.Div([html.P("Outlier Detection", style={'font-weight':'bold', 'textDecoration':'underline', 'color': '#0a275c'}), html.P(outlier_btc)]))
        return "Overview (Bitcoin)", card
    
    elif selected == "Ethereum-2":
        card = []
        card.append(html.Div([html.P("Background of Anomaly Detection Framework", style={'font-weight':'bold', 'textDecoration':'underline', 'color': '#0a275c'}), html.P(ov_eth)]))
        card.append(html.Div([html.P("Address Detection", style={'font-weight':'bold', 'textDecoration':'underline', 'color': '#0a275c'}), html.P(address_eth)]))
        return "Overview (Ethereum)", card
    
    return None, None