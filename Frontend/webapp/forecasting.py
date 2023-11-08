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

dash.register_page(__name__, path='/forecasting', name="Forecasting")
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
                        [dbc.DropdownMenuItem("Ethereum (ETH)", id="Ethereum-4"),
                        dbc.DropdownMenuItem(divider=True),
                        html.Div([
                            html.Span("to be implemented in future", className='disabled-info'),
                            dbc.DropdownMenuItem("Bitcoin (BTC)", id="Bitcoin-4", disabled=True),
                        ], className='disabled-coin'),
                        dbc.DropdownMenuItem(divider=True),
                        html.Div([
                            html.Span("to be implemented in future", className='disabled-info'),
                            dbc.DropdownMenuItem("Tether (USDT)", id="Tether-4", disabled=True),
                        ], className='disabled-coin-2'),
                        ],
                        id = 'cryptocurrency-select-4',
                        label = 'Ethereum (ETH)',
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
                        #[dbc.ListGroupItem(x, action=True, id={"type": "list-group-item", "index": x}, color = '#E8EBEE00', style = {'font-size': '13px'}) for x in sorted(metrics_desc['metric_name'].tolist())], 
                        id='list-group', 
                        flush = True,
                        style={'margin-top':'15px', 'overflow-y':'scroll', 'width':'350px', 'height': '450px'})
                    
                ], justify = 'center', style = {'padding':'25px', 'border-top': '2px solid grey'}),

            ], width = 3, style = {'background-color':'#E8EBEE99',  'border-right':'2px solid grey', 'padding-top': '20px'}),

            # Display column
            dbc.Col([
                html.H5(id='graph-title', style = {'display': 'inline-block', 'vertical-align': 'middle', 'margin': '10px 0', 'color':'#0a275c'}),

                # area for metric description
                html.Div([
                    html.P("Metric Description", style={'font-weight':'bold', 'textDecoration':'underline'}),
                    #html.P(id='metric-desc', style={'width':'48vw'}),
                ]),

                dbc.Row([
                html.Div(id="scale-dropdown", children=[
                    dbc.DropdownMenu(
                        [dbc.DropdownMenuItem(
                            html.Div([
                            html.Div(['Price'], style={'textAlign': 'center', 'font-weight':'bold'}),
                            dmc.SegmentedControl(
                                    id="yaxis-type",
                                    value="Log",
                                    radius=10,
                                    data=[
                                        {"value": "Log", "label": "Log"},
                                        {"value": "Linear", "label": "Linear"},
                                    ],
                                    color = "teal"
                            )], style = {'display':'flex', 'flex-direction':'column', 'align-items': 'center', 'justify-content': 'center'}), toggle=False),
                         dbc.DropdownMenuItem(divider=True),
                         dbc.DropdownMenuItem(
                            html.Div([
                            html.Div(['Metric'], style={'textAlign': 'center', 'font-weight':'bold'}),
                            dmc.SegmentedControl(
                                    id="yaxis-type-2",
                                    value="Linear",
                                    radius=10,
                                    data=[
                                        {"value": "Log", "label": "Log"},
                                        {"value": "Linear", "label": "Linear"},
                                    ],
                                    color = "teal"
                            )], style = {'display':'flex', 'flex-direction':'column', 'align-items': 'center', 'justify-content': 'center'}), toggle=False)],
                        id = 'test-drop',
                        label = 'Select Scale',
                        color = 'white',
                        toggle_style = {'text-align':'center', 'font-size':'13px', 'height':'35px', 'color':'#bcc4cb', 'font-family': 'Open Sans','border-color':'#bcc4cb'}
                    )
                ], style = {'display':'block', 'float': 'right', 'width':'100px'}), ], style={'justify-content': 'right', 'padding-bottom':'5px', 'padding-right':'45px'}),

                # area to display selected metric's graph
                dcc.Loading(
                    #dcc.Graph(id="analytics-graph", style={'height': '80vh'}),
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
    Output('cryptocurrency-select-4', "label"),
    [Input("Bitcoin-4", "n_clicks"), Input("Ethereum-4", "n_clicks"), Input("Tether-4", "n_clicks")]
)

def update_dropdown(n1, n2, n3):
    label_id = {"Bitcoin-4": "Bitcoin (BTC)", "Ethereum-4": "Ethereum (ETH)", "Tether-4": "Tether (USDT)"}
    if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:
        return "Ethereum (ETH)"
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    return label_id[button_id]

