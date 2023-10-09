# import dash
# from dash import html, dcc, ALL, ctx
# from dash.dependencies import Input, Output, State
# import dash_bootstrap_components as dbc
# from app import app
# from navbar import create_navbar
# from footer import create_footer
# import pandas as pd
# import plotly.express as px
# import plotly.graph_objects as go
# from datetime import date, datetime
# import re
# import duckdb as ddb
# import psycopg2
# import json

# dash.register_page(__name__, path='/analytics/ethereum', name="Analytics Ethereum")
# nav = create_navbar()
# footer = create_footer()

# content = html.Div([
#     dbc.Row([

#         dbc.Col([

#                 # Select Cryptocurrency 
#                 dbc.Row([
#                     html.P(" Select Cryptocurrency", className = 'bi bi-coin', style={'color':'black', 'text-align':'center', 'font-size':'15px', 'font-family':'Open Sans', 'font-weight':'bold'}),
#                     dbc.DropdownMenu(
#                         [dbc.DropdownMenuItem("Bitcoin (BTC)", id="Bitcoin", href="/analytics"),
#                         dbc.DropdownMenuItem(divider=True),
#                         dbc.DropdownMenuItem("Ethereum (ETH)", id="Ethereum"),
#                         dbc.DropdownMenuItem(divider=True),
#                         html.Div([
#                             html.Span("to be implemented in future", className='disabled-info'),
#                             dbc.DropdownMenuItem("Tether (USDT)", id="Tether", disabled=True),
#                         ], className='disabled-coin-2'),
#                         ],
#                         id = 'cryptocurrency-select',
#                         label = 'Ethereum (ETH)',
#                         color = '#0d1e26',
#                         toggle_style = {'text-align':'center', 'font-size':'13px', 'width':'160px', 'height':'35px', 'color':'white', 'font-family': 'Open Sans'}
#                     )
#                 ], style={'text-align':'center', 'padding-bottom':'15px'}),
                
#         ], width = 3, style = {'background-color':'#E8EBEE99',  'border-right':'2px solid grey', 'padding-top': '20px'})
#     ])
# ], style={'height': '200px'})


# def create_charts_ethereum():
#     layout = html.Div([
#         nav,
#         content,
#         footer
#     ], style={'min-height':'100%', 'position':'relative', 'overflow-x':'hidden'})
#     return layout


################## Callbacks ##############################

# Update dropdown label 
# @app.callback(
#     Output('cryptocurrency-select', "label"),
#     [Input("Bitcoin", "n_clicks"), Input("Ethereum", "n_clicks"), Input("Tether", "n_clicks")]
# )

# def update_dropdown(n1, n2, n3):
#     label_id = {"Bitcoin": "Bitcoin (BTC)", "Ethereum": "Ethereum (ETH)", "Tether": "Tether (USDT)"}
#     if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:
#         return "Bitcoin (BTC)"
#     button_id = ctx.triggered[0]["prop_id"].split(".")[0]
#     return label_id[button_id]