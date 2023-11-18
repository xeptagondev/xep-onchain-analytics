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

dash.register_page(__name__, path='/anomaly/models/eth', name="ETH Anomaly Detection Models")
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

# Dataframe for output of Anomaly Detection
df_eth_fraud = pd.read_sql('SELECT DISTINCT * FROM "anomaly_predictions_eth"', psqlconn)
df_eth_fraud_results = pd.read_sql('SELECT DISTINCT * FROM "anomaly_results_eth"', psqlconn)

df_illicit_cols_eth = {'y_logr_pred': 'Illicit Account', 'y_xgb_pred': 'Illicit Account', 'y_rf_pred': 'Illicit Account', 'y_ensemble_pred': 'Illicit Account',
                    'to_address': 'Recipient Address', 'from_address': 'Sender Address', 'hash': 'Transaction Hash', 'value': 'Value',
                    'transaction_index': 'Transaction Index', 'gas': 'Gas', 'gas_price': 'Gas Price', 'input': 'Input', 
                    'receipt_cumulative_gas_used': 'Recept Cumulative Gas Used', 'receipt_gas_used': 'Receipt Gas Used', 'block_number': 'Block Number',
                    'block_hash': 'Block Hash', 'year': 'Year', 'month': 'Month', 'day_of_the_month': 'Day of the Month', 'day_name': 'Day Name',
                    'hour': 'Hour', 'daypart': 'Daypart', 'weekend_flag': 'Is Weekend'
                    }

# Convert metric values to xx.x%
metrics = ['test_acc', 'test_precision', 'test_recall', 'test_f1score']
for metric in metrics:
    df_eth_fraud_results[metric] = pd.to_numeric(df_eth_fraud_results[metric])
    df_eth_fraud_results[metric] = pd.Series(["{0:.1f}%".format(val * 100) for val in df_eth_fraud_results[metric]], index = df_eth_fraud_results.index)
    

content = html.Div([
    dbc.Row([
            # Control panel column
            dbc.Col([
                
                # Select Cryptocurrency 
                dbc.Row([
                    html.P(" Select Cryptocurrency", className = 'bi bi-coin', style={'color':'black', 'text-align':'center', 'font-size':'15px', 'font-family':'Open Sans', 'font-weight':'bold'}),
                    dbc.DropdownMenu(
                        [dbc.DropdownMenuItem("Bitcoin (BTC)", id="Bitcoin-4", href='/anomaly/models'),
                        dbc.DropdownMenuItem(divider=True),
                        dbc.DropdownMenuItem("Ethereum (ETH)", id = "Ethereum-4"),
                        dbc.DropdownMenuItem(divider=True),
                        html.Div([
                            html.Span("to be implemented in future", className='disabled-info'),
                            dbc.DropdownMenuItem("Tether (USDT)", id="Tether-4", disabled=True),
                        ], className='disabled-coin-2'),
                        ],
                        id = 'cryptocurrency-select-4',
                        label = 'Ethereum (ETH)',
                        color = '#0d1e26',
                        align_end = True,
                        toggle_style = {'text-align':'center', 'font-size':'13px', 'width':'160px', 'height':'35px', 'color':'white', 'font-family': 'Open Sans'}
                    )
                ], style={'text-align':'center', 'padding-bottom':'15px'}),

                # Detection type
                dbc.Row(html.Div(id = 'menu-eth'), justify = 'center', style = {'padding':'25px', 'border-top': '2px solid grey'}),

            ], width = 3, style = {'background-color':'#E8EBEE99',  'border-right':'2px solid grey', 'padding-top': '20px'}),

            # Table / Graph display segment
            dbc.Col([
                html.Div(
                    html.H4(id='anomaly-title-eth', style = {'display':'inline-block', 'vertical-align':'center'}),
                    ),
                  
                html.Div(
                    dcc.Loading(
                        html.Div(id='anomaly-graphs-eth'),
                        color='#0a275c',
                        style = {'position':'fixed', 'top': '50%'}
                    )

                , style= {'margin-top':'30px', 'width':'72vw', 'height':'70vh', 'overflow-y':'scroll'})

            ], width = 9, style = {'padding-right':'40px', 'padding-left':'30px', 'padding-top': '20px'})
    
    ], justify = 'evenly', style={'height': '100vh', 'border-top': '2px solid grey'})

], style = {'padding-bottom':'60px'})


def create_anomaly_models_eth():
    '''Returns the layout of the page comprising of navigation bar, content and footer sections.'''
    layout = html.Div([
        nav,
        content,
        footer
    ], style={'min-height':'100%', 'position':'relative', 'overflow-x':'hidden'})
    return layout

############################## Callbacks ##############################

# Update dropdown label
@app.callback(
    Output('cryptocurrency-select-4', 'label-eth'),
    [Input("Bitcoin-4", "n_clicks"), Input("Ethereum-4", "n_clicks"), Input("Tether-4", "n_clicks")]
)

def update_dropdown(n1, n2, n3):
    '''Returns the updated dropdown button label.'''
    label_id = {"Bitcoin-4": "Bitcoin (BTC)", "Ethereum-4": "Ethereum (ETH)", "Tether-4": "Tether (USDT)"}
    if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:
        return "Ethereum (ETH)"
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    return label_id[button_id]

# Update detection menu
@app.callback(
    Output('menu-eth', 'children'),
    Input('cryptocurrency-select-4', 'label')
)

def update_menu(selected_cryptocurrency):
    '''Returns the updated fraud detection model menu based on the selected cryptocurrency.'''
    if selected_cryptocurrency == 'Ethereum (ETH)':
        return [
                    dbc.Accordion(
                    dbc.AccordionItem(
                        dbc.ListGroup([
                            dbc.ListGroupItem("Logistic Regression", action=True, id={"type":'anomaly-logr-eth', "index": "myindex"}, color='#E8EBEE00', style={'cursor': 'pointer'}),
                            dbc.ListGroupItem("XGBoost", action=True, id={"type":'anomaly-xgb-eth', "index": "myindex"}, color='#E8EBEE00', style={'cursor': 'pointer'}),
                            dbc.ListGroupItem("Random Forest", action=True, id={"type":'anomaly-rf-eth', "index": "myindex"}, color='#E8EBEE00', style={'cursor': 'pointer'}),
                            dbc.ListGroupItem("Ensemble Learning", action=True, id={"type":'anomaly-ensemble-eth', "index": "myindex"}, color='#E8EBEE00', style={'cursor': 'pointer'})
                        ], flush=True, style={'font-size': '14px'}),
                        
                        title="ETH Address Detection"
                    ), 
                    flush=True, start_collapsed=True, style = {'width':'300px', 'margin-top':'15px', 'margin-left': 'auto', 'margin-right': 'auto'}
                )
                ]

# Update graph display title
@app.callback(
    Output('anomaly-title-eth', "children"),
    [Input({"type": "anomaly-logr-eth", "index": ALL}, "n_clicks"), Input({"type": "anomaly-xgb-eth", "index": ALL}, "n_clicks"), Input({"type": "anomaly-rf-eth", "index": ALL}, "n_clicks"), Input({"type": "anomaly-ensemble-eth", "index": ALL}, "n_clicks")]
)

def update_title(n1,n2,n3,n4):
    '''Returns the updated title based on the selected cryptocurrency.'''
    titles_dict = {
                "anomaly-logr-eth": "Ethereum Illicit Transactions Detected using Logistic Regression",
                "anomaly-xgb-eth": "Ethereum Illicit Transactions Detected using XGBoost",
                "anomaly-rf-eth": "Ethereum Illicit Transactions Detected using Random Forest",
                "anomaly-ensemble-eth": "Ethereum Illicit Transactions Detected using Ensemble Learning"
                }
    
    selected = ctx.triggered[0]["prop_id"].split(".")[0]

    if not selected:
        return titles_dict['anomaly-logr-eth']
    
    mytype = json.loads(selected)["type"]
    return titles_dict[mytype]
        

def create_table_eth(df, model):
    '''Returns a list of two tables: a table representing model performance metrics and a table of accounts detected to have illicit transactions'''
    tables = []
    model_name = str(model.split("_")[1])
    db_column = f'y_{model_name}_pred'

    tables.append(html.P("Model Performance:", style = {'font-weight': 'bold'}))
    tables.append(dash_table.DataTable(
        columns = [
            {'name': 'Accuracy', 'id': 'test_acc', 'type':'text'},
            {'name': 'Precision', 'id': 'test_precision', 'type':'text'},
            {'name': 'Recall', 'id': 'test_recall', 'type':'text'},
            {'name': 'F1 Score', 'id': 'test_f1score', 'type':'text'}
        ],
        data = df_eth_fraud_results.loc[df_eth_fraud_results['class'] == model_name].to_dict('records'),
        style_header = {'font-size':'16px', 'color': 'black', 'text-transform': 'none'},
        style_cell = {'font-family':'Trebuchet MS', 'font-size':'15px', 'textAlign': 'left', 
                    'color': '#0a275c', 'padding': '5px 10px 5px 10px'},
        id = 'anomaly-performance-table-2'
    ))

    model = model.split("_eth")[0]
    df_illicit_cols_eth = {model: 'Illicit Account',
            'to_address': 'Recipient Address', 'from_address': 'Sender Address', 'hash': 'Transaction Hash', 'value': 'Value',
            'transaction_index': 'Transaction Index', 'gas': 'Gas', 'gas_price': 'Gas Price', 'input': 'Input', 
            'receipt_cumulative_gas_used': 'Recept Cumulative Gas Used', 'receipt_gas_used': 'Receipt Gas Used', 'block_number': 'Block Number',
            'block_hash': 'Block Hash', 'year': 'Year', 'month': 'Month', 'day_of_the_month': 'Day of the Month', 'day_name': 'Day Name',
            'hour': 'Hour', 'daypart': 'Daypart', 'weekend_flag': 'Is Weekend'
            }

    tables.append(html.P("Accounts detected to have illicit transactions:", style = {'padding-top':'20px', 'font-weight':'bold'}))
    tables.append(dbc.Row([
        dbc.Col([
            dcc.Dropdown(value=10, clearable=False, options=[10, 25, 50, 100], id='row-drop')
        ], width = 2, style={'padding-bottom':'15px'}),
        dbc.Col([
            html.Div(
                dcc.Dropdown(options=[{'label': y, 'value': x} for x, y in df_illicit_cols_eth.items()],
                            value=[model, 'to_address', 'from_address', 'hash', 'value'],
                            multi = True,
                            placeholder = "Select table fields...",
                            id = 'table-fields-eth')
            )
        ], width = 6, style={'padding-bottom':'15px'})
    ], justify = 'start'))
    
    tables.append(dash_table.DataTable(
        columns = [],
        # data = df.to_dict('records'),
        data = df[df[db_column] == 1].to_dict('records'), # edited by ETH group 11/11/23
        style_as_list_view = True,
        page_size = 10,
        style_header = {'font-size':'16px', 'color': 'black', 'backgroundColor': '#dee9ed', 'text-transform': 'none'},
        style_cell = {'font-family':'Trebuchet MS', 'font-size':'15px', 'textAlign': 'center',
                    'color': '#0a275c', 'padding': '12px 15px 12px 15px'},
        style_table = {'overflowX': 'auto'},
        style_data = {'overflow': 'hidden', 'textOverflow': 'ellipsis', 
                    'minWidth': '180px', 'width': '180px', 'maxWidth': '180px'},
        style_data_conditional=[{
            'if': {'row_index': 'odd'},
            'backgroundColor': 'rgb(220, 220, 220, 0.5)',
        }],
        id = 'anomaly-table-eth'
    ))

    return tables

@app.callback(
    Output("anomaly-graphs-eth", "children"),
     [Input({"type": "anomaly-logr-eth", "index": ALL}, "n_clicks"), Input({"type": "anomaly-xgb-eth", "index": ALL}, "n_clicks"), Input({"type": "anomaly-rf-eth", "index": ALL}, "n_clicks"), Input({"type": "anomaly-ensemble-eth", "index": ALL}, "n_clicks")],
    Input('anomaly-title-eth', 'children')
)

def update_line_chart_eth(logr_eth, xgb_eth, rf_eth, ensemble_eth, curr_title):
    '''Update the content of the "anomaly-graphs-eth" component based on the selected model type.'''
    graphs = []

    if ctx.triggered[0]["prop_id"].split(".")[0] != 'anomaly-title-eth':

        selected = ctx.triggered[0]["prop_id"].split(".")[0]
        mytype = json.loads(selected)["type"]      

        if mytype == 'anomaly-logr-eth':
            graphs = create_table_eth(df_eth_fraud, 'y_logr_pred_eth')

        elif mytype == 'anomaly-xgb-eth':
            graphs = create_table_eth(df_eth_fraud, 'y_xgb_pred_eth')
        
        elif mytype == 'anomaly-rf-eth':
            graphs = create_table_eth(df_eth_fraud, 'y_rf_pred_eth')

        elif mytype == 'anomaly-ensemble-eth':
            graphs = create_table_eth(df_eth_fraud, 'y_ensemble_pred_eth')

        return graphs

    else:
        return create_table_eth(df_eth_fraud, 'y_logr_pred_eth')

# Updating page size of address detection's table 
@app.callback(
    Output("anomaly-table-eth", "page_size"),
    Input("row-drop", "value"),
)

def update_row_dropdown(row_v):
    '''Updates the number of rows displayed in the Ethereum anomaly detection table based on the selected value.'''
    return row_v

# Updating columns shown in table based on dropdown 
@app.callback(
    Output('anomaly-table-eth', 'columns'),
    [Input('table-fields-eth', 'value')],
    Input('anomaly-title-eth', 'children'),
    [State('anomaly-table-eth', 'columns')]
)

def update_cols_displayed_eth(value, model, columns):
    '''Updates the columns displayed in the Ethereum anomaly detection table based on the selected fields.'''
    columns = []

    try:
        ml = model.split("using ")[1]

        if ml == 'Logistic Regression':
            model = 'y_logr_pred'

        elif ml == 'XGBoost':
            model = 'y_xgb_pred'

        elif ml == 'Neural Networks':
            model == 'y_nn_pred'

        elif ml == 'Random Forest':
            model == 'y_rf_pred'

        if not value:
            value = [model, 'to_address', 'from_address', 'hash', 'value']

        for feature in value:
            columns.append({
                'name': df_illicit_cols_eth[feature],
                'id': feature
            })

        return columns

    except:
        print("This is not address detection!")