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

dash.register_page(__name__, path='/anomaly/models', name="Anomaly Detection Models")
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
df_isoForest = pd.read_sql('SELECT * FROM "isoForest_outliers"', psqlconn)
df_autoEncoder = pd.read_sql('SELECT * FROM "autoEncoder_outliers"', psqlconn)
df_kmeans = pd.read_sql('SELECT * FROM "kmeans_outliers"', psqlconn)
df_illicit = pd.read_sql('SELECT * FROM "anomaly_predictions"', psqlconn)
df_illicit_results = pd.read_sql('SELECT distinct * FROM "anomaly_results"', psqlconn)
df_kmeans["anomaly"] = df_kmeans["anomaly"].astype(str)
df_kmeans["cluster"] = df_kmeans["cluster"].astype(str)

df_eth_fraud = pd.read_sql('SELECT DISTINCT * FROM "anomaly_predictions_eth"', psqlconn)
df_eth_fraud_results = pd.read_sql('SELECT DISTINCT * FROM "anomaly_results_eth"', psqlconn)

df_illicit_cols = {'y_knn_pred': 'Illicit Account', 'y_dtc_pred': 'Illicit Account', 'y_xgb_pred': 'Illicit Account', 'account': 'Recipient Address', 'hash': 'Transaction Hash', 
                   'value': 'Value (BTC)', 'value_usd': 'Value (USD)', 'is_from_coinbase': 'Is From Coinbase', 'is_spendable': 'Is Spendable', 'spending_index': 'Spending Index', 
                   'spending_value_usd': 'Spending Value (USD)', 'lifespan': 'Lifespan', 'cdd': 'CDD', 'size': 'Size', 'weight': 'Weight', 'version': 'Version', 'lock_time': 'Lock Time', 
                   'is_coinbase': 'Is Coinbase', 'has_witness': 'Has Witness', 'input_count': 'Input Count', 'output_count': 'Output Count', 'input_total': 'Total Input (BTC)', 
                   'input_total_usd': 'Total Input (USD)', 'output_total': 'Total Output (BTC)', 'output_total_usd': 'Total Output (USD)', 'fee': 'Fee (BTC)', 'fee_usd': 'Fee (USD)', 
                   'fee_per_kb': 'Fee Per KB (BTC)', 'fee_per_kb_usd': 'Fee Per KB (USD)', 'fee_per_kwu': 'Fee Per KWU (BTC)', 'fee_per_kwu_usd': 'Fee Per KWU (USD)', 'cdd_total': 'CDD Total'}

df_illicit_cols_eth = {'y_logr_pred': 'Illicit Account', 'y_xgb_pred': 'Illicit Account', 'y_nn_pred': 'Illicit Account', 'y_rf_pred': 'Illicit Account',
                    'to_address': 'Recipient Address', 'from_address': 'Sender Address', 'hash': 'Transaction Hash', 'value': 'Value',
                    'transaction_index': 'Transaction Index', 'gas': 'Gas', 'gas_price': 'Gas Price', 'input': 'Input', 
                    'receipt_cumulative_gas_used': 'Recept Cumulative Gas Used', 'receipt_gas_used': 'Receipt Gas Used', 'block_number': 'Block Number',
                    'block_hash': 'Block Hash', 'year': 'Year', 'month': 'Month', 'day_of_the_month': 'Day of the Month', 'day_name': 'Day Name',
                    'hour': 'Hour', 'daypart': 'Daypart', 'weekend_flag': 'Is Weekend'
                    }

content = html.Div([
    dbc.Row([
            # Control panel column
            dbc.Col([
                
                # Select Cryptocurrency 
                dbc.Row([
                    html.P(" Select Cryptocurrency", className = 'bi bi-coin', style={'color':'black', 'text-align':'center', 'font-size':'15px', 'font-family':'Open Sans', 'font-weight':'bold'}),
                    dbc.DropdownMenu(
                        [dbc.DropdownMenuItem("Bitcoin (BTC)", id="Bitcoin-3"),
                        dbc.DropdownMenuItem(divider=True),
                        dbc.DropdownMenuItem("Ethereum (ETH)", id = "Ethereum-3", href='/anomaly/models/eth'),
                        dbc.DropdownMenuItem(divider=True),
                        html.Div([
                            html.Span("to be implemented in future", className='disabled-info'),
                            dbc.DropdownMenuItem("Tether (USDT)", id="Tether-3", disabled=True),
                        ], className='disabled-coin-2'),
                        ],
                        id = 'cryptocurrency-select-3',
                        label = 'Bitcoin (BTC)',
                        color = '#0d1e26',
                        align_end = True,
                        toggle_style = {'text-align':'center', 'font-size':'13px', 'width':'160px', 'height':'35px', 'color':'white', 'font-family': 'Open Sans'}
                    )
                ], style={'text-align':'center', 'padding-bottom':'15px'}),

                # Detection type
                dbc.Row(html.Div(id = 'menu'), justify = 'center', style = {'padding':'25px', 'border-top': '2px solid grey'}),

            ], width = 3, style = {'background-color':'#E8EBEE99',  'border-right':'2px solid grey', 'padding-top': '20px'}),

            # Table / Graph display segment
            dbc.Col([
                html.Div(
                    html.H4(id="anomaly-title", style = {'display':'inline-block', 'vertical-align':'center'}),
                    ),
                  
                html.Div(
                    dcc.Loading(
                        html.Div(id='anomaly-graphs'),
                        color='#0a275c',
                        style = {'position':'fixed', 'top': '50%'}
                    )

                , style= {'margin-top':'30px', 'width':'72vw', 'height':'70vh', 'overflow-y':'scroll'})

            ], width = 9, style = {'padding-right':'40px', 'padding-left':'30px', 'padding-top': '20px'})
    
    ], justify = 'evenly', style={'height': '100vh', 'border-top': '2px solid grey'})

], style = {'padding-bottom':'60px'})


def create_anomaly_models():
    layout = html.Div([
        nav,
        content,
        footer
    ], style={'min-height':'100%', 'position':'relative', 'overflow-x':'hidden'})
    return layout

################## Callbacks ##############################

# Update dropdown label
@app.callback(
    Output('cryptocurrency-select-3', "label"),
    [Input("Bitcoin-3", "n_clicks"), Input("Ethereum-3", "n_clicks"), Input("Tether-3", "n_clicks")]
)

def update_dropdown(n1, n2, n3):
    label_id = {"Bitcoin-3": "Bitcoin (BTC)", "Ethereum-3": "Ethereum (ETH)", "Tether-3": "Tether (USDT)"}
    if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:
        return "Bitcoin (BTC)"
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    return label_id[button_id]

# Update detection menu
@app.callback(
    Output('menu', 'children'),
    Input('cryptocurrency-select-3', 'label')
)

def update_menu(selected_cryptocurrency):
    print(selected_cryptocurrency)
    # if selected_cryptocurrency == 'Ethereum (ETH)':
    #     return [
    #                     dbc.Accordion(
    #                     dbc.AccordionItem(
    #                         dbc.ListGroup([
    #                             dbc.ListGroupItem("Logistic Regression", action=True, id={"type":'anomaly-logr-eth', "index": "myindex"}, color='#E8EBEE00', style={'cursor': 'pointer'}),
    #                             dbc.ListGroupItem("XGBoost", action=True, id={"type":'anomaly-xgb-eth', "index": "myindex"}, color='#E8EBEE00', style={'cursor': 'pointer'}),
    #                             dbc.ListGroupItem("Neural Networks", action=True, id={"type":'anomaly-nn-eth', "index": "myindex"}, color='#E8EBEE00', style={'cursor': 'pointer'}),
    #                             dbc.ListGroupItem("Random Forest", action=True, id={"type":'anomaly-rf-eth', "index": "myindex"}, color='#E8EBEE00', style={'cursor': 'pointer'})
    #                         ], flush=True, style={'font-size': '14px'}),
                            
    #                         title="Address Detection"
    #                     ), 
    #                     flush=True, start_collapsed=True, style = {'width':'300px', 'margin-top':'15px', 'margin-left': 'auto', 'margin-right': 'auto'}
    #                 )
    #             ]

    # else:
    if True:
        return [
                    dbc.Accordion(
                        dbc.AccordionItem(
                            dbc.ListGroup([
                                dbc.ListGroupItem("Decision Tree", action=True, id={'type': 'anomaly-dtc', 'index': 'myindex'}, color='#E8EBEE00', style={'cursor': 'pointer'}),
                                dbc.ListGroupItem("K-Nearest Neighbours", action=True, id={'type':'anomaly-knn', 'index': 'myindex'}, color='#E8EBEE00', style={'cursor': 'pointer'}),
                                dbc.ListGroupItem("XGBoost", action=True, id={'type':'anomaly-xgboost', 'index': 'myindex'}, color='#E8EBEE00', style={'cursor': 'pointer'})
                            ], flush=True, style={'font-size':'14px'}),
                            
                            title="Address Detection"
                        ), 
                        flush=True, start_collapsed=True, style = {'width':'300px', 'margin-top':'15px', 'margin-left': 'auto', 'margin-right': 'auto'}
                    ),
                    dbc.Accordion(
                        dbc.AccordionItem(
                            dbc.ListGroup([
                                dbc.ListGroupItem("Isolation Forest", action=True, id={'type':'outlier-isoForest', 'index': 'myindex'}, color='#E8EBEE00', style={'cursor': 'pointer'}),
                                dbc.ListGroupItem("Auto-Encoders", action=True, id={'type':'outlier-autoEncoder', 'index': 'myindex'}, color='#E8EBEE00', style={'cursor': 'pointer'}),
                                dbc.ListGroupItem("K-Means Clustering", action=True, id={'type':'outlier-kmeans', 'index': 'myindex'}, color='#E8EBEE00', style={'cursor': 'pointer'})
                            ], flush=True, style={'font-size':'14px'}),
                            
                            title="Outlier Detection"
                        ), 
                        flush=True, start_collapsed=True, style = {'width':'300px', 'margin-top':'15px', 'margin-left': 'auto', 'margin-right': 'auto'}
                    )
                ]
    

# Update graph display title
@app.callback(
    Output('anomaly-title', "children"),
    [Input({"type": "anomaly-dtc", "index": ALL}, "n_clicks"), Input({"type": "anomaly-knn", "index": ALL}, "n_clicks"), Input({"type": "anomaly-xgboost", "index": ALL}, "n_clicks"),
     Input({"type": "outlier-isoForest", "index": ALL}, "n_clicks"), Input({"type": "outlier-autoEncoder", "index": ALL}, "n_clicks"), Input({"type": "outlier-kmeans", "index": ALL}, "n_clicks"),
     Input({"type": "anomaly-logr-eth", "index": ALL}, "n_clicks"), Input({"type": "anomaly-xgb-eth", "index": ALL}, "n_clicks"), Input({"type": "anomaly-nn-eth", "index": ALL}, "n_clicks"), Input({"type": "anomaly-rf-eth", "index": ALL}, "n_clicks")
     ]
)

def update_title(n1,n2,n3,n4,n5,n6,n7,n8,n9,n10):
    titles_dict = {"anomaly-dtc": "Bitcoin Illicit Transactions Detected using Decision Tree", 
                "anomaly-knn": "Bitcoin Illicit Transactions Detected using K-Nearest Neighbours", 
                "anomaly-xgboost": "Bitcoin Illicit Transactions Detected using XGBoost",
                "outlier-isoForest": "Bitcoin Outliers Detected using Isolation Forest", 
                "outlier-autoEncoder": "Bitcoin Outliers Detected using Auto-Encoders", 
                "outlier-kmeans": " Bitcoin Outliers Detected using K-Means Clustering",
                "anomaly-logr-eth": "Ethereum Illicit Transactions Detected using Logistic Regression",
                "anomaly-xgb-eth": "Ethereum Illicit Transactions Detected using XGBoost",
                "anomaly-nn-eth": "Ethereum Illicit Transactions Detected using Neural Networks",
                "anomaly-rf-eth": "Ethereum Illicit Transactions Detected using Random Forest"}
    
    selected = ctx.triggered[0]["prop_id"].split(".")[0]

    if not selected:
        return titles_dict['anomaly-dtc']
    
    mytype = json.loads(selected)["type"]
    return titles_dict[mytype]
        

# Standalone method to reduce repeated chunks in callback below
def create_fig(df, model):
    graphs = []
    default = dict(rangeslider=dict(visible=True, bgcolor="#d0e0e5"),type="date")

    non_features = ['Date', 'index', 'anomaly', 'score']
    features = sorted(set(df.columns) - set(non_features))

    graphs.append(html.P("The following features were used to detect the outliers:"))

    f_list = []
    for c in features:
        f_list.append(c)
    graphs.append(dbc.ListGroup([dbc.ListGroupItem(c, style={'font-size':'13px', 'color':'#0a275c', 'font-weight':'bold', 'border-top':'None', 'border-bottom':'None', 'border-radius':'0px'}) for c in f_list], 
                  horizontal=True, style = {'padding-bottom': '10px'}))

    for c in features:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df['Date'], y=df[c], mode='lines', line = dict(color = "#0a275c"), name='normal value'))
        outlier = df.loc[df['anomaly'] == 1]
        fig.add_trace(go.Scatter(x=outlier['Date'], y=outlier[c], mode='markers', line = dict(color = "firebrick"), name='outlier'))
        fig.update_traces(hovertemplate='Date: %{x} <br>Value: %{y}')
        fig.update_xaxes(default) # adding in default range slider
        # setting date range limit to fix range slider bug with scatter plots
        fig.update_xaxes(range=[df['Date'].iloc[0], df['Date'].iloc[-1]],
                        rangeslider_range=[df['Date'].iloc[0], df['Date'].iloc[-1]])
        fig.update_xaxes(title_text = "Date")
        fig.update_yaxes(title_text = c)
        fig.update_layout(plot_bgcolor='white', title_text = '{} over Time'.format(c), title_x=0.5, title_font_color = 'black', title_font_size=16) 
        graphs.append(dcc.Graph(id ='{}-{}'.format(model,c), figure = fig))

    return graphs

def create_cluster(df, model):
    graphs = []

    non_features = ['Date', 'index', 'anomaly', 'cluster', 'Principal Component 1', 'Principal Component 2']
    features = sorted(set(df.columns) - set(non_features))

    graphs.append(html.P("The following features were used to detect the outliers:"))

    graphs.append(dbc.ListGroup([dbc.ListGroupItem(c, style={'font-size':'13px', 'color':'#0a275c', 'font-weight':'bold', 'border-top':'None', 'border-bottom':'None', 'border-radius':'0px'}) for c in features], 
                  horizontal=True, style = {'padding-bottom': '10px'}))

    # Visualising Clusters
    fig = px.scatter(df, x="Principal Component 1", y="Principal Component 2", color="cluster", color_discrete_sequence=px.colors.qualitative.Prism)
    fig.update_xaxes(title_text = "Principal Component 1")
    fig.update_yaxes(title_text = "Principal Component 2")
    fig.update_layout(plot_bgcolor='white', title_text = "Clusters detected using K-Means", title_x=0.5, title_font_color = 'black', title_font_size=16) 
    graphs.append(dcc.Graph(id ='{}-{}'.format(model, "cluster"), figure = fig))

    # Visualising Anomalies
    fig = px.scatter(df, x="Principal Component 1", y="Principal Component 2", color="anomaly", color_discrete_sequence=["firebrick", "#0a275c"])
    fig.update_xaxes(title_text = "Principal Component 1")
    fig.update_yaxes(title_text = "Principal Component 2")
    fig.update_layout(plot_bgcolor='white', title_text = "Outlier Detection using K-Means: Red represents Anomaly", title_x=0.5, title_font_color = 'black', title_font_size=16) 
    graphs.append(dcc.Graph(id ='{}-{}'.format(model, "anomaly"), figure = fig))
    
    return graphs

def create_table(df, model):
    tables = []
    model_name = str(model.split("_")[1])
    db_column = f'y_{model_name}_pred'
    
    if model.split("_")[-1] != "eth":
        tables.append(html.P("Model Performance:", style = {'font-weight': 'bold'}))
        tables.append(dash_table.DataTable(
            columns = [
                {'name': 'Accuracy', 'id': 'test_acc', 'type':'text'},
                {'name': 'Precision', 'id': 'test_precision', 'type':'text'},
                {'name': 'Recall', 'id': 'test_recall', 'type':'text'},
                {'name': 'F1 Score', 'id': 'test_f1score', 'type':'text'}
            ],
            data = df_illicit_results.loc[df_illicit_results['class'] == model_name].to_dict('records'),
            style_header = {'font-size':'16px', 'color': 'black', 'text-transform': 'none'},
            style_cell = {'font-family':'Trebuchet MS', 'font-size':'15px', 'textAlign': 'left', 
                        'color': '#0a275c', 'padding': '5px 10px 5px 10px'},
            id = 'anomaly-performance-table'
        ))

        df_illicit_cols = {model: 'Illicit Account', 'account': 'Recipient Address', 'hash': 'Transaction Hash', 'value': 'Value (BTC)', 'value_usd': 'Value (USD)', 
                    'is_from_coinbase': 'Is From Coinbase', 'is_spendable': 'Is Spendable', 'spending_index': 'Spending Index', 'spending_value_usd': 'Spending Value (USD)', 
                    'lifespan': 'Lifespan', 'cdd': 'CDD', 'size': 'Size', 'weight': 'Weight', 'version': 'Version', 'lock_time': 'Lock Time', 'is_coinbase': 'Is Coinbase', 
                    'has_witness': 'Has Witness', 'input_count': 'Input Count', 'output_count': 'Output Count', 'input_total': 'Total Input (BTC)', 'input_total_usd': 'Total Input (USD)', 
                    'output_total': 'Total Output (BTC)', 'output_total_usd': 'Total Output (USD)', 'fee': 'Fee (BTC)', 'fee_usd': 'Fee (USD)', 
                    'fee_per_kb': 'Fee Per KB (BTC)', 'fee_per_kb_usd': 'Fee Per KB (USD)', 'fee_per_kwu': 'Fee Per KWU (BTC)', 'fee_per_kwu_usd': 'Fee Per KWU (USD)', 'cdd_total': 'CDD Total'
                    }


        tables.append(html.P("Accounts detected to have illicit transactions:", style = {'padding-top':'20px', 'font-weight':'bold'}))
        tables.append(dbc.Row([
            dbc.Col([
                dcc.Dropdown(value=10, clearable=False, options=[10, 25, 50, 100], id='row-drop')
            ], width = 2, style={'padding-bottom':'15px'}),
            dbc.Col([
                html.Div(
                    dcc.Dropdown(options=[{'label': y, 'value': x} for x, y in df_illicit_cols.items()],
                                value=[model, 'account', 'hash', 'value', 'value_usd'],
                                multi = True,
                                placeholder = "Select table fields...",
                                id = 'table-fields')
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
            id = 'anomaly-table'
        ))

    else:
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
            id = 'anomaly-performance-table'
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
                                id = 'table-fields')
                )
            ], width = 6, style={'padding-bottom':'15px'})
        ], justify = 'start'))
        
        tables.append(dash_table.DataTable(
            columns = [],
            data = df.to_dict('records'),
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
            id = 'anomaly-table'
        ))

    return tables

@app.callback(
    Output("anomaly-graphs", "children"),
     [Input({"type": "anomaly-dtc", "index": ALL}, "n_clicks"), Input({"type": "anomaly-knn", "index": ALL}, "n_clicks"), Input({"type": "anomaly-xgboost", "index": ALL}, "n_clicks"),
     Input({"type": "outlier-isoForest", "index": ALL}, "n_clicks"), Input({"type": "outlier-autoEncoder", "index": ALL}, "n_clicks"), Input({"type": "outlier-kmeans", "index": ALL}, "n_clicks"),
     Input({"type": "anomaly-logr-eth", "index": ALL}, "n_clicks"), Input({"type": "anomaly-xgb-eth", "index": ALL}, "n_clicks"), Input({"type": "anomaly-nn-eth", "index": ALL}, "n_clicks"), Input({"type": "anomaly-rf-eth", "index": ALL}, "n_clicks")
     ],
    Input('anomaly-title', 'children')
)

def update_line_chart(dtc, knn, xgboost, iso, autoEncoder, kmeans, logr_eth, xgb_eth, nn_eth, rf_eth, curr_title):
    graphs = []

    if ctx.triggered[0]["prop_id"].split(".")[0] != 'anomaly-title':
        selected = ctx.triggered[0]["prop_id"].split(".")[0]
        mytype = json.loads(selected)["type"]

        if mytype == 'anomaly-dtc':
            graphs = create_table(df_illicit, 'y_dtc_pred')
        
        elif mytype == 'anomaly-knn':
            graphs = create_table(df_illicit, 'y_knn_pred')
        
        elif mytype == 'anomaly-xgboost':
            graphs = create_table(df_illicit, 'y_xgb_pred')
        
        elif mytype == 'outlier-isoForest':
            graphs = create_fig(df_isoForest, 'isoForest')
        
        elif mytype == 'outlier-autoEncoder':
            graphs = create_fig(df_autoEncoder, 'autoEncoder')
        
        elif mytype == 'outlier-kmeans':
            graphs = create_cluster(df_kmeans, 'kmeans')       

        elif mytype == 'anomaly-logr-eth':
            graphs = create_table(df_eth_fraud, 'y_logr_pred_eth')

        elif mytype == 'anomaly-xgb-eth':
            graphs = create_table(df_eth_fraud, 'y_xgb_pred_eth')
        
        elif mytype == 'anomaly-nn-eth':
            graphs = create_table(df_eth_fraud, 'y_nn_pred_eth')

        elif mytype == 'anomaly-rf-eth':
            graphs = create_table(df_eth_fraud, 'y_rf_pred_eth')

    return graphs
    

# Updating page size of address detection's table 
@app.callback(
    Output("anomaly-table", "page_size"),
    Input("row-drop", "value"),
)

def update_row_dropdown(row_v):
    return row_v

# Updating columns shown in table based on dropdown 
@app.callback(
    Output('anomaly-table', 'columns'),
    [Input('table-fields', 'value')],
    Input('anomaly-title', 'children'),
    [State('anomaly-table', 'columns')]
)

def update_cols_displayed(value, model, columns):
    columns = []

    try:
        currency = model.split(" ")[0]
        ml = model.split("using ")[1]

        if currency == "Bitcoin":
            if ml == 'Decision Tree':
                model = 'y_dtc_pred'
        
            elif ml == 'K-Nearest Neighbours':
                model = 'y_knn_pred'
        
            elif ml == 'XGBoost':
                model = 'y_xgb_pred'

            if not value:
                value=[model, 'account', 'hash', 'value', 'value_usd']
            for feature in value:
                columns.append({
                    'name': df_illicit_cols[feature],
                    'id': feature
                })
            return columns

        elif currency == "Ethereum":
            if ml == 'Logistic Regression':
                model = 'y_logr_pred_eth'

            elif ml == 'XGBoost':
                model = 'y_xgb_pred_eth'

            elif ml == 'Neural Networks':
                model == 'y_nn_pred_eth'

            elif ml == 'Random Forest':
                model == 'y_rf_pred_eth'
    
            if not value:
                value=[model, 'to_address', 'from_address', 'hash', 'value']
            for feature in value:
                columns.append({
                    'name': df_illicit_cols_eth[feature],
                    'id': feature
                })
            return columns
        
    except:
        print("This is not address detection!")