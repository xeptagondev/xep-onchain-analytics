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

import psycopg2
from sqlalchemy import create_engine
import json

dash.register_page(__name__, path='/anomaly', name="Anomaly Detection")
nav = create_navbar()
footer = create_footer()

# Database configurations
with open("/home/ec2-user/etl/extract/config.json") as config_file:
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
df_illicit_results = pd.read_sql('SELECT * FROM "anomaly_results"', psqlconn)
df_kmeans["anomaly"] = df_kmeans["anomaly"].astype(str)
df_kmeans["cluster"] = df_kmeans["cluster"].astype(str)

df_illicit_cols = {'y_knn_pred': 'Illicit Account', 'y_dtc_pred': 'Illicit Account', 'y_xgb_pred': 'Illicit Account', 'account': 'Recipient Address', 'hash': 'Transaction Hash', 
                   'value': 'Value (BTC)', 'value_usd': 'Value (USD)', 'is_from_coinbase': 'Is From Coinbase', 'is_spendable': 'Is Spendable', 'spending_index': 'Spending Index', 
                   'spending_value_usd': 'Spending Value (USD)', 'lifespan': 'Lifespan', 'cdd': 'CDD', 'size': 'Size', 'weight': 'Weight', 'version': 'Version', 'lock_time': 'Lock Time', 
                   'is_coinbase': 'Is Coinbase', 'has_witness': 'Has Witness', 'input_count': 'Input Count', 'output_count': 'Output Count', 'input_total': 'Total Input (BTC)', 
                   'input_total_usd': 'Total Input (USD)', 'output_total': 'Total Output (BTC)', 'output_total_usd': 'Total Output (USD)', 'fee': 'Fee (BTC)', 'fee_usd': 'Fee (USD)', 
                   'fee_per_kb': 'Fee Per KB (BTC)', 'fee_per_kb_usd': 'Fee Per KB (USD)', 'fee_per_kwu': 'Fee Per KWU (BTC)', 'fee_per_kwu_usd': 'Fee Per KWU (USD)', 'cdd_total': 'CDD Total'}


content = html.Div([
    dbc.Row([
            # Control panel column
            dbc.Col([
                
                # Select Cryptocurrency 
                dbc.Row([
                    html.P(" Select Cryptocurrency", className = 'bi bi-coin', style={'color':'black', 'text-align':'center', 'font-size':'15px', 'font-family':'Open Sans', 'font-weight':'bold'}),
                    dbc.DropdownMenu(
                        [dbc.DropdownMenuItem("Bitcoin (BTC)", id="Bitcoin-2"),
                        dbc.DropdownMenuItem(divider=True),
                        html.Div([
                            html.Span("to be implemented in future", className='disabled-info'),
                            dbc.DropdownMenuItem("Ethereum (ETH)", id="Ethereum-2", disabled=True),
                        ], className='disabled-coin'),
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
                ], style={'text-align':'center', 'padding-bottom':'15px'}),

                # Detection type
                dbc.Row([
                    dbc.Accordion(
                        dbc.AccordionItem(
                            dbc.ListGroup([
                                dbc.ListGroupItem("Decision Tree", action=True, id='anomaly-dtc', color='#E8EBEE00'),
                                dbc.ListGroupItem("K-Nearest Neighbours", action=True, id='anomaly-knn', color='#E8EBEE00'),
                                dbc.ListGroupItem("XGBoost", action=True, id='anomaly-xgboost', color='#E8EBEE00')
                            ], flush=True, style={'font-size':'14px'}),
                            
                            title="Address Detection"
                        ), 
                        flush=True, start_collapsed=True, style = {'width':'300px', 'margin-top':'15px'}
                    ),
                    dbc.Accordion(
                        dbc.AccordionItem(
                            dbc.ListGroup([
                                dbc.ListGroupItem("Isolation Forest", action=True, id='outlier-isoForest', color='#E8EBEE00'),
                                dbc.ListGroupItem("Auto-Encoders", action=True, id='outlier-autoEncoder', color='#E8EBEE00'),
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
                html.Div([
                    html.H4(id="anomaly-title", style = {'display':'inline-block', 'vertical-align':'center'}),
                    ]),
                  
                html.Div([
                    dcc.Loading([
                        html.Div(id='anomaly-description'),
                        html.Div(id='anomaly-graphs')],
                        color='#0a275c',
                        style = {'position':'fixed', 'top': '50%'}
                    )

                ], style= {'margin-top':'30px', 'width':'72vw', 'height':'70vh', 'overflow-y':'scroll'})

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
    Output('anomaly-description', "children"),
    [Input("anomaly-dtc", "n_clicks"), Input("anomaly-knn", "n_clicks"), Input("anomaly-xgboost", "n_clicks"), 
    Input("outlier-isoForest", "n_clicks"), Input("outlier-autoEncoder", "n_clicks"), Input("outlier-kmeans", "n_clicks")]
)

def update_title(n1,n2,n3,n4,n5,n6):
    titles_dict = {"anomaly-dtc": "Illicit Transactions Detected using Decision Tree", 
                   "anomaly-knn": "Illicit Transactions Detected using K-Nearest Neighbours", 
                   "anomaly-xgboost": "Illicit Transactions Detected using XGBoost",
                   "outlier-isoForest": "Outliers Detected using Isolation Forest", 
                   "outlier-autoEncoder": "Outliers Detected using Auto-Encoders", 
                   "outlier-kmeans": "Outliers Detected using K-Means Clustering"}
    if not ctx.triggered: # Default
        ov = """
            Due to the nature of a Blockchain network, cryptocurrencies are becoming the preferred option 
            for cybercriminal activities. Criminals are turning towards cryptocurrency as it is anonymous, 
            quick, requires no former relationship between parties, and can facilitate seamless international trades. 
            With the number of cybercriminal activities on the rise, members of the network would want to detect 
            these criminals as soon as possible to prevent them from harming the network’s community and integrity. 
            In financial networks, thieves and illegal activities are often anomalous in nature. Hence, we will be 
            implementing an anomaly detection framework which aims to reveal the identities of illicit transaction 
            makers as well as spot outliers in the network.
        """
    
        address = """
            The dataset is an input graph collected from Blockahir's bitcoin blockchain. A row in the dataset 
            represents an input along with its transactions data which is joined by its transaction hash. 
            Each node has 27 features used along with its hash which contains the transactions hash and its account 
            which contains the address of the recipient. Each input is marked as illicit if the recipient has 
            been reported to have used its account for illicit activites such as sextortion, blackmail, darknet market, 
            ransomeware etc. Illicit address are collected from bitcoinabuse.com which has the cryptocurrency community 
            reporting addresses where they have encountered activity from hackers and criminals to make the internet a safer place. 
            Additional address are also collected from the BABD-13 dataset used in the Basic models are created and trained for 
            demonstration purposes on 2022-07-01 data, parameters are available in our GitHub source code."""

        outlier = """
            An Outlier is a rare chance of occurrence within a given data set and is an observation point that is distant from 
            other observations. Outliers act as signals that an anomalous transaction or activity might be taking place in the Blockchain network. 
            In this scenario, we used a combination of metrices such as Transaction Volume, Transaction Count etc as features for our 
            outlier detection models.
        """

        card = []
        card.append(html.Div([html.P("Background of Anomaly Detection Framework", style={'font-weight':'bold', 'textDecoration':'underline', 'color': '#0a275c'}), html.P(ov)]))
        card.append(html.Div([html.P("Address Detection", style={'font-weight':'bold', 'textDecoration':'underline', 'color': '#0a275c'}), html.P(address)]))
        card.append(html.Div([html.P("Outlier Detection", style={'font-weight':'bold', 'textDecoration':'underline', 'color': '#0a275c'}), html.P(outlier)]))
        return "Overview", card

    selected = ctx.triggered[0]["prop_id"].split(".")[0]
    return titles_dict[selected], None

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
    model_name = model.split("_")[1]

    df_illicit_cols = {model: 'Illicit Account', 'account': 'Recipient Address', 'hash': 'Transaction Hash', 'value': 'Value (BTC)', 'value_usd': 'Value (USD)', 
                       'is_from_coinbase': 'Is From Coinbase', 'is_spendable': 'Is Spendable', 'spending_index': 'Spending Index', 'spending_value_usd': 'Spending Value (USD)', 
                       'lifespan': 'Lifespan', 'cdd': 'CDD', 'size': 'Size', 'weight': 'Weight', 'version': 'Version', 'lock_time': 'Lock Time', 'is_coinbase': 'Is Coinbase', 
                       'has_witness': 'Has Witness', 'input_count': 'Input Count', 'output_count': 'Output Count', 'input_total': 'Total Input (BTC)', 'input_total_usd': 'Total Input (USD)', 
                       'output_total': 'Total Output (BTC)', 'output_total_usd': 'Total Output (USD)', 'fee': 'Fee (BTC)', 'fee_usd': 'Fee (USD)', 
                       'fee_per_kb': 'Fee Per KB (BTC)', 'fee_per_kb_usd': 'Fee Per KB (USD)', 'fee_per_kwu': 'Fee Per KWU (BTC)', 'fee_per_kwu_usd': 'Fee Per KWU (USD)', 'cdd_total': 'CDD Total'}
    
    tables.append(html.P("Model Performance:", style = {'font-weight': 'bold'}))
    tables.append(dash_table.DataTable(
        columns = [
            {'name': 'Accuracy', 'id': 'test_acc', 'type':'string'},
            {'name': 'Precision', 'id': 'test_precision', 'type':'string'},
            {'name': 'Recall', 'id': 'test_recall', 'type':'string'},
            {'name': 'F1 Score', 'id': 'test_f1score', 'type':'string'}
        ],
        data = df_illicit_results.loc[df_illicit_results['class'] == model_name].to_dict('records'),
        style_header = {'font-size':'16px', 'color': 'black', 'text-transform': 'none'},
        style_cell = {'font-family':'Trebuchet MS', 'font-size':'15px', 'textAlign': 'left', 
                      'color': '#0a275c', 'padding': '5px 10px 5px 10px'},
        id = 'anomaly-performance-table'
    ))

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
    [Input("anomaly-dtc", "n_clicks"), 
    Input("anomaly-knn", "n_clicks"), 
    Input("anomaly-xgboost", "n_clicks"),
    Input("outlier-isoForest", "n_clicks"), 
    Input("outlier-autoEncoder", "n_clicks"), 
    Input("outlier-kmeans", "n_clicks")],
    Input('anomaly-title', 'children'),
    prevent_initial_call = True
)

def update_line_chart(dtc, knn, xgboost, iso, autoEncoder, kmeans, curr_title):
    graphs = []
    if ctx.triggered[0]["prop_id"].split(".")[0] == 'anomaly-dtc':
        graphs = create_table(df_illicit, 'y_dtc_pred')
    
    elif ctx.triggered[0]["prop_id"].split(".")[0] == 'anomaly-knn':
        graphs = create_table(df_illicit, 'y_knn_pred')
    
    elif ctx.triggered[0]["prop_id"].split(".")[0] == 'anomaly-xgboost':
        graphs = create_table(df_illicit, 'y_xgb_pred')
    
    elif ctx.triggered[0]["prop_id"].split(".")[0] == 'outlier-isoForest':
        graphs = create_fig(df_isoForest, 'isoForest')
    
    elif ctx.triggered[0]["prop_id"].split(".")[0] == 'outlier-autoEncoder':
        graphs = create_fig(df_autoEncoder, 'autoEncoder')
    
    elif ctx.triggered[0]["prop_id"].split(".")[0] == 'outlier-kmeans':
        graphs = create_cluster(df_kmeans, 'kmeans')          
    
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
        if model.split("using ")[1] == 'Decision Tree':
            model = 'y_dtc_pred'
    
        elif model.split("using ")[1] == 'K-Nearest Neighbours':
            model = 'y_knn_pred'
    
        elif model.split("using ")[1] == 'XGBoost':
            model = 'y_xgb_pred'
    
        if not value:
            value=[model, 'account', 'hash', 'value', 'value_usd']
        for feature in value:
            columns.append({
                'name': df_illicit_cols[feature],
                'id': feature
            })
        return columns
    except:
        print("This is not address detection!")