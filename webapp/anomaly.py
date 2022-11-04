import dash
from dash import html, dcc, ALL, ctx, dash_table
from dash.dependencies import Input, Output, ALL
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

dash.register_page(__name__, path='/anomaly', name="Anomaly Detection")
nav = create_navbar()
footer = create_footer()

# Connecting to PostgreSQL Database
engine = create_engine('postgresql://ec2-user:password@localhost:5432/bitcoin')

psqlconn = psycopg2.connect(database="bitcoin",
                            host="44.206.88.106",
                            user="ec2-user",
                            password="password",
                            port="5432")

psqlcursor = psqlconn.cursor()

# Dataframe for output of Anomaly Detection
df_isoForest = pd.read_sql('SELECT * FROM "isoForest_outliers"', psqlconn)
df_autoEncoder = pd.read_sql('SELECT * FROM "autoEncoder_outliers"', psqlconn)
df_kmeans = pd.read_sql('SELECT * FROM "kmeans_outliers"', psqlconn)
df_illicit = pd.read_sql('SELECT * FROM "anomaly_predictions"', psqlconn)
df_illicit_results = pd.read_sql('SELECT * FROM "anomaly_results"', psqlconn)
df_kmeans["anomaly"] = df_kmeans["anomaly"].astype(str)
df_kmeans["cluster"] = df_kmeans["cluster"].astype(str)

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
                    # html.Div([
                    #         html.Span("Select your preferred date range.", className='date-picker-text', style = {'font-size':'12px'}),
                    #             dcc.DatePickerRange(
                    #             id='my-date-picker-range2',
                    #             clearable = True,
                    #             min_date_allowed=date(2009, 1, 3),
                    #             max_date_allowed=datetime.now(),
                    #             start_date_placeholder_text='MM/DD/YY',
                    #             end_date_placeholder_text='MM/DD/YY',
                    #         ),
                    #     ], className='date-picker-div', style = {'display':'inline-block', 'position': 'relative', 'float':'right', 'margin-top':'13px'})
                    ]),
                html.Div([
                    # dbc.Button('ALL', id="range-all-button", 
                    #             style={'background-color':'#E8EBEE99', 'border-color':'#E8EBEE99', 'color':'#354447', 'margin-left':'240px', 'bottom':'59.7%', 
                    #                     'font-size':15, 'z-index':'1', 'position':'absolute', 'height':'25px', 'width':'30px', 'padding':'0px 1px 3px'}),
                    #dcc.Graph(id="anomaly-graph", style= {'z-index':'-1', 'height': '80vh'}),
                    dcc.Loading([
                        html.Div(id='anomaly-description'),
                        html.Div(id='anomaly-graphs')],
                        color='#0a275c',
                        style = {'padding-top':'40px'}
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
                   "outlier-isoForest": "Outliers Detected Using Isolation Forest", 
                   "outlier-autoEncoder": "Outliers Detected Using Auto-Encoders", 
                   "outlier-kmeans": "Outliers Detected Using K-Means Clustering"}
    if not ctx.triggered: #default
        card = html.P("Due to the nature of a Blockchain network, cryptocurrencies are becoming the preferred option for cybercriminal activities. Criminals are turning towards cryptocurrency as it is anonymous, quick, requires no former relationship between parties, and can facilitate seamless international trades. With the number of cybercriminal activities on the rise, members of the network would want to detect these criminals as soon as possible to prevent them from harming the network’s community and integrity. In financial networks, thieves and illegal activities are often anomalous in nature. Hence, we will be implementing an anomaly detection framework which aims to reveal the identities of illicit transaction makers as well as spot outliers in the network.")
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
    graphs.append(dbc.ListGroup([dbc.ListGroupItem(c, style={'font-size':'13px', 'color':'#0a275c', 'font-weight':'bold', 'border-top':'None', 'border-bottom':'None', 'border-radius':'0px'}) for c in f_list], horizontal=True))

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

    for c in features:
        graphs.append(html.P(c))

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
    
    tables.append(html.P("Model Performance:"))
    tables.append(dash_table.DataTable(
        columns = [
            {'name': 'Accuracy', 'id': 'test_acc', 'type':'string'},
            {'name': 'Precision', 'id': 'test_precision', 'type':'string'},
            {'name': 'Recall', 'id': 'test_recall', 'type':'string'},
            {'name': 'F1 Score', 'id': 'test_f1score', 'type':'string'}
        ],
        data = df_illicit_results.loc[df_illicit_results['class'] == model_name].to_dict('records'),
        style_header = {'font-size':'17px', 'color': 'black'},
        style_cell = {'font-family':'Trebuchet MS', 'textAlign': 'left', 'font-size':'14px', 'color': '#0a275c'},
        id = 'anomaly-performance-table'
    ))

    tables.append(html.P("Accounts detected to have illicit transactions:", style = {'padding-top':'20px'}))
    tables.append(dbc.Row([
        dbc.Col([
            dcc.Dropdown(value=10, clearable=False, style={'width':'35%'}, options=[10, 25, 50, 100], id='row-drop')
        ], style={'padding-bottom':'15px'}),
    ]))
    tables.append(dash_table.DataTable(
        columns = [
            {'name': 'Account Address', 'id': 'account', 'type':'string'},
            {'name': 'Transaction Hash', 'id': 'hash', 'type':'string'},
            {'name': 'Transaction Value (BTC)', 'id': 'value', 'type':'numeric'},
            {'name': 'Transaction Value (USD)', 'id': 'value_usd', 'type':'numeric'},
            {'name': 'Illicit Account', 'id': model, 'type':'numeric'}
        ],
        data = df.to_dict('records'),
        page_size = 10,
        style_header = {'font-size':'17px', 'color': 'black'},
        style_cell = {'font-family':'Trebuchet MS', 'textAlign': 'left', 'color': '#0a275c',
                      'font-size':'14px'},
        style_data = {'overflow': 'hidden', 'textOverflow': 'ellipsis', 'maxWidth': 0},
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
    # Input('my-date-picker-range2', 'start_date'),
    # Input('my-date-picker-range2', 'end_date'),
    #Input('range-all-button', 'n_clicks')
)

def update_line_chart(dtc, knn, xgboost, iso, autoEncoder, kmeans, curr_title):
    graphs = []
    if ctx.triggered[0]["prop_id"].split(".")[0] == 'anomaly-dtc':
        print("1st")             
        graphs = create_table(df_illicit, 'y_dtc_pred')
    elif ctx.triggered[0]["prop_id"].split(".")[0] == 'anomaly-knn':
        print("2nd")
        graphs = create_table(df_illicit, 'y_knn_pred')
    elif ctx.triggered[0]["prop_id"].split(".")[0] == 'anomaly-xgboost':
        print("3rd")
        graphs = create_table(df_illicit, 'y_xgb_pred')
    elif ctx.triggered[0]["prop_id"].split(".")[0] == 'outlier-isoForest':
        print("4th")             
        graphs = create_fig(df_isoForest, 'isoForest')
    elif ctx.triggered[0]["prop_id"].split(".")[0] == 'outlier-autoEncoder':
        print("5th")
        graphs = create_fig(df_autoEncoder, 'autoEncoder')
    elif ctx.triggered[0]["prop_id"].split(".")[0] == 'outlier-kmeans':
        print("6th")
        graphs = create_cluster(df_kmeans, 'kmeans')
    
    # # update graph based on date range selected by user
    # if start is not None and end is not None:
    #     if 'isolation forest' in curr_title.lower():
    #         print("4th")
    #         filtered_df = df_isoForest[df_isoForest['Date'].between(parse(start).date(), parse(end).date())]
    #         graphs = create_fig(filtered_df, 'isoForest')
    #     elif 'auto-encoder' in curr_title.lower():
    #         print("5th")
    #         filtered_df = df_autoEncoder[df_autoEncoder['Date'].between(parse(start).date(), parse(end).date())]
    #         graphs = create_fig(filtered_df, 'autoEncoder')
    #     elif 'k-means' in curr_title.lower():
    #         print("6th")
    #         filtered_df = df_kmeans[df_kmeans['Date'].between(parse(start).date(), parse(end).date())]
    #         graphs = create_cluster(filtered_df, 'kmeans')

    # # return default range when datepicker is empty/cleared
    # elif start is None and end is None:
    #     if 'isolation forest' in curr_title.lower():
    #         print("7th")
    #         graphs = create_fig(df_isoForest, 'isoForest')
    #     elif 'auto-encoder' in curr_title.lower():
    #         print("8th")
    #         graphs = create_fig(df_autoEncoder, 'autoEncoder')  
    #     elif 'k-means' in curr_title.lower():
    #         print("9th")
    #         graphs = create_cluster(df_kmeans, 'kmeans')           
    
    return graphs

@app.callback(
    Output("anomaly-table", "page_size"),
    Input("row-drop", "value"),
)

def update_row_dropdown(row_v):
    return row_v