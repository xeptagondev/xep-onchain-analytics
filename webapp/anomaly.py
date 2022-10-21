import dash
from dash import html, dcc, ALL, ctx
from dash.dependencies import Input, Output, ALL
import dash_bootstrap_components as dbc
from app import app
from navbar import create_navbar
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, datetime
from dateutil.parser import parse

import psycopg2
from sqlalchemy import create_engine

dash.register_page(__name__, path='/anomaly', name="Anomaly Detection")
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
                html.Div([
                    html.H4(id="anomaly-title", style = {'display':'inline-block', 'vertical-align':'center'}),
                    html.Div([
                            html.Span("Select your preferred date range.", className='date-picker-text', style = {'font-size':'12px'}),
                                dcc.DatePickerRange(
                                id='my-date-picker-range2',
                                clearable = True,
                                min_date_allowed=date(2009, 1, 3),
                                max_date_allowed=datetime.now(),
                                start_date_placeholder_text='MM/DD/YY',
                                end_date_placeholder_text='MM/DD/YY',
                            ),
                        ], className='date-picker-div', style = {'display':'inline-block', 'position': 'relative', 'float':'right', 'margin-top':'13px'})
                    ]),
                html.Div([
                    # dbc.Button('ALL', id="range-all-button", 
                    #             style={'background-color':'#E8EBEE99', 'border-color':'#E8EBEE99', 'color':'#354447', 'margin-left':'240px', 'bottom':'59.7%', 
                    #                     'font-size':15, 'z-index':'1', 'position':'absolute', 'height':'25px', 'width':'30px', 'padding':'0px 1px 3px'}),
                    #dcc.Graph(id="anomaly-graph", style= {'z-index':'-1', 'height': '80vh'}),
                    
                    html.Div(id='outlier-graphs')

                ], style= {'margin-top':'30px', 'width':'72vw', 'height':'70vh', 'overflow-y':'scroll'})

            ], width = 9, style = {'padding-right':'40px', 'padding-left':'30px', 'padding-top': '20px'})
    
    ], justify = 'evenly', style={'position':'fixed', 'height': '100vh', 'border-top': '2px solid grey'})

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

# standalone method to reduce repeated chunks in callback below ##
def create_fig(df, model):
    graphs = []
    default = dict(rangeselector=dict(buttons=list([
                dict(count=1,
                    label="1M",
                    step="month",
                    stepmode="backward"),
                dict(count=6,
                    label="6M",
                    step="month",
                    stepmode="backward"),
                dict(count=1,
                    label="YTD",
                    step="year",
                    stepmode="todate"),
                dict(count=1,
                    label="1Y",
                    step="year",
                    stepmode="backward"),
                dict(label="ALL", step="all")
                ])
            ),
            rangeslider=dict(
                visible=True
            ),
            type="date"
        )
    non_features = ['Date', 'index', 'anomaly_IsoForest', 'anomaly_kmeans']
    features = set(df.columns) - set(non_features)
    for c in features:
        fig1 = px.line(df, x="Date", y=c, color_discrete_sequence=["#0a275c"])
        outlier = df.loc[df['anomaly_kmeans'] == 1] if model == 'kmeans' else df.loc[df['anomaly_IsoForest'] == 1]
        fig2 = px.scatter(outlier, x="Date", y=c, color_discrete_sequence=["firebrick"])
        fig = go.Figure(data = fig1.data + fig2.data)
        fig.update_xaxes(default) #adding in default range selectors & slider
        # setting date range limit to fix bug with range selector
        fig.update_xaxes(range=[df['Date'].iloc[0], df['Date'].iloc[-1]],
                        rangeslider_range=[df['Date'].iloc[0], df['Date'].iloc[-1]])
        fig.update_xaxes(rangeselector_font_size = 15, title_text = "Date")
        fig.update_yaxes(title_text = c)
        fig.update_layout(plot_bgcolor='white', title_text = 'Plot of {} over Time'.format(c), title_x=0.5, title_font_color = 'black', title_font_size=14) 
        graphs.append(dcc.Graph(id ='{}-{}'.format(model,c), figure = fig))

    return graphs

@app.callback(
    Output("outlier-graphs", "children"),
    [Input("outlier-isoForest", "n_clicks"), Input("outlier-kmeans", "n_clicks")],
    Input('my-date-picker-range2', 'start_date'),
    Input('my-date-picker-range2', 'end_date'),
    Input('anomaly-title', 'children'),
    #Input('range-all-button', 'n_clicks')
)

def update_line_chart(iso, kmeans, start, end, curr_title):
    graphs = []
    if ctx.triggered[0]["prop_id"].split(".")[0] == 'outlier-isoForest':
        print("1st")             
        graphs = create_fig(df_isoForest, 'isoForest')
    elif ctx.triggered[0]["prop_id"].split(".")[0] == 'outlier-kmeans':
        print("2nd")
        graphs = create_fig(df_kmeans, 'kmeans')
    
    #update graph based on date range selected by user
    if start is not None and end is not None:
        if 'isolation forest' in curr_title.lower():
            print("3rd")
            filtered_df = df_isoForest[df_isoForest['Date'].between(parse(start).date(), parse(end).date())]
            graphs = create_fig(filtered_df, 'isoForest')
        elif 'k-means' in curr_title.lower():
            print("4th")
            filtered_df = df_kmeans[df_kmeans['Date'].between(parse(start).date(), parse(end).date())]
            graphs = create_fig(filtered_df, 'kmeans')

    # return default range when datepicker is empty/cleared
    elif start is None and end is None:
        if 'isolation forest' in curr_title.lower():
            print("5th")
            graphs = create_fig(df_isoForest, 'isoForest')
        elif 'k-means' in curr_title.lower():
            print("6th")
            graphs = create_fig(df_kmeans, 'kmeans')           
    
    return graphs
