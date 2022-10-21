import dash
from dash import html, dcc, ALL, ctx
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from app import app
from navbar import create_navbar
import pandas as pd
import plotly.express as px
from datetime import date, datetime
import re
import duckdb as ddb
import psycopg2

dash.register_page(__name__, path='/analytics', name="Analytics")
nav = create_navbar()

# Connecting to database
ddbconn = ddb.connect("/home/ec2-user/bitcoin-basic-metrics/bitcoin.duckdb", read_only=True)
ddbc = ddbconn.cursor()

psqlconn = psycopg2.connect(database="bitcoin",
                            host="localhost",
                            user="ec2-user",
                            password="password",
                            port="5432")

psqlcursor = psqlconn.cursor()

# Dataframe for our metrics and indicators
basic_metrics = pd.read_sql("SELECT * FROM basic_metrics", psqlconn)
computed_metrics = pd.read_sql("SELECT * FROM computed_metrics", psqlconn)

# Metrics and their descriptions
metrics_desc = pd.read_csv("assets/metrics_desc.csv")


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
                        [dbc.ListGroupItem(x, action=True, id={"type": "list-group-item", "index": x}, color = '#E8EBEE00', style = {'font-size': '13px'}) for x in sorted(metrics_desc['metric_name'].tolist())], 
                        id='list-group', 
                        flush = True,
                        style={'margin-top':'15px', 'overflow-y':'scroll', 'width':'350px', 'height': '450px'})
                    
                ], justify = 'center', style = {'padding':'25px', 'border-top': '2px solid grey'}),

                # dbc.Row([
                #     html.P("Select a date range", style = {'align':'left'}),
                #     html.Div([
                #         dcc.DatePickerRange(
                #             id='my-date-picker-range',
                #             clearable = True,
                #             min_date_allowed=date(2009, 1, 3),
                #             max_date_allowed=datetime.now(),
                #             start_date_placeholder_text='MM/DD/YY',
                #             end_date_placeholder_text='MM/DD/YY',
                #         ),
                #     ]),
                # ], justify = 'center', style = {'text-align':'left', 'padding': '0px 25px 25px 25px'})

            ], width = 3, style = {'background-color':'#E8EBEE99',  'border-right':'2px solid grey', 'padding-top': '20px'}),

            # Area to display selected indicator's graph
            dbc.Col([
                html.Div([
                    html.H5(id='graph-title', style = {'display': 'inline-block', 'vertical-align': 'middle', 'margin': '10px 0'}),
                    dbc.Button(id='toast-toggle', n_clicks=0, className="bi bi-question-circle rounded-circle", color='white', style={'display': 'inline-block', 'vertical-align': 'middle', 'margin': '10px 0'}),
                    html.Div([
                        html.Span("Select your preferred date range.", className='date-picker-text', style = {'font-size':'12px'}),
                            dcc.DatePickerRange(
                            id='my-date-picker-range',
                            clearable = True,
                            min_date_allowed=date(2009, 1, 3),
                            max_date_allowed=datetime.now(),
                            start_date_placeholder_text='MM/DD/YY',
                            end_date_placeholder_text='MM/DD/YY',
                        ),
                    ], className='date-picker-div', style = {'display':'inline-block', 'position': 'relative', 'float':'right', 'margin-top':'13px'})
                ]),
                
                html.Div(
                    dbc.Toast(
                            [html.P(id='metric-desc')],
                            id="toast",
                            header="Metric Description",
                            dismissable=True,
                            is_open=False,
                            style = {'width':'48vw'}
                    ), style = {'padding-top': '15px', 'padding-bottom':'15px'}
                ),

                dcc.Graph(id="analytics-graph", style={'height': '80vh'}),

            ], width = 9, style = {'padding-right':'40px', 'padding-left':'30px', 'padding-top': '20px'})

    ], justify = 'evenly', style={'height': '100vh', 'border-top': '2px solid grey'})
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
    label_id = {"Bitcoin": "Bitcoin (BTC)", "Ethereum": "Ethereum (ETH)", "Tether": "Tether (USDT)"}
    if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:
        return "Bitcoin (BTC)"
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    return label_id[button_id]

# Update listed metrics based on search term
@app.callback(
   Output("list-group", "children"),
   Input("searchbar", "value"),
   prevent_initial_call=True  
)
def update_metrics(searchterm):
    if searchterm == "": # when search bar is cleared
        return [dbc.ListGroupItem(x, action=True, id={"type": "list-group-item", "index": x}, color = '#E8EBEE00', style = {'font-size': '13px'}) for x in sorted(metrics_desc['metric_name'].tolist())]
    # when there is non-empty input to search bar
    result = metrics_desc['metric_name'].tolist().copy()
    for word in searchterm.split(" "):
        count=0
        while count < len(result):
            if word.lower() not in result[count].lower():
                result.remove(result[count])
            else:
                count+=1
    return [dbc.ListGroupItem(i, action=True, id={"type": "list-group-item", "index": i}, color = '#E8EBEE00', style = {'font-size': '13px'}) for i in result]

# Update line graph data
@app.callback(
    Output("analytics-graph", "figure"), 
    Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
    Input('my-date-picker-range', 'start_date'),
    Input('my-date-picker-range', 'end_date'),
    Input('graph-title', 'children'),
    Input({'type': 'list-group-item', 'index': ALL}, 'id')
)
def update_line_chart(n_clicks_list, start, end, curr_metric, id_list):
    # provides default range selectors
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
    if ctx.triggered_id is None or 1 not in n_clicks_list:
        fig = px.line(basic_metrics, x="Date", y="Price ($)", color_discrete_sequence=["#0a275c"])
        fig.update_layout(
          xaxis=default
        )
        
    elif ctx.triggered_id is not None or 1 in n_clicks_list:
        # print for debugging
        # print(clicked_id)
        # print(n_clicks_list.index(1))
        # print(id_list[n_clicks_list.index(1)]['index'])
        clicked = id_list[n_clicks_list.index(1)]['index']
        if metrics_desc[metrics_desc['metric_name'] == clicked]['is_computed'].values[0]:
            fig = px.line(computed_metrics, x="Date", y=clicked, color_discrete_sequence=["#0a275c"])
        else:
            fig = px.line(basic_metrics, x="Date", y=clicked, color_discrete_sequence=["#0a275c"])
        fig.update_layout(
            xaxis=default
        )
    # update graph based on date range selected by user
    if start is not None and end is not None:
        if metrics_desc[metrics_desc['metric_name'] == curr_metric]['is_computed'].values[0]:
            filtered_df = computed_metrics[computed_metrics['Date'].between(start, end)]
            fig = px.line(filtered_df, x="Date", y=curr_metric, color_discrete_sequence=["#0a275c"])
        else:
            filtered_df = basic_metrics[basic_metrics['Date'].between(start, end)]
            fig = px.line(filtered_df, x="Date", y=curr_metric, color_discrete_sequence=["#0a275c"])
    # return default range when datepicker is empty/cleared
    elif start is None and end is None:
        if metrics_desc[metrics_desc['metric_name'] == curr_metric]['is_computed'].values[0]:
            fig = px.line(computed_metrics, x="Date", y=curr_metric, color_discrete_sequence=["#0a275c"])
        else:
            fig = px.line(basic_metrics, x="Date", y=curr_metric, color_discrete_sequence=["#0a275c"])
        fig.update_layout(
            xaxis=default
        )
    fig.update_layout(plot_bgcolor='white')
    fig.update_xaxes(rangeselector_font_size = 15)
    
    return fig

# Update graph title and description
@app.callback(
    Output('graph-title', 'children'),
    Output('metric-desc', "children"),
    Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
)
def update_title_desc(n_clicks_list):
    if not ctx.triggered or 1 not in n_clicks_list:
        return "Price ($)", metrics_desc[metrics_desc['metric_name'] == 'Price ($)']['description']
    clicked_id = ctx.triggered_id.index
    return clicked_id, metrics_desc[metrics_desc['metric_name'] == clicked_id]['description']

# Toggling metric's description to be shown or not
@app.callback(
    Output("toast", "is_open"),
    Input("toast-toggle", "n_clicks"),
)
def open_toast(n):
    if n == 0:
        return False
    return True
