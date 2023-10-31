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
from dash.exceptions import PreventUpdate
import dash_mantine_components as dmc

dash.register_page(__name__, path='/analytics', name="Analytics")
nav = create_navbar()
footer = create_footer()

# Database configurations (Bitcoin)
with open("config.json") as config_file:
    config = json.load(config_file)

# Connecting to PostgreSQL database
psqlconn = psycopg2.connect(database = config['postgre']['database'],
                            host = config['postgre']['host'],
                            user = config['postgre']['user'],
                            password = config['postgre']['password'],
                            port = config['postgre']['port'])

psqlcursor = psqlconn.cursor()

# Dataframe for our metrics and indicators
basic_metrics = pd.read_sql("SELECT * FROM basic_metrics", psqlconn)
computed_metrics = pd.read_sql("SELECT * FROM computed_metrics", psqlconn)

bm_eth = pd.read_sql("SELECT * FROM basic_metrics_ethereum", psqlconn)
cm_eth = pd.read_sql("SELECT * FROM computed_metrics_ethereum", psqlconn)


# Metrics and their descriptions
metrics_desc = pd.read_csv("assets/metrics_desc.csv")
md_eth = pd.read_csv("assets/metrics_desc_eth.csv")


bm_dict = {'Bitcoin': basic_metrics, 'Ethereum': bm_eth}
cm_dict = {'Bitcoin': computed_metrics, 'Ethereum': cm_eth}
md_dict = {'Bitcoin': metrics_desc, 'Ethereum': md_eth}


def show_metrics_list(idx, metric_description_df):
    return html.Div(
        [
            dbc.ListGroupItem(
                sorted(metric_description_df['metric_name'].tolist())[int(idx)], # metric_name
                action=True,
                id={"type": "list-group-item", "index": sorted(metric_description_df['metric_name'].tolist())[int(idx)]},
                color = '#E8EBEE00', 
                style = {'font-size': '13px'},
                # class_name = "border-0"
            ),
            dbc.Tooltip(
                metric_description_df[metric_description_df['metric_name'] == sorted(metric_description_df['metric_name'].tolist())[int(idx)]]['description'],
                id="tooltip" + idx, 
                target={"type": "list-group-item", "index": sorted(metric_description_df['metric_name'].tolist())[int(idx)]}
            )
        ]
    )

content = html.Div([

    dcc.Store(id='bm-data', storage_type='memory', data=bm_dict['Bitcoin'].to_dict('records')),
    dcc.Store(id='cm-data', storage_type='memory', data=cm_dict['Bitcoin'].to_dict('records')),
    dcc.Store(id='md-data', storage_type='memory', data=md_dict['Bitcoin'].to_dict('records')),


    dbc.Row([
            # Control panel column
            dbc.Col([
                
                # Select Cryptocurrency 
                dbc.Row([
                    html.P(" Select Cryptocurrency", className = 'bi bi-coin', style={'color':'black', 'text-align':'center', 'font-size':'15px', 'font-family':'Open Sans', 'font-weight':'bold'}),
                    dbc.DropdownMenu(
                        [dbc.DropdownMenuItem("Bitcoin (BTC)", id="Bitcoin"),
                        dbc.DropdownMenuItem(divider=True),
                        dbc.DropdownMenuItem("Ethereum (ETH)", id="Ethereum"),
                        dbc.DropdownMenuItem(divider=True),
                        html.Div([
                            html.Span("to be implemented in future", className='disabled-info'),
                            dbc.DropdownMenuItem("Tether (USDT)", id="Tether", disabled=True),
                        ], className='disabled-coin-2'),
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

                    dbc.ListGroup([show_metrics_list(f"{i}", metrics_desc) for i in range(len(metrics_desc))],
                                    id='list-group', 
                                    flush = True,
                                    style={'margin-top':'15px', 'overflow-y':'scroll', 'width':'350px', 'height': '450px'})
                    
                ], justify = 'center', style = {'padding':'25px', 'border-top': '2px solid grey'}),

            ], width = 3, style = {'background-color':'#E8EBEE99',  'border-right':'2px solid grey', 'padding-top': '20px'}),

            # Display column
            dbc.Col([
                html.Div([
                    html.H5(id='graph-title', style = {'display': 'inline-block', 'vertical-align': 'middle', 'margin': '10px 0', 'color':'#0a275c'}),
                    dbc.Button(id='toast-toggle', n_clicks=0, className="bi bi-question-circle rounded-circle", color='white', style={'display': 'inline-block', 'vertical-align': 'middle', 'margin': '10px 0'}),
                    html.Div([
                        html.Span("Select your preferred date range.", className='date-picker-text', style = {'font-size':'12px'}),
                            dmc.DateRangePicker(
                                id='my-date-picker-range',
                                placeholder='MM/DD/YYYY - MM/DD/YYYY',
                                inputFormat="MM/DD/YYYY",
                                clearable = True,
                                minDate=date(2009, 1, 3),
                                maxDate=datetime.now(),
                                style = {'width':'250px'}
                            ),
                    ], className='date-picker-div', style = {'display':'inline-block', 'position': 'relative', 'float':'right', 'margin-top':'13px'})
                ]),
                # area for toggable toast to display metric's formula
                html.Div(
                    dbc.Toast(
                            [dcc.Markdown(mathjax=True, id='metric-formula')],
                            id="toast",
                            header="How is it Calculated?",
                            dismissable=True,
                            is_open=False,
                            style = {'width':'40vw'}
                    ), style = {'padding-top': '15px', 'padding-bottom':'15px'}
                ),
                # area for metric description
                html.Div([
                    html.P("Metric Description", style={'font-weight':'bold', 'textDecoration':'underline'}),
                    html.P(id='metric-desc', style={'width':'48vw'}),
                ]),

                # area to display selected metric's graph
                dcc.Loading(
                    dcc.Graph(id="analytics-graph", style={'height': '80vh'}),
                    color='#0a275c'
                )

            ], width = 9, style = {'padding-right':'40px', 'padding-left':'30px', 'padding-top': '20px'})

    ], justify = 'evenly', style={'border-top': '2px solid grey', 'border-bottom': '1px solid grey'})
], style = {'padding-bottom':'60px'})



def create_charts():
    layout = html.Div([
        nav,
        content,
        footer
    ], style={'min-height':'100%', 'position':'relative', 'overflow-x':'hidden'})
    return layout

################## Callbacks ##############################

# Update dropdown label 
@app.callback(
    Output('cryptocurrency-select', "label"),
    Output('bm-data', "data"),
    Output('cm-data', "data"),
    Output('md-data', "data"),
    [Input("Bitcoin", "n_clicks"), Input("Ethereum", "n_clicks"), Input("Tether", "n_clicks")]
)

def update_dropdown(n1, n2, n3):
    label_id = {"Bitcoin": "Bitcoin (BTC)", "Ethereum": "Ethereum (ETH)", "Tether": "Tether (USDT)"}
    if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:
        # bm = basic_metrics.to_dict('records')
        # cm = computed_metrics.to_dict('records')
        # md = metrics_desc.to_dict('records')
        # return "Bitcoin (BTC)", bm, cm, md
        raise PreventUpdate
    else:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]
        bm = bm_dict[button_id].to_dict('records')
        cm = cm_dict[button_id].to_dict('records')
        md = md_dict[button_id].to_dict('records')
        return label_id[button_id], bm, cm, md


# Update listed metrics based on search term
@app.callback(
   Output("list-group", "children"),
   Input("searchbar", "value"),
   Input('md-data', 'modified_timestamp'),
   State('md-data', 'data'),
   prevent_initial_call=True  
)
def update_metrics(searchterm, ts, md_data):
    names_df = pd.DataFrame.from_dict(md_data)

    triggered_comp = ctx.triggered_id
    if triggered_comp == 'searchbar':

        if searchterm == "": # when search bar is cleared
            return [show_metrics_list(f"{i}", names_df) for i in range(len(names_df))]
        result = names_df['metric_name'].tolist().copy()
        for word in searchterm.split(" "):
            count=0
            while count < len(result):
                if word.lower() not in result[count].lower():
                    result.remove(result[count])
                else:
                    count+=1
        metric_desc_filtered = names_df[names_df['metric_name'].isin(result)]
        return [show_metrics_list(f"{i}", metric_desc_filtered) for i in range(len(metric_desc_filtered))]
    
    else:
        return [show_metrics_list(f"{i}", names_df) for i in range(len(names_df))]

# Method to plot basic metrics
def plot_basic_metrics(fig, df, metric):
    if (metric != "Difficulty Ribbon"):
        fig.add_trace(go.Scatter(x=df['Date'], y=df[metric], mode='lines', line = dict(color = "#0a275c")))
    else:
        fig.add_trace(go.Scatter(x=df['Date'], y=df["Difficulty"], mode='lines', line = dict(color = "#0a275c"), name = "Market"))

        rolling_window = [14, 25, 40, 60, 90, 128, 200]
        for i in rolling_window:
            rolling_mean = df["Difficulty"].rolling(window=i).mean()
            trace = go.Scatter(x = df['Date'], y=rolling_mean, mode='lines', line = dict(color = "rgba(255, 0, 0, 0.5)"), name = "D{}".format(i))
            fig.add_trace(trace)

    return fig

# Update line graph data
@app.callback(
    Output("analytics-graph", "figure"), 
    Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
    Input('my-date-picker-range', 'value'),
    Input('graph-title', 'children'),
    Input({'type': 'list-group-item', 'index': ALL}, 'id'),
    State('bm-data', 'data'),
    State('cm-data', 'data'),
    State('md-data', 'data'),
)
def update_line_chart(n_clicks_list, dates, curr_metric, id_list, bm_data, cm_data, md_data):
    bm_df = pd.DataFrame.from_dict(bm_data)
    cm_df = pd.DataFrame.from_dict(cm_data)
    md_df = pd.DataFrame.from_dict(md_data)

    if dates is None:
        start = None
        end = None
    else:
        start, end = dates

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
            visible=True,
            bgcolor="#d0e0e5",
            thickness=0.1 
        ),
        type="date"
    )
    fig = go.Figure()
    if ctx.triggered_id is None or 1 not in n_clicks_list:
        fig.add_trace(go.Scatter(x=basic_metrics['Date'], y=basic_metrics['Price ($)'],
                    mode='lines', line = dict(color = "#0a275c")))
        
    elif ctx.triggered_id is not None or 1 in n_clicks_list:
        clicked = id_list[n_clicks_list.index(1)]['index']
        if md_df[md_df['metric_name'] == clicked]['is_computed'].values[0]:
            fig.add_trace(go.Scatter(x=cm_df['Date'], y=cm_df[clicked],
                    mode='lines', line = dict(color = "#0a275c")))
        else:
            plot_basic_metrics(fig, bm_df, clicked)

    # Update graph based on date range selected by user
    if start is not None and end is not None:
        if md_df[md_df['metric_name'] == curr_metric]['is_computed'].values[0]:
            filtered_df = cm_df[cm_df['Date'].between(start, end)]
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=filtered_df['Date'], y=filtered_df[curr_metric],
                    mode='lines', line = dict(color = "#0a275c")))
        else:
            filtered_df = bm_df[bm_df['Date'].between(start, end)]
            fig = go.Figure()
            plot_basic_metrics(fig, filtered_df, curr_metric)
        fig.update_xaxes(rangeslider=dict(
                            visible=True,
                            bgcolor="#d0e0e5",
                            thickness=0.1))
        fig.update_yaxes(title_text = curr_metric)

    # Return default range when datepicker is empty/cleared
    elif start is None and end is None:
        if md_df[md_df['metric_name'] == curr_metric]['is_computed'].values[0]:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=cm_df['Date'], y=cm_df[curr_metric],
                    mode='lines', line = dict(color = "#0a275c")))
        else:
            fig = go.Figure()
            plot_basic_metrics(fig, bm_df, curr_metric)
        fig.update_layout(
            xaxis=default
        )
        fig.update_yaxes(title_text = curr_metric)
    fig.update_traces(hovertemplate='Date: %{x} <br>Value: %{y}')
    fig.update_xaxes(title_text = "Date")
    fig.update_layout(plot_bgcolor='white')
    fig.update_xaxes(rangeselector_font_size = 15)
    
    return fig

# Update graph title and description
@app.callback(
    Output('graph-title', 'children'),
    Output('metric-desc', "children"),
    Output('metric-formula', 'children'),
    Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'),
    State('md-data', 'data')
)
def update_title_desc(n_clicks_list, md_data):
    md_df = pd.DataFrame.from_dict(md_data)

    if not ctx.triggered or 1 not in n_clicks_list:
        return "Price ($)", md_df[md_df['metric_name'] == 'Price ($)']['description'], md_df[md_df['metric_name'] == 'Price ($)']['formula']
    clicked_id = ctx.triggered_id.index
    return clicked_id, md_df[md_df['metric_name'] == clicked_id]['description'], md_df[md_df['metric_name'] == clicked_id]['formula']

# Toggling metric's description to be shown or not
@app.callback(
    Output("toast", "is_open"),
    Input("toast-toggle", "n_clicks"),
)
def open_toast(n):
    if n == 0:
        return False
    return True