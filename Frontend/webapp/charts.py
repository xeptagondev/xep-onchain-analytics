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

                # hy test graph output print msg - to delete
                html.Div(id='graph-test-print-msg'),

                # area for metric description
                html.Div([
                    html.P("Metric Description", style={'font-weight':'bold', 'textDecoration':'underline'}),
                    html.P(id='metric-desc', style={'width':'48vw'}),
                ]),

                dmc.Grid(
                    children=[
                        dmc.Col(html.Div("Select Price scale: "), span=2),
                        dmc.Col(html.Div(
                            [
                                dmc.SegmentedControl(
                                    id="yaxis-type",
                                    value="Log",
                                    radius=20,
                                    data=[
                                        {"value": "Log", "label": "Log"},
                                        {"value": "Linear", "label": "Linear"},
                                    ],
                                    color = "teal"
                                )
                            ]
                        ), span=2),
                    ],
                    # gutter="xs", 
                    justify='flex-end'
                ),

                # dmc.Stack(
                #     [
                #         html.Div("Select Price scale: ", style={"font-weight": "bold", "color":"teal"}),
                #         html.Div(
                #             [
                #                 dmc.SegmentedControl(
                #                     id="yaxis-type",
                #                     value="Log",
                #                     radius=20,
                #                     data=[
                #                         {"value": "Log", "label": "Log"},
                #                         {"value": "Linear", "label": "Linear"},
                #                     ],
                #                     color = "teal"
                #                 )
                #             ]
                #         )
                #     ],
                #     justify='flex-end',
                #     align='right'    
                # ),

                # html.Div(
                #     [
                #         html.P("Select Price data axis scale: "),
                #         dmc.SegmentedControl(
                #             id="yaxis-type",
                #             value="Log",
                #             radius=20,
                #             data=[
                #                 {"value": "Log", "label": "Log"},
                #                 {"value": "Linear", "label": "Linear"},
                #             ],
                #             color = "teal"
                #         )
                #     ]
                # ),

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
    if (n1 is None and n2 is None and n3 is None) or not ctx.triggered: # if dropdown button is not clicked
        # bm = basic_metrics.to_dict('records')
        # cm = computed_metrics.to_dict('records')
        # md = metrics_desc.to_dict('records')
        # return "Bitcoin (BTC)", bm, cm, md
        raise PreventUpdate
    else:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0] # "Bitcoin" or "Ethereum"
        bm = bm_dict[button_id].to_dict('records') # returns basic_metrics / bm_eth
        cm = cm_dict[button_id].to_dict('records') # returns computed_metrics / cm_eth
        md = md_dict[button_id].to_dict('records') # returns metrics_desc / md_eth
        return label_id[button_id], bm, cm, md # returns dropdown label: Bitcoin (BTC) / Ethereum (ETH) / Tether (USDT)


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

# Method to plot computed metrics
def plot_computed_metrics(fig, computed_metrics_df, basic_metrics_df, metric, price_axis_scale): # need basic_metrics_df to get price data
    fig.add_trace(go.Scatter(x=computed_metrics_df['Date'], y=computed_metrics_df[metric], yaxis='y1', name=metric, mode='lines', line = dict(color = "#3d90e3"))) # original colour - #0a275c
    fig.add_trace(go.Scatter(x=computed_metrics_df['Date'], y=basic_metrics_df["Price ($)"], yaxis='y2', name='Price ($)', mode='lines', line = dict(color = "#0a275c"))) # plotting price data on same chart
    
    fig.update_layout(
    # create first y axis
    yaxis=dict(title=metric),

    # Create second y axis
    yaxis2=dict(title="Price ($)",
                overlaying="y",
                side="right",
                type='log' if price_axis_scale == 'Log' else 'linear'), # to use log scale by default,
    
    legend=dict(orientation="h", yanchor="top", y=1.02)
    )
    return fig

# Method to plot basic metrics
def plot_basic_metrics(fig, df, metric, price_axis_scale):
    if metric == "Price ($)":
        fig.add_trace(go.Scatter(x=df['Date'], y=df[metric], yaxis='y1', name=metric, mode='lines', line = dict(color = "#3d90e3")))
        fig.update_yaxes(title_text = metric, type='log' if price_axis_scale == 'Log' else 'linear')

    else:
        # plot other metrics with Price data added 
        fig.add_trace(go.Scatter(x=df['Date'], y=df[metric], yaxis='y1', name=metric, mode='lines', line = dict(color = "#3d90e3")))
        fig.add_trace(go.Scatter(x=df['Date'], y=df["Price ($)"], yaxis='y2', name='Price ($)', mode='lines', line = dict(color = "#0a275c"))) # plotting price data on same chart

        if (metric == "Difficulty"):
            rolling_window = [14, 25, 40, 60, 90, 128, 200]
            for i in rolling_window:
                rolling_mean = df["Difficulty"].rolling(window=i).mean()
                trace = go.Scatter(x = df['Date'], y=rolling_mean, mode='lines', line = dict(color = "rgba(255, 0, 0, 0.5)"), name = "D{}".format(i))
                fig.add_trace(trace)

        fig.update_layout(
        # create first y axis for selected metric
        yaxis=dict(title=metric),

        # Create second y axis for Price
        yaxis2=dict(title="Price ($)",
                    overlaying="y",
                    side="right",
                    type='log' if price_axis_scale == 'Log' else 'linear'), # to use log scale by default

        legend=dict(orientation="h", yanchor="top", y=1.02)
        )

        return fig

# Update line graph data
@app.callback(
    Output("analytics-graph", "figure"), # updating figure property of the dcc.Graph compoent to display new data
    Output('graph-test-print-msg', 'children'), # hy added to see which part of update_line_charts is producing the chart -- to delete
    Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'), # n_clicks_list
    Input('my-date-picker-range', 'value'), # dates
    Input('graph-title', 'children'), # curr_metric
    Input({'type': 'list-group-item', 'index': ALL}, 'id'), # id_list
    Input('yaxis-type', 'value'), # price_axis_scale - linear / log scale for price data
    State('bm-data', 'data'),
    State('cm-data', 'data'),
    State('md-data', 'data'),
)
def update_line_chart(n_clicks_list, dates, curr_metric, id_list, price_axis_scale, bm_data, cm_data, md_data):
    bm_df = pd.DataFrame.from_dict(bm_data)
    cm_df = pd.DataFrame.from_dict(cm_data)
    md_df = pd.DataFrame.from_dict(md_data)

    if dates is None:
        start = None
        end = None
    else:
        start, end = dates

    print_msg = html.Div("testing first ah") # initialising value

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
    if ctx.triggered_id is None or 1 not in n_clicks_list: # if user did not select any metric, show Price chart by default
        print_msg = "chart generated as triggered_id is None or 1 not in n_clicks_list"
        plot_basic_metrics(fig, bm_df, "Price ($)", price_axis_scale)
        
    elif ctx.triggered_id is not None or 1 in n_clicks_list: # if user clicked on a metric
        clicked = id_list[n_clicks_list.index(1)]['index'] # get metric name of the very first metric that user clicked on 
        is_computed = md_df[md_df['metric_name'] == clicked]['is_computed'].values[0]
        if clicked == "Difficulty Ribbon":
            clicked = "Difficulty"
        if is_computed: # if selected metric is a computed one
            plot_computed_metrics(fig, cm_df, bm_df, clicked, price_axis_scale)
        else: # selected metric is not a computed one - basic metric
            print_msg = "chart generated as this is the first metric user clicked on && user selected BASIC metric"
            plot_basic_metrics(fig, bm_df, clicked, price_axis_scale)

    # Update graph based on date range selected by user
    if start is not None and end is not None:
        is_computed = md_df[md_df['metric_name'] == curr_metric]['is_computed'].values[0]
        if curr_metric == "Difficulty Ribbon":
            curr_metric = "Difficulty"
        if is_computed: # computed metric
            print_msg = "chart generated as start is Not None and end is not None && user selected COMPUTED metric"
            filtered_df = cm_df[cm_df['Date'].between(start, end)]
            fig = go.Figure()
            plot_computed_metrics(fig, filtered_df, bm_df, curr_metric, price_axis_scale) # bm_df is for price data
        else: # basic metrics
            print_msg = "chart generated as start is Not None and end is not None && user selected BASIC metric"
            filtered_df = bm_df[bm_df['Date'].between(start, end)]
            fig = go.Figure()
            plot_basic_metrics(fig, filtered_df, curr_metric, price_axis_scale)
        fig.update_xaxes(rangeslider=dict(
                            visible=True,
                            bgcolor="#d0e0e5",
                            thickness=0.1))

    # Return default range when datepicker is empty/cleared
    elif start is None and end is None:
        is_computed = md_df[md_df['metric_name'] == curr_metric]['is_computed'].values[0]
        if curr_metric == "Difficulty Ribbon":
            curr_metric = "Difficulty"
        if is_computed: # computed metric
            print_msg = "chart generated from where date filter is empty (start & end is None) && selected metric is a COMPUTED metric"
            fig = go.Figure()
            plot_computed_metrics(fig, cm_df, bm_df, curr_metric, price_axis_scale)
        else: # basic metric
            print_msg = "chart generated from where date filter is empty (start & end is None) && selected metric is a BASIC metric"
            fig = go.Figure()
            plot_basic_metrics(fig, bm_df, curr_metric, price_axis_scale)
        fig.update_layout(
            xaxis=default
        )
    fig.update_traces(hovertemplate='Date: %{x} <br>Value: %{y}')
    fig.update_xaxes(title_text = "Date")
    fig.update_layout(plot_bgcolor='white')
    fig.update_xaxes(rangeselector_font_size = 15)
    
    return fig, print_msg

# Update graph title and description
@app.callback(
    Output('graph-title', 'children'),
    Output('metric-desc', "children"),
    Output('metric-formula', 'children'),
    Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'), # n_clicks_list
    State('md-data', 'data')
)
def update_title_desc(n_clicks_list, md_data):
    md_df = pd.DataFrame.from_dict(md_data)

    if not ctx.triggered: # no user selection yet
        return "Price ($)", md_df[md_df['metric_name'] == 'Price ($)']['description'], md_df[md_df['metric_name'] == 'Price ($)']['formula'] # return price by default
    clicked_id = ctx.triggered_id.index # metric name that was clicked
    return clicked_id, md_df[md_df['metric_name'] == clicked_id]['description'], md_df[md_df['metric_name'] == clicked_id]['formula']

# Toggling metric's formula to be shown or not
@app.callback(
    Output("toast", "is_open"),
    Input("toast-toggle", "n_clicks"),
)
def open_toast(n):
    if n == 0:
        return False
    return True