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
from charts_data import get_bm_dict, get_cm_dict, get_md_dict

dash.register_page(__name__, path='/analytics', name="Analytics")
nav = create_navbar()
footer = create_footer()

bm_dict = get_bm_dict()
cm_dict = get_cm_dict()
md_dict = get_md_dict()
default_crypto = 'Bitcoin'


def show_metrics_list(idx, metric_description_df):
    '''Returns a sorted list of metrics for the respective cryptocurrencies.'''
    return html.Div(
        [
            dbc.ListGroupItem(
                sorted(metric_description_df['metric_name'].tolist())[
                    int(idx)],  # metric_name
                action=True,
                id={"type": "list-group-item",
                    "index": sorted(metric_description_df['metric_name'].tolist())[int(idx)]},
                color='#E8EBEE00',
                style={'font-size': '13px'},
                # class_name = "border-0"
            ),
            dbc.Tooltip(
                metric_description_df[metric_description_df['metric_name'] == sorted(
                    metric_description_df['metric_name'].tolist())[int(idx)]]['description'],
                id="tooltip" + idx,
                target={"type": "list-group-item", "index": sorted(
                    metric_description_df['metric_name'].tolist())[int(idx)]}
            )
        ]
    )


content = html.Div([

    dcc.Store(id='bm-data', storage_type='memory',
              data=bm_dict[default_crypto].to_dict('records')),
    dcc.Store(id='cm-data', storage_type='memory',
              data=cm_dict[default_crypto].to_dict('records')),
    dcc.Store(id='md-data', storage_type='memory',
              data=md_dict[default_crypto].to_dict('records')),


    dbc.Row([
            # Control panel column
            dbc.Col([

                # Select Cryptocurrency
                dbc.Row([
                    html.P(" Select Cryptocurrency", className='bi bi-coin', style={
                           'color': 'black', 'text-align': 'center', 'font-size': '15px', 'font-family': 'Open Sans', 'font-weight': 'bold'}),
                    dbc.DropdownMenu(
                        [dbc.DropdownMenuItem("Bitcoin (BTC)", id="Bitcoin"),
                         dbc.DropdownMenuItem(divider=True),
                         dbc.DropdownMenuItem("Ethereum (ETH)", id="Ethereum"),
                         dbc.DropdownMenuItem(divider=True),
                         html.Div([
                             html.Span("to be implemented in future",
                                       className='disabled-info'),
                             dbc.DropdownMenuItem(
                                 "Tether (USDT)", id="Tether", disabled=True),
                         ], className='disabled-coin-2'),
                         ],
                        id='cryptocurrency-select',
                        label='Bitcoin (BTC)',
                        color='#0d1e26',
                        toggle_style={'text-align': 'center', 'font-size': '13px', 'width': '160px',
                                      'height': '35px', 'color': 'white', 'font-family': 'Open Sans'}
                    )
                ], style={'text-align': 'center', 'padding-bottom': '15px'}),

                # Search Metrics
                dbc.Row([
                    dbc.Input(
                        id='searchbar',
                        placeholder='Search Metric',
                        type='search',
                        style={'text-align': 'center', 'width': '200px', 'height': '35px',
                               'border': '1px solid black', 'font-size': '13px', 'font-family': 'Open Sans'}
                    ),

                    dbc.ListGroup([show_metrics_list(f"{i}", md_dict[default_crypto]) for i in range(len(md_dict[default_crypto]))],
                                  id='list-group',
                                  flush=True,
                                  style={'margin-top': '15px', 'overflow-y': 'scroll', 'width': '350px', 'height': '450px'})

                ], justify='center', style={'padding': '25px', 'border-top': '2px solid grey'}),

            ], width=3, style={'background-color': '#E8EBEE99',  'border-right': '2px solid grey', 'padding-top': '20px'}),

            # Display column
            dbc.Col([
                html.Div([
                    html.H5(id='graph-title', style={'display': 'inline-block',
                            'vertical-align': 'middle', 'margin': '10px 0', 'color': '#0a275c'}),
                    dbc.Button(id='toast-toggle', n_clicks=0, className="bi bi-question-circle rounded-circle",
                               color='white', style={'display': 'inline-block', 'vertical-align': 'middle', 'margin': '10px 0'}),
                    html.Div([
                        html.Span("Select your preferred date range.",
                                  className='date-picker-text', style={'font-size': '12px'}),
                        dmc.DateRangePicker(
                            id='my-date-picker-range',
                            placeholder='DD/MM/YYYY - DD/MM/YYYY',
                            inputFormat="DD/MM/YYYY",
                            clearable=True,
                            minDate=date(2009, 1, 3),
                            maxDate=datetime.now(),
                            style={'width': 280}
                        ),
                    ], className='date-picker-div', style={'display': 'inline-block', 'position': 'relative', 'float': 'right', 'margin-top': '13px'})
                ]),
                # area for toggable toast to display metric's formula
                html.Div(
                    dbc.Toast(
                        [dcc.Markdown(mathjax=True, id='metric-formula')],
                        id="toast",
                        header="How is it Calculated?",
                        dismissable=True,
                        is_open=False,
                        style={'width': '40vw'}
                    ), style={'padding-top': '15px', 'padding-bottom': '15px'}
                ),

                # area for metric description
                html.Div([
                    html.P("Metric Description", style={
                           'font-weight': 'bold', 'textDecoration': 'underline'}),
                    html.P(id='metric-desc', style={'width': '48vw'}),
                ]),

                dbc.Row([
                    html.Div(id="scale-dropdown", children=[
                        html.Span("Select your preferred scale.",
                                  className='scale-dropdown-text', style={'font-size': '12px'}),
                        dbc.DropdownMenu(
                            [dbc.DropdownMenuItem(
                                html.Div([
                                    html.Div(['Price'], style={
                                         'textAlign': 'center', 'font-weight': 'bold'}),
                                    dmc.SegmentedControl(
                                        id="yaxis-type",
                                        value="Log",
                                        radius=10,
                                        data=[
                                            {"value": "Log", "label": "Log"},
                                            {"value": "Linear", "label": "Linear"},
                                        ],
                                        color="teal"
                                    )], style={'display': 'flex', 'flex-direction': 'column', 'align-items': 'center', 'justify-content': 'center'}), id='price-scale', toggle=False),
                                dbc.DropdownMenuItem(divider=True),
                                dbc.DropdownMenuItem(
                                html.Div([
                                    html.Div(['Metric'], style={
                                         'textAlign': 'center', 'font-weight': 'bold'}),
                                    dmc.SegmentedControl(
                                        id="yaxis-type-2",
                                        value="Linear",
                                        radius=10,
                                        data=[
                                            {"value": "Log", "label": "Log"},
                                            {"value": "Linear", "label": "Linear"},
                                        ],
                                        color="teal"
                                    )], style={'display': 'flex', 'flex-direction': 'column', 'align-items': 'center', 'justify-content': 'center'}),
                                id='metric-scale', toggle=False)],
                            id='scale-select',
                            label='Select Scale',
                            color='white',
                            toggle_style={'text-align': 'center', 'font-size': '13px', 'height': '35px',
                                          'color': '#bcc4cb', 'font-family': 'Open Sans', 'border-color': '#bcc4cb'}
                        )
                    ], className='scale-dropdown-div', style={'display': 'block', 'float': 'right', 'width': '100px'}), ], style={'justify-content': 'right', 'padding-bottom': '5px', 'padding-right': '45px'}),

                # area to display selected metric's graph
                dcc.Loading(
                    dcc.Graph(id="analytics-graph", style={'height': '80vh'}),
                    color='#0a275c'
                )

            ], width=9, style={'padding-right': '40px', 'padding-left': '30px', 'padding-top': '20px'})

            ], justify='evenly', style={'border-top': '2px solid grey', 'border-bottom': '1px solid grey'})
], style={'padding-bottom': '60px'})


def create_charts():
    '''Returns the layout of the page comprising of navigation bar, page content and footer sections.'''
    layout = html.Div([
        nav,
        content,
        footer
    ], style={'min-height': '100%', 'position': 'relative', 'overflow-x': 'hidden'})
    return layout

################## Callbacks ##############################

# Update dropdown label


@app.callback(
    Output('cryptocurrency-select', "label"),
    Output('bm-data', "data"),
    Output('cm-data', "data"),
    Output('md-data', "data"),
    [Input("Bitcoin", "n_clicks"), Input(
        "Ethereum", "n_clicks"), Input("Tether", "n_clicks")]
)
def update_dropdown(n1, n2, n3):
    '''Returns the updated dropdown button label.'''
    label_id = {
        "Bitcoin": "Bitcoin (BTC)", "Ethereum": "Ethereum (ETH)", "Tether": "Tether (USDT)"}
    if (n1 is None and n2 is None and n3 is None) or not ctx.triggered:  # if dropdown button is not clicked
        # bm = basic_metrics.to_dict('records')
        # cm = computed_metrics.to_dict('records')
        # md = metrics_desc.to_dict('records')
        # return "Bitcoin (BTC)", bm, cm, md
        raise PreventUpdate
    else:
        button_id = ctx.triggered[0]["prop_id"].split(
            ".")[0]  # "Bitcoin" or "Ethereum"
        # returns basic_metrics / bm_eth
        bm = bm_dict[button_id].to_dict('records')
        # returns computed_metrics / cm_eth
        cm = cm_dict[button_id].to_dict('records')
        # returns metrics_desc / md_eth
        md = md_dict[button_id].to_dict('records')
        # returns dropdown label: Bitcoin (BTC) / Ethereum (ETH) / Tether (USDT)
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
    '''Returns an updated metrics list when user enters text in the search bar under the Analytics tab or when user switches between cryptocurrencies on the Analytics tab.'''
    names_df = pd.DataFrame.from_dict(md_data)

    triggered_comp = ctx.triggered_id
    if triggered_comp == 'searchbar':

        if searchterm == "":  # when search bar is cleared
            return [show_metrics_list(f"{i}", names_df) for i in range(len(names_df))]
        result = names_df['metric_name'].tolist().copy()
        for word in searchterm.split(" "):
            count = 0
            while count < len(result):
                if word.lower() not in result[count].lower():
                    result.remove(result[count])
                else:
                    count += 1
        metric_desc_filtered = names_df[names_df['metric_name'].isin(result)]
        return [show_metrics_list(f"{i}", metric_desc_filtered) for i in range(len(metric_desc_filtered))]

    else:
        return [show_metrics_list(f"{i}", names_df) for i in range(len(names_df))]

# Method to plot metrics computed using multiple columns


def plot_multi_column_metrics(fig, computed_metrics_df, metrics_desc_df, metric):
    '''Returns an updated figure for metrics requiring data from multiple columns and outputting the corresponding (multiple) lines.'''
    to_search = metrics_desc_df[metrics_desc_df['metric_name']
                                == metric]['search_string'].values[0]

    cols = [c for c in computed_metrics_df.columns if c.startswith(to_search)]
    for c in cols:
        if 'Total' in c:
            fig.add_trace(go.Scatter(
                x=computed_metrics_df['Date'], y=computed_metrics_df[c], yaxis='y1', name=c, mode='lines', line=dict(color="#3d90e3")))
        else:
            fig.add_trace(go.Scatter(
                x=computed_metrics_df['Date'], y=computed_metrics_df[c], yaxis='y1', name=c, mode='lines', visible='legendonly'))
    return fig

# Method to plot computed metrics


# need basic_metrics_df to get price data
def plot_computed_metrics(fig, computed_metrics_df, basic_metrics_df, metrics_desc_df, metric, price_axis_scale, metric_axis_scale):
    '''Returns an updated line chart figure with the price data overlaid when users select a computed metric.'''
    need_multi_column = metrics_desc_df[metrics_desc_df['metric_name']
                                        == metric]['plot_using_multiple_columns'].values[0]

    if not need_multi_column:
        fig.add_trace(go.Scatter(x=computed_metrics_df['Date'], y=computed_metrics_df[metric], yaxis='y1',
                      name=metric, mode='lines', line=dict(color="#3d90e3")))  # original colour - #0a275c
    else:
        plot_multi_column_metrics(
            fig, computed_metrics_df, metrics_desc_df, metric)

    fig.add_trace(go.Scatter(x=computed_metrics_df['Date'], y=basic_metrics_df["Price ($)"], yaxis='y2',
                  name='Price ($)', mode='lines', line=dict(color="#0a275c")))  # plotting price data on same chart

    fig.update_layout(
        # create first y axis
        yaxis=dict(title=metric,
                   type='linear' if metric_axis_scale == 'Linear' else 'log'),

        # Create second y axis
        yaxis2=dict(title="Price ($)",
                    overlaying="y",
                    side="right",
                    type='log' if price_axis_scale == 'Log' else 'linear'),  # to use log scale by default,

        legend=dict(orientation="h", yanchor="top", y=1.35)
    )
    return fig

# Method to plot basic metrics


def plot_basic_metrics(fig, df, metric, price_axis_scale, metric_axis_scale):
    '''Returns an updated line chart figure with the price data overlaid when users select a basic metric.'''
    if metric == "Price ($)":
        fig.add_trace(go.Scatter(x=df['Date'], y=df[metric], yaxis='y1',
                      name=metric, mode='lines', line=dict(color="#3d90e3")))
        fig.update_yaxes(
            title_text=metric, type='linear' if metric_axis_scale == 'Linear' else 'log')

    else:
        if (metric == "Difficulty"):
            fig.add_trace(go.Scatter(x=df['Date'], y=df[metric], yaxis='y1',
                          name="Market", mode='lines', line=dict(color="#3d90e3")))

            rolling_window = [14, 25, 40, 60, 90, 128, 200]
            for i in rolling_window:
                rolling_mean = df["Difficulty"].rolling(window=i).mean()
                trace = go.Scatter(x=df['Date'], y=rolling_mean, mode='lines', line=dict(
                    color="rgba(255, 0, 0, 0.5)"), name="D{}".format(i))
                fig.add_trace(trace)

        else:
            # plot other metrics with Price data added
            fig.add_trace(go.Scatter(x=df['Date'], y=df[metric], yaxis='y1',
                          name=metric, mode='lines', line=dict(color="#3d90e3")))

        # plotting price data on same chart
        fig.add_trace(go.Scatter(x=df['Date'], y=df["Price ($)"], yaxis='y2',
                      name='Price ($)', mode='lines', line=dict(color="#0a275c")))

        fig.update_layout(
            # create first y axis for selected metric
            yaxis=dict(title=metric,
                       type='linear' if metric_axis_scale == 'Linear' else 'log'),

            # Create second y axis for Price
            yaxis2=dict(title="Price ($)",
                        overlaying="y",
                        side="right",
                        type='log' if price_axis_scale == 'Log' else 'linear'),  # to use log scale by default

            legend=dict(orientation="h", yanchor="top", y=1.35)
        )

        return fig

# Update line graph data


@app.callback(
    # updating figure property of the dcc.Graph compoent to display new data
    Output("analytics-graph", "figure"),
    # Input({'type': 'list-group-item', 'index': ALL}, 'n_clicks'), # n_clicks_list
    Input('my-date-picker-range', 'value'),  # dates
    Input('graph-title', 'children'),  # curr_metric
    # Input({'type': 'list-group-item', 'index': ALL}, 'id'), # id_list
    # price_axis_scale - linear / log scale for price data
    Input('yaxis-type', 'value'),
    # metric_axis_scale - linear / log scale for metric data
    Input('yaxis-type-2', 'value'),
    State('bm-data', 'data'),
    State('cm-data', 'data'),
    State('md-data', 'data'),
)
def update_line_chart(dates, curr_metric, price_axis_scale, metric_axis_scale, bm_data, cm_data, md_data):  # n_clicks_list, id_list
    '''
    Updates the line chart figure displayed under the Analytics tab.

            Parameters:
                    dates (date): The start and end dates from the date filter, if any.
                    curr_metric (string): The name of the metric currently selected.
                    price_axis_scale (string): The scale selected for price line chart. 
                    metric_axis_scale (string): The scale selected for the selected metric line chart.
                    bm_data (dataframe): The dataframe storing the basic metrics data.
                    cm_data (dataframe): The dataframe storing the computed metrics data.
                    md_data (dataframe): The dataframe for metrics description.

            Returns:
                    fig (plotly figure object): Plotly graph object for displaying line chart.
    '''
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
    # if ctx.triggered_id is None or 1 not in n_clicks_list: # if user did not select any metric, show Price chart by default
    #     plot_basic_metrics(fig, bm_df, "Price ($)", price_axis_scale, metric_axis_scale)

    # elif ctx.triggered_id is not None or 1 in n_clicks_list: # if user clicked on a metric
    #     clicked = id_list[n_clicks_list.index(1)]['index'] # get metric name of the very first metric that user clicked on
    #     is_computed = md_df[md_df['metric_name'] == clicked]['is_computed'].values[0]
    #     if clicked == "Difficulty Ribbon":
    #         clicked = "Difficulty"
    #     if is_computed: # if selected metric is a computed one
    #         plot_computed_metrics(fig, cm_df, bm_df, clicked, price_axis_scale, metric_axis_scale)
    #     else: # selected metric is not a computed one - basic metric
    #         plot_basic_metrics(fig, bm_df, clicked, price_axis_scale, metric_axis_scale)

    # Update graph based on date range selected by user
    if start is not None and end is not None:
        is_computed = md_df[md_df['metric_name'] ==
                            curr_metric]['is_computed'].values[0]
        if curr_metric == "Difficulty Ribbon":
            curr_metric = "Difficulty"
        if is_computed:  # computed metric
            filtered_df = cm_df[cm_df['Date'].between(start, end)]
            fig = go.Figure()
            plot_computed_metrics(fig, filtered_df, bm_df, md_df, curr_metric,
                                  price_axis_scale, metric_axis_scale)  # bm_df is for price data
        else:  # basic metrics
            filtered_df = bm_df[bm_df['Date'].between(start, end)]
            fig = go.Figure()
            plot_basic_metrics(fig, filtered_df, curr_metric,
                               price_axis_scale, metric_axis_scale)
        fig.update_xaxes(rangeslider=dict(
            visible=True,
            bgcolor="#d0e0e5",
            thickness=0.1))

    # Return default range when datepicker is empty/cleared
    elif start is None and end is None:
        is_computed = md_df[md_df['metric_name'] ==
                            curr_metric]['is_computed'].values[0]
        if curr_metric == "Difficulty Ribbon":
            curr_metric = "Difficulty"
        if is_computed:  # computed metric
            fig = go.Figure()
            plot_computed_metrics(
                fig, cm_df, bm_df, md_df, curr_metric, price_axis_scale, metric_axis_scale)
        else:  # basic metric
            fig = go.Figure()
            plot_basic_metrics(fig, bm_df, curr_metric,
                               price_axis_scale, metric_axis_scale)
        fig.update_layout(
            xaxis=default
        )
    fig.update_traces(hovertemplate='%{y}')
    fig.update_xaxes(title_text="Date")
    fig.update_layout(plot_bgcolor='white',
                      hovermode='x unified', hoverlabel=dict(namelength=-1))
    fig.update_xaxes(rangeselector_font_size=15)

    return fig

# Hide scale dropdown when price is selected


def disable_item(metric):
    '''Disables scale dropdown when the selected price is price.'''
    if metric == "Price ($)":
        return True
    else:
        return False

# Update graph title and description


@app.callback(
    Output('graph-title', 'children'),
    Output('metric-desc', "children"),
    Output('metric-formula', 'children'),
    Output('price-scale', 'disabled'),
    Output('yaxis-type', 'disabled'),
    Input({'type': 'list-group-item', 'index': ALL},
          'n_clicks'),  # n_clicks_list
    State('md-data', 'data')
)
def update_title_desc(n_clicks_list, md_data):
    '''Returns the updated metric name, metric description, metric formula, price scale and value of the disable property for the price toggle.'''
    md_df = pd.DataFrame.from_dict(md_data)

    # no user selection yet
    if not ctx.triggered or not any(x is not None for x in n_clicks_list):
        # return price by default
        return "Price ($)", md_df[md_df['metric_name'] == 'Price ($)']['description'], md_df[md_df['metric_name'] == 'Price ($)']['formula'], disable_item("Price ($)"), disable_item("Price ($)")
    clicked_id = ctx.triggered_id.index  # metric name that was clicked
    return clicked_id, md_df[md_df['metric_name'] == clicked_id]['description'], md_df[md_df['metric_name'] == clicked_id]['formula'], disable_item(clicked_id), disable_item(clicked_id)

# Toggling metric's formula to be shown or not


@app.callback(
    Output("toast", "is_open"),
    Input("toast-toggle", "n_clicks"),
)
def open_toast(n):
    '''Displays metric formula (if any) when user clicks on toast toggle else keeps it hidden.'''
    if n == 0:
        return False
    return True
