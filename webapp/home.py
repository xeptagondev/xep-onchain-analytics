from dash import html
import dash_bootstrap_components as dbc
from app import app
from navbar import create_navbar

nav = create_navbar()

content = html.Div([
    dbc.Row([
        dbc.Col([
            html.H1("Blockchain Data made simplified.", 
                    style={'color': '#0a275c', 'font-size': '35px','font-family':'Open Sans'}),
            html.P("Get access to our consolidated metrics & insights of blockchain transactions at no additional cost.", 
                   style={'color':'#0a275c', 'font-size':'20px', 'margin-top': '20px'}),
            dbc.Button("Go to Analytics", id = 'go-chart-butt', href='/analytics',
                       style= {'background-color': '#0a275c', 'color':'white', 'text-transform':'none', 'font-weight': 900, 'border-radius':'15px', 'margin-top':'30px'})
        ], width=3, style = {'margin': 'auto', 'text-align':'center'}),
        
        dbc.Col(
            html.Img(src = app.get_asset_url('homepg-icon.png'), style={'width': '80%', 'height':'90%'}),
            width=6, style = {'padding-top': '100px', 'padding-left':'60px'}
        )
    ], justify='evenly', style = {'border-top': '2px solid grey'})

], style = {'position':'fixed', 'height': '100vh'})

def create_page_home():
    layout = html.Div([
        nav,
        content,
    ])
    return layout
