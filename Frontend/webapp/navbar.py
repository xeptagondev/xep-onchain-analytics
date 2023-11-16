import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, html
from dash_bootstrap_components._components.Container import Container

xeptagon_logo = "https://www.xeptagon.com/assets/images/logos/logo-transparent.svg"

nav = dbc.Nav(
    #[dbc.NavItem(dbc.NavLink(page['name'], href=page['path'])) for page in dash.page_registry]
    [
        dbc.NavItem(dbc.NavLink("Home", active='exact', href="/")),
        dbc.NavItem(dbc.NavLink("Analytics", active='exact', href="/analytics")),
        dbc.NavItem(dbc.NavLink("Anomaly Detection", active='exact', href="/anomaly")),
        dbc.NavItem(dbc.NavLink("Forecasting", active='exact', href = '/forecasting'))
    ]
)

def create_navbar():
    navbar = dbc.Navbar(
        dbc.Container(
            [
                html.A(
                # Use row and col to control vertical alignment of logo / brand
                    dbc.Row(
                        [
                            dbc.Col(html.Img(src=xeptagon_logo, height="60px")),
                        ],
                    ),
                    href="/",
                    style={"textDecoration": "none"},
                ),
                nav
            ],
        ),
        color= "white",
        dark=False,
        style = {'height': '100px'}   
    )

    return navbar


