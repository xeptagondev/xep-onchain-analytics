from dash import html, dcc
from dash.dependencies import Input, Output
from home import create_page_home
from charts import create_charts
from anomaly import create_page_3
from app import app

server = app.server
app.config.suppress_callback_exceptions = True

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])

def display_page(pathname):
    if pathname == '/charts':
        return create_charts()
    if pathname == '/anomaly':
        return create_page_3()
    else:
        return create_page_home()


if __name__ == '__main__':
    app.run_server(debug=False)
