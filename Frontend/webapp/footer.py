from dash import html
import dash_bootstrap_components as dbc

def create_footer():
    footer = html.Div([
        html.Div([
                html.Span("© 2022 Xeptagon. All Rights Reserved.", style={'color':'#1a4855', 'font-weight':'bold', 'font-size':'12px', 'padding-right':'50px', 'padding-top':'30px'}),
                html.A("About Us", href = "https://www.xeptagon.com/", className='footer-link'),
                html.A("Our Github", href = "https://github.com/xeptagondev/xep-onchain-analytics", className='footer-link'),
                html.A("Contact Us", href = "mailto:info@xeptagon.com", className='footer-link'),
                html.Div([
                        html.A(className='bi bi-linkedin', href = "https://www.linkedin.com/company/xeptagon/", style={'color':'#0c2127', 'padding-right':'15px'}),
                        html.A(className='bi bi-facebook', href = "https://www.facebook.com/xeptagon", style={'color':'#0c2127', 'padding-right':'15px'}),
                        html.A(className="bi bi-twitter", href="https://twitter.com/xeptagon", style={'color':'#0c2127', 'padding-right':'15px'})
                ], style={'display':'flex', 'flex-direction':'row', 'justify-content':'center', 'padding-top':'27px'})
                
        ], style={'display':'flex', 'flex-direction':'row', 'justify-content':'center'})
    
    ], style={'position':'absolute', 'bottom':0, 'height':'75px', 'width':'100%', 'background-color':'#dee9ed'})

    return footer