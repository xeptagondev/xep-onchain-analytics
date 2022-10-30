from dash import html
import dash_bootstrap_components as dbc

def create_footer():
    footer = html.Div([
        html.Div([
                html.Span("© 2022 Xeptagon. All Rights Reserved.", style={'color':'#1a4855', 'font-weight':'bold', 'font-size':'12px', 'padding-right':'15px'}),
                html.A("About Us", href = "https://www.xeptagon.com/", className='footer-link'),
                html.A("Our Github", href = "https://github.com/xeptagondev/xep-onchain-analytics", className='footer-link'),
                html.A("info@xeptagon.com", href = "mailto:info@xeptagon.com", className='footer-contact'),
                html.Div([
                        html.A(className='bi bi-linkedin', href = "https://www.linkedin.com/company/xeptagon/", style={'color':'#0c2127', 'padding-right':'15px'}),
                        html.A(className='bi bi-facebook', href = "https://www.facebook.com/xeptagon", style={'color':'#0c2127', 'padding-right':'15px'}),
                        html.A(className="bi bi-twitter", href="https://twitter.com/xeptagon", style={'color':'#0c2127', 'padding-right':'15px'})
                ], style={'display':'flex', 'flex-direction':'row', 'justify-content':'center', 'height':'16px'})
                
        ], style={'display':'flex', 'flex-direction':'row', 'justify-content':'center', 'height':'16px', 'padding-top':'30px'})



            # dbc.Row([
            #     # 'About Us' column
            #     dbc.Col([
            #         html.P("About Us", style = {'color':'#c7e7ed', 'font-weight':'bold', 'font-size':'16px'}),
            #         # xeptagon company's website
            #         html.A("Visit Our Website", href = "https://www.xeptagon.com/", className='footer-link'),
                    
            #     ], width = 2, style = {'text-align':'center', 'padding-top':'15px'}),

            #     # 'Resources' column
            #     dbc.Col([
            #         html.P("Resources", style = {'color':'#c7e7ed', 'font-weight':'bold', 'font-size':'16px'}),
            #         # Blockchair data dumps
            #         html.A("Data Sources", href = "https://blockchair.com/dumps#database", className='footer-link'),
            #         # Github repository for contributions
            #         html.A("Contribute to our Github", href = "https://github.com/xeptagondev/xep-onchain-analytics", className='footer-link'),
                
            #     ], width = 2, style = {'text-align':'center', 'padding-top':'15px'}),

            #     # 'Contact Us' column
            #     dbc.Col([
            #         html.P("Contact Us", style = {'color':'#c7e7ed', 'font-weight':'bold', 'font-size':'16px'}),
            #         # email address
            #         html.Div([
            #             html.P("Email : "),
            #             html.A("info@xeptagon.com", href = "mailto:info@xeptagon.com", className='footer-contact')
            #         ], style={'display':'flex', 'flex-direction':'row', 'color':'white', 'justify-content':'center', 'font-size':'13px', 'height':'18px'}),
            #         # contact number
            #         html.Div([
            #             html.P("Phone : "),
            #             html.A("+94 72 353 4333", href = "tel:94723534333", className='footer-contact')
            #         ], style={'display':'flex', 'flex-direction':'row', 'color':'white', 'justify-content':'center', 'font-size':'13px', 'height':'18px'}),
            #         # socials
            #         html.Div([
            #             html.A(className='bi bi-linkedin', href = "https://www.linkedin.com/company/xeptagon/", style={'color':'#c7e7ed', 'padding-right':'10px'}),
            #             html.A(className='bi bi-facebook', href = "https://www.facebook.com/xeptagon", style={'color':'#c7e7ed', 'padding-right':'10px'}),
            #             html.A(className="bi bi-twitter", href="https://twitter.com/xeptagon", style={'color':'#c7e7ed', 'padding-right':'10px'})
            #         ], style={'display':'flex', 'flex-direction':'row', 'justify-content':'center', 'height':'16px'})
                
            #     ], width = 2, style = {'text-align':'center', 'padding-top':'15px'})

            # ], justify = 'evenly')
    
    ], style={'position':'absolute', 'bottom':0, 'height':'75px', 'width':'100%', 'background-color':'#dee9ed'})

    return footer