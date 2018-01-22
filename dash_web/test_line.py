# -*- coding: utf-8 -*-


import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
from loremipsum import get_sentences
import numpy as np


import pandas as pd
from time import strftime, localtime
from datetime import datetime
from tools import get_tradeDay
import color


def barra_data(start, end):
    end = get_tradeDay.get_advance_day(end,1)

    path = 'Z:/MSCI/daily/CNE5%s.DFRT'%end[2:]

    df = pd.read_csv(path,skiprows= 1)
    del df['Unnamed: 44']
    col = df.columns.tolist()
    col = list(map(lambda x: x.strip(), col))
    df.columns = col

    df = df[(df['DATE']>=int(start)) &(df['DATE']<=int(end))]
    df.DATE = pd.to_datetime(df.DATE, format='%Y%m%d')
    df.DATE = df.DATE.apply(lambda x: x.strftime("%Y-%m-%d"))
    df = df.set_index('DATE')

    df = (df+1).cumprod()
    df = df.round(3)

    col = df.columns.tolist()
    col1 = col[:10]
    col2 = col[10:]
    return df, col1, col2

def splist(L, s):
    return [L[i:i + s][0] for i in range(len(L)) if i % s == 0]


def get_color(col):
    color_range = color.cnames

    color_range= sorted(color_range.values())

    n = len(color_range)/len(col)
    color_universe = splist(color_range,n)
    return color_universe



app = dash.Dash()

app.scripts.config.serve_locally = True

app.layout = html.Div([
    html.H1(u' barra 风险因子累计收益率'),
    html.Br(),
    dcc.Input(id='input-1-barra', type='text', value='', placeholder='Enter start date'),
    dcc.Input(id='input-2-barra', type='text', value='', placeholder='Enter end date'),
    html.Button(id='submit-barra', n_clicks=0, children='Submit'),
    html.H4(u'风格因子累计收益率'),
    html.Div(id='barra-output'),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.H4(u'行业因子累计收益率'),
    html.Div(id='barra-output2')
], style={
    'width': '80%',
    'fontFamily': 'Sans-Serif',
    'margin-left': 'auto',
    'margin-right': 'auto'
})



@app.callback(dash.dependencies.Output('barra-output', 'children'),
              [dash.dependencies.Input('submit-barra', 'n_clicks')],
              [dash.dependencies.State('input-1-barra', 'value'),
               dash.dependencies.State('input-2-barra', 'value')])
def display_content(n_clicks, start, end):
    if start == '':
        end = strftime("%Y%m%d", localtime())
        start = get_tradeDay.get_advance_day(end,30)
        end = get_tradeDay.get_advance_day(end,1)

    df, col1, col2 = barra_data(start, end)
    color_universe = get_color(col1)
    data = []
    x1 = df.index.tolist()
    for i in range(len(col1)):
        y1 = df[col1[i]].tolist()
        data.append({
            'x': x1,
            'y': y1,
            'name': col1[i],
            'marker': {
                'color': color_universe[i]
            },
            'type': 'line'
        })

    return html.Div([
        dcc.Graph(
            id='graph',
            figure={
                'data': data,
                'layout': {
                    'margin': {
                        'l': 30,
                        'r': 0,
                        'b': 30,
                        't': 0
                    },
                    'legend': {'x': -0.12, 'y': 1}
                }
            },
            style={'width': '100%','height': '100%', 'display': 'inline-block'}
        )
    ])


@app.callback(dash.dependencies.Output('barra-output2', 'children'),
              [dash.dependencies.Input('submit-barra', 'n_clicks')],
              [dash.dependencies.State('input-1-barra', 'value'),
               dash.dependencies.State('input-2-barra', 'value')])
def display_content2(n_clicks, start, end):
    if start == '':
        end = strftime("%Y%m%d", localtime())
        start = get_tradeDay.get_advance_day(end,30)
        end = get_tradeDay.get_advance_day(end,1)
    df, col1, col2 = barra_data(start, end)
    color_universe2 = get_color(col2)
    data = []
    x1 = df.index.tolist()
    for i in range(len(col2)):
        y1 = df[col2[i]].tolist()
        data.append({
            'x': x1,
            'y': y1,
            'name': col2[i],
            'marker': {
                'color': color_universe2[i]
            },
            'type': 'line'
        })

    return html.Div([
        dcc.Graph(
            id='graph2',
            figure={
                'data': data,
                'layout': {
                    'margin': {
                        'l': 30,
                        'r': 0,
                        'b': 30,
                        't': 0
                    },
                    'legend': {'x': -0.12, 'y': 1}
                }
            },
            style={'width': '100%','height': '100%', 'display': 'inline-block'}
        )
    ])



if __name__ == '__main__':
    app.run_server(debug=True)