# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import json
import pandas as pd
import datetime
import sys
sys.path.append("C:/Users/Administrator/Desktop/syc/QUANT/")
reload(sys)
from tools import get_tradeDay, client_db

print(dcc.__version__)  # 0.6.0 or above is required

app = dash.Dash()

DF_SIMPLE = pd.DataFrame({
    'x': ['A', 'B', 'C', 'D', 'E', 'F'],
    'y': [4, 3, 1, 2, 3, 6],
    'z': ['a', 'b', 'c', 'a', 'b', 'c']
})


# Since we're adding callbacks to elements that don't exist in the app.layout,
# Dash will raise an exception to warn us that we might be
# doing something wrong.
# In this case, we're adding the elements through a callback, so we can ignore
# the exception.
app.config.supress_callback_exceptions = True
app.scripts.config.serve_locally = True
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content'),
    html.Div(dt.DataTable(rows=[{}]), style={'display': 'none'})
])

index_page = html.Div([
    html.H1(u'量化投资二部-因子查询系统'),
    html.Br(),
    dcc.Link('Go to Home', href='/'),
    html.Br(),
    dcc.Link('Go to Page Summary', href='/page-1'),
    html.Br(),
    dcc.Link('Go to Page Query', href='/page-2')
])



def top_bottom(input1, input2, flag):
    # input2 = '20180108'
    # input1 = 'ESG100'
    # flag = False  # top
    sql = "select S_INFO_CODE,S_INFO_NAME from AShareDescription"

    get_db = client_db.read_db()
    co_name = get_db.db_query(sql)
    co_name['S_INFO_NAME'] = co_name['S_INFO_NAME'].apply(lambda x: x.decode('gbk'))
    co_name['S_INFO_CODE'] = co_name['S_INFO_CODE'].apply(lambda x: str(x) + '-CN')
    co_name.columns = ['stock_code', 'stock_name']


    day = datetime.datetime.strptime(input2, '%Y%m%d')
    pre_day = day - datetime.timedelta(days=20)
    day = day.strftime('%Y%m%d')
    pre_day = pre_day.strftime('%Y%m%d')

    trade_day = get_tradeDay.wind(pre_day, day, fre='day')
    pre_day = trade_day[trade_day < day].iloc[-1]

    df1 = pd.read_csv('Z:/daily_data/alpha/neut/alphaALL/alphaALL_neut_CN_%s.csv' % day, skiprows=3)
    df1.columns = [u'stock_code', u'alphaALL_T', u'alphaALL_ESG100_T', u'alphaALL_HS300_T',
                   u'alphaALL_ZZ500_T', u'Value_T', u'Quality_T', u'Revision_T', u'Fndsurp_T', u'IU_T',
                   u'Mktmmt_T', u'Insider_T', u'lincoef', u'sqrtcoef', u'UNIVERSE']
    df1 = df1[[u'stock_code', u'alphaALL_ESG100_T', u'alphaALL_ZZ500_T', u'Value_T', u'Quality_T', u'Revision_T',
               u'Fndsurp_T', u'IU_T', u'Mktmmt_T', u'Insider_T']]
    df2 = pd.read_csv('Z:/daily_data/alpha/neut/alphaALL/alphaALL_neut_CN_%s.csv' % pre_day, skiprows=3)
    df2.columns = [u'stock_code', u'alphaALL_T-1', u'alphaALL_ESG100_T-1', u'alphaALL_HS300_T-1',
                   u'alphaALL_ZZ500_T-1', u'Value_T-1', u'Quality_T-1', u'Revision_T-1', u'Fndsurp_T-1', u'IU_T-1',
                   u'Mktmmt_T-1', u'Insider_T-1', u'lincoef', u'sqrtcoef', u'UNIVERSE']
    df2 = df2[[u'stock_code', u'alphaALL_ESG100_T-1', u'alphaALL_ZZ500_T-1', u'Value_T-1', u'Quality_T-1', u'Revision_T-1',
               u'Fndsurp_T-1', u'IU_T-1', u'Mktmmt_T-1', u'Insider_T-1']]

    df = pd.merge(df1, df2, on='stock_code')
    # df = df[['stock_code','alphaALL_%s_T'%input2,'alphaALL_%s_T-1'%input2]]
    df = df[['stock_code', 'alphaALL_%s_T' % input1, 'alphaALL_%s_T-1' % input1, u'Value_T', u'Value_T-1', u'Quality_T',
             u'Quality_T-1',
             u'Revision_T', u'Revision_T-1', u'Fndsurp_T', u'Fndsurp_T-1', u'IU_T', u'IU_T-1',
             u'Mktmmt_T', u'Mktmmt_T-1', u'Insider_T', u'Insider_T-1']]

    df['delta'] = df['alphaALL_%s_T' % input1] - df['alphaALL_%s_T-1' % input1]

    df = df.sort_values(by='delta', ascending=flag)
    df = df.iloc[:20]
    df =df.round(3)
    df = pd.merge(df,co_name,how='left', on = 'stock_code')
    df = df[['stock_code', 'stock_name', 'alphaALL_%s_T' % input1, 'alphaALL_%s_T-1' % input1, 'delta', u'Value_T', u'Value_T-1', u'Quality_T',
             u'Quality_T-1',
             u'Revision_T', u'Revision_T-1', u'Fndsurp_T', u'Fndsurp_T-1', u'IU_T', u'IU_T-1',
             u'Mktmmt_T', u'Mktmmt_T-1', u'Insider_T', u'Insider_T-1']]
    return df

@app.callback(
    dash.dependencies.Output('datatable-top', 'children'),
    [dash.dependencies.Input('submit-button2', 'n_clicks'),
     dash.dependencies.Input('xaxis-type', 'value')],
    [dash.dependencies.State('input-1-state2', 'value')])
def generate_top(n_clicks, input1, input2):
    dataframe = top_bottom(input1, input2, False)

    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(len(dataframe))]
    )

@app.callback(
    dash.dependencies.Output('datatable-bottom', 'children'),
    [dash.dependencies.Input('submit-button2', 'n_clicks'),
     dash.dependencies.Input('xaxis-type', 'value')],
    [dash.dependencies.State('input-1-state2', 'value')])
def generate_bottom(n_clicks, input1, input2):
    dataframe = top_bottom(input1, input2, True)

    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(len(dataframe))]
    )

page_1_layout = html.Div([
    html.H1('delta Alpha'),
    html.Br(),
    dcc.Link('Go back to home', href='/'),
    html.Br(),
    dcc.Input(id='input-1-state2', type='text', value='', placeholder='Enter date'),
    html.Button(id='submit-button2', n_clicks=0, children='Submit'),
    html.Br(),
    dcc.RadioItems(
                id='xaxis-type',
                options=[{'label': i, 'value': i} for i in ['ESG100', 'ZZ500']],
                value='ESG100',
                labelStyle={'display': 'inline-block'}
            ),
    html.H4('delta Alpha Top 20'),
    html.Table(id='datatable-top'),
    html.Br(),
    html.H4('delta Alpha Bottom 20'),
    html.Table(id='datatable-bottom')

])


def data_select(input1, input2, input3):
    trade_day = get_tradeDay.wind(input1, input2, fre='day')
    df_alpha = pd.DataFrame([])
    for i in trade_day:
        df = pd.read_csv('Z:/daily_data/alpha/neut/alphaALL/alphaALL_neut_CN_%s.csv' % i, skiprows=3)
        df.columns = [u'name', u'alphaALL', u'alphaALL_ESG100', u'alphaALL_HS300',
                             u'alphaALL_ZZ500', u'Value', u'Quality', u'Revision', u'Fndsurp', u'IU',
                             u'Mktmmt', u'Insider',u'lincoef',u'sqrtcoef',u'UNIVERSE']

        df['Date'] = i
        df_alpha = pd.concat([df_alpha, df])
    col = [u'name', u'Date', u'alphaALL', u'alphaALL_ESG100', u'alphaALL_HS300',
                         u'alphaALL_ZZ500', u'Value', u'Quality', u'Revision', u'Fndsurp', u'IU',
                         u'Mktmmt', u'Insider']
    df_alpha = df_alpha[col]
    df_alpha = df_alpha.sort_values(by=['name', 'Date'])
    df_alpha = df_alpha.round(3)
    code_list = input3.split(',')
    code_list = list(map(lambda x: str(x)+'-CN', code_list))
    if code_list[0] != 'ALL':
        df_alpha = df_alpha[df_alpha.name.isin(code_list)]
    return df_alpha


@app.callback(
    dash.dependencies.Output('datatable', 'children'),
    [dash.dependencies.Input('submit-button', 'n_clicks')],
    [dash.dependencies.State('input-1-state', 'value'),
     dash.dependencies.State('input-2-state', 'value'),
     dash.dependencies.State('input-3-state', 'value')])
def generate_table(n_clicks, input1, input2,input3):
    dataframe = data_select(input1, input2, input3)

    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(len(dataframe))]
    )

    # return html.Iframe(srcDoc=dataframe.to_html())

#####################################
#####################################
page_2_layout = html.Div([
    html.H1('Query Alpha'),
    html.Br(),
    dcc.Link('Go back to home', href='/'),
    html.Br(),
    dcc.Input(id='input-1-state', type='text', value='', placeholder='Enter start date'),
    dcc.Input(id='input-2-state', type='text', value='', placeholder='Enter end date'),
    dcc.Input(id='input-3-state', type='text', value='', placeholder='Enter stock code'),
    html.Button(id='submit-button', n_clicks=0, children='Submit'),
    html.H4('DataTable Alpha'),
    # dt.DataTable(
    #     rows=df2.to_dict('records'),
    #     # rows = generate_table(df2),
    #     # # optional - sets the order of columns
    #     columns=dataframe.columns,
    #
    #     id='datatable'
    # )
    html.Table(id='datatable')
    # html.Table(id='datatable')
])


# @app.callback(
#     dash.dependencies.Output('output', 'children'),
#     [dash.dependencies.Input('editable-table', 'rows')])
# def update_selected_row_indices(rows):
#     return json.dumps(rows, indent=2)
#
#
# @app.callback(
#     dash.dependencies.Output('graph', 'figure'),
#     [dash.dependencies.Input('editable-table', 'rows')])
# def update_figure(rows):
#     dff = pd.DataFrame(rows)
#     return {
#         'data': [{
#             'x': dff['x'],
#             'y': dff['y'],
#         }],
#         'layout': {
#             'margin': {'l': 10, 'r': 0, 't': 10, 'b': 20}
#         }
#     }


# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/page-1':
        return page_1_layout
    elif pathname == '/page-2':
        return page_2_layout
    else:
        return index_page
        # You could also return a 404 "URL not found" page here




app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

if __name__ == '__main__':
    app.run_server(host='10.180.10.92',port=8011,debug=True)