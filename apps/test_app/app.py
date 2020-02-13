import flask
import dash
import dash_core_components as dcc
import dash_html_components as html

app = dash.Dash(
    __name__,  meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)

server = app.server

# main app def

app.layout = html.Div(
    className="row",
    children=[
        html.Div(),
        html.Div()
    ]
)

if __name__ == "__main__":
    app.run_server(debug=True, host='0.0.0.0')