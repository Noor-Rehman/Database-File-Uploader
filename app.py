import dash
from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import sqlalchemy as sa
import pandas as pd
import io
import base64
import os

# Note: User needs to install required packages:
# pip install dash dash-bootstrap-components sqlalchemy pandas
# For MySQL: pip install pymysql
# For SQL Server: pip install pyodbc (and install ODBC Driver for SQL Server)
# For Oracle: pip install cx_Oracle (and install Oracle Instant Client)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container(
    [
        dbc.Row(
            dbc.Col(
                html.H1("Database File Uploader", className="text-center mb-4", style={"color": "#007bff"}),
                width=12
            )
        ),
        dbc.Row(
            dbc.Col(
                dbc.Card(
                    [
                        dbc.CardHeader("Step 1: Select Database Type", className="bg-primary text-white"),
                        dbc.CardBody(
                            dcc.Dropdown(
                                id="db-type",
                                options=[
                                    {"label": "MySQL", "value": "mysql"},
                                    {"label": "SQL Server", "value": "mssql"},
                                    {"label": "Oracle", "value": "oracle"},
                                ],
                                placeholder="Select DB Type",
                                className="mb-3"
                            )
                        ),
                    ],
                    className="shadow-lg"
                ),
                width=6, lg=4, className="mx-auto"
            )
        ),
        dbc.Row(
            dbc.Col(
                dbc.Card(
                    [
                        dbc.CardHeader("Step 2: Enter Connection Details", className="bg-info text-white"),
                        dbc.CardBody(
                            [
                                dbc.Input(id="host", placeholder="Host (e.g., localhost)", type="text", className="mb-2"),
                                dbc.Input(id="port", placeholder="Port (e.g., 3306 for MySQL)", type="text", className="mb-2"),
                                dbc.Input(id="username", placeholder="Username", type="text", className="mb-2"),
                                dbc.Input(id="password", placeholder="Password", type="password", className="mb-3"),
                                dbc.Button("Connect", id="connect-btn", color="success", className="w-100"),
                            ]
                        ),
                    ],
                    className="shadow-lg mt-4"
                ),
                width=6, lg=4, className="mx-auto"
            )
        ),
        dbc.Row(
            dbc.Col(
                dbc.Alert(id="connection-alert", is_open=False, dismissable=True, className="mt-3"),
                width=6, lg=4, className="mx-auto"
            )
        ),
        dbc.Row(
            dbc.Col(
                dbc.Card(
                    [
                        dbc.CardHeader("Step 3: Select Database", className="bg-warning text-dark"),
                        dbc.CardBody(
                            dcc.Dropdown(
                                id="db-name",
                                placeholder="Databases will appear after connecting",
                                className="mb-3"
                            )
                        ),
                    ],
                    className="shadow-lg mt-4",
                    id="db-card",
                    style={"display": "none"}
                ),
                width=6, lg=4, className="mx-auto"
            )
        ),
        dbc.Row(
            dbc.Col(
                dbc.Card(
                    [
                        dbc.CardHeader("Step 4: Upload Data File (CSV)", className="bg-success text-white"),
                        dbc.CardBody(
                            [
                                dcc.Upload(
                                    id="upload-file",
                                    children=html.Div(
                                        ["Drag and Drop or ", html.A("Select a CSV File", style={"color": "#007bff"})]
                                    ),
                                    style={
                                        "width": "100%",
                                        "height": "60px",
                                        "lineHeight": "60px",
                                        "borderWidth": "1px",
                                        "borderStyle": "dashed",
                                        "borderRadius": "5px",
                                        "textAlign": "center",
                                        "marginBottom": "10px",
                                    },
                                    multiple=False,
                                ),
                                dbc.Button("Upload to Database", id="upload-btn", color="primary", className="w-100 mt-2"),
                            ]
                        ),
                    ],
                    className="shadow-lg mt-4",
                    id="upload-card",
                    style={"display": "none"}
                ),
                width=6, lg=4, className="mx-auto"
            )
        ),
        dbc.Row(
            dbc.Col(
                dbc.Alert(id="upload-alert", is_open=False, dismissable=True, className="mt-3"),
                width=6, lg=4, className="mx-auto"
            )
        ),
    ],
    fluid=True,
    style={"backgroundColor": "#f8f9fa", "padding": "20px"}
)

@callback(
    [
        Output("db-name", "options"),
        Output("connection-alert", "children"),
        Output("connection-alert", "color"),
        Output("connection-alert", "is_open"),
        Output("db-card", "style"),
        Output("upload-card", "style"),
    ],
    Input("connect-btn", "n_clicks"),
    [
        State("db-type", "value"),
        State("host", "value"),
        State("port", "value"),
        State("username", "value"),
        State("password", "value"),
    ],
)
def connect_to_db(n_clicks, db_type, host, port, username, password):
    if n_clicks is None:
        raise PreventUpdate

    if not all([db_type, host, username, password]):
        return [], "Missing connection details", "danger", True, {"display": "none"}, {"display": "none"}

    try:
        if db_type == "mysql":
            base_url = f"mysql+pymysql://{username}:{password}@{host}:{port or 3306}"
            engine = sa.create_engine(base_url)
            db_list = [row[0] for row in engine.execute("SHOW DATABASES").fetchall()]

        elif db_type == "mssql":
            base_url = f"mssql+pyodbc://{username}:{password}@{host}:{port or 1433}/master?driver=ODBC+Driver+17+for+SQL+Server"
            engine = sa.create_engine(base_url)
            db_list = [row[0] for row in engine.execute("SELECT name FROM sys.databases WHERE database_id > 4").fetchall()]  # Exclude system DBs

        elif db_type == "oracle":
            base_url = f"oracle+cx_oracle://{username}:{password}@{host}:{port or 1521}/?service_name=orcl"  # Assume service_name=orcl, adjust if needed
            engine = sa.create_engine(base_url)
            db_list = [row[0] for row in engine.execute("SELECT username FROM all_users ORDER BY username").fetchall()]  # Schemas as 'databases'

        else:
            return [], "Invalid DB type", "danger", True, {"display": "none"}, {"display": "none"}

        options = [{"label": db, "value": db} for db in db_list]
        return options, "Connected successfully!", "success", True, {"display": "block"}, {"display": "none"}

    except Exception as e:
        return [], f"Connection failed: {str(e)}", "danger", True, {"display": "none"}, {"display": "none"}

@callback(
    [
        Output("upload-alert", "children", allow_duplicate=True),
        Output("upload-alert", "color", allow_duplicate=True),
        Output("upload-alert", "is_open", allow_duplicate=True),
        Output("upload-card", "style", allow_duplicate=True),
    ],
    Input("db-name", "value"),
    prevent_initial_call=True
)
def show_upload_on_db_select(db_name):
    if db_name:
        return "", "", False, {"display": "block"}
    return "", "", False, {"display": "none"}

@callback(
    [
        Output("upload-alert", "children"),
        Output("upload-alert", "color"),
        Output("upload-alert", "is_open"),
    ],
    Input("upload-btn", "n_clicks"),
    [
        State("db-type", "value"),
        State("host", "value"),
        State("port", "value"),
        State("username", "value"),
        State("password", "value"),
        State("db-name", "value"),
        State("upload-file", "contents"),
        State("upload-file", "filename"),
    ],
)
def upload_file(n_clicks, db_type, host, port, username, password, db_name, contents, filename):
    if n_clicks is None or not contents or not db_name:
        raise PreventUpdate

    try:
        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)
        df = pd.read_csv(io.StringIO(decoded.decode("utf-8")))

        table_name = filename.split(".")[0]  # Use filename without extension as table name

        if db_type == "mysql":
            url = f"mysql+pymysql://{username}:{password}@{host}:{port or 3306}/{db_name}"

        elif db_type == "mssql":
            url = f"mssql+pyodbc://{username}:{password}@{host}:{port or 1433}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"

        elif db_type == "oracle":
            url = f"oracle+cx_oracle://{username}:{password}@{host}:{port or 1521}/{db_name}"  # For Oracle, db_name is schema, adjust if needed

        engine = sa.create_engine(url)
        df.to_sql(table_name, engine, if_exists="replace", index=False)

        return f"Uploaded {filename} to table {table_name} successfully!", "success", True

    except Exception as e:
        return f"Upload failed: {str(e)}", "danger", True

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))  # Default to 8050 if PORT is not set
    app.run(host="0.0.0.0", port=port, debug=False)
