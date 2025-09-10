from airflow.plugins_manager import AirflowPlugin
from helper.download import setup_ticker, retrieve_ticker, download_stocks, store_tables

class HelperPlugin(AirflowPlugin):
    name = "helper_plugin"
    operators = []
    hooks = []
    macros = []
    executors = []
    admin_views = []
    flask_blueprints = []
    menu_links = []