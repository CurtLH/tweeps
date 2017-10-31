import click
from tweeps import streaming_api
from tweeps import etl_process

@click.group()
def cli():

    """
    Twitter Streaming API and ETL Process
    """

    pass

cli.add.commands(streaming_api.cli, 'api')
cli.add_commands(etl_process.cli, 'etl') 
