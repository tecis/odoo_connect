#!python3
from os.path import isdir, join, abspath
import json
import xmlrpc.client
import click


def parse_conditions(text: str) -> list:
    """
    Parse multiple conditions defined as a string onto Odoo External API condition.

    Arguments
    ---------
    query: str, conditions passed as an SQL WHERE condition string sepraated
        by commas and spaces.

    Returns
    -------
    list: Django API compatible operators to filter data returned by query.
    """
    val = list()
    queries = text.split(",")
    for query in quries:
        i = list()
        mask = query.split(" ")
        for m in mask:
            mask.strip()
            i.append(mask)
        val.append(i)
    return val


def query_data(
    url: str,
    odoo_db: str,
    odoo_username: str,
    odoo_password: str,
    table_name: str,
    conditions: str = None,
    fields: tuple = None,
    limit: int = None,
):
    """
    Query data from Odoo server.

    Arguments
    ---------
    url : str, URL for Odoo server.
    odoo_db : str, Name of database within server to query.
    odoo_username: str, User name with access to data.
    odoo_password: str, Password for username to access data.
    table_name: str, Name of table to extract datafrom.
    condition: str, Text defining queries to execute on data. (default = None)
    fields: tuple, Of field names to use. (default = None)
    limit: int, Limit of fields to query. (default = None)

    Returns
    -------
    JSON

    Example
    -------
    query_data(
        url = "https://www.example.com",
        odoo_db = "example-db-8858",
        odoo_username = "user",
        odoo_password = "mypassword",
        condition = "data > 2024/12/1, name like 'sam%'",
        fields = ("id", "name", "date",),
        limit = 30,
    )

    Note
    ----
    - Condition: if any date values are passwed as a condition it must fallow
        the fallowing format 'YYYY/m/d' example: 2024/12/1.
    """
    # Create variable to store parsed conditions limits to include in query.
    filters = dict()
    # Clean URL by removing empty spaces.
    url = str(url).strip()
    odoo_db = str(odoo_db).strip()
    odoo_username = str(odoo_username).strip()
    if len(fields) > 0:
        field_mask = list()
        for field in fields:
            field_mask.append(str(field).strip().lower())
        filters["fields"] = field_mask
        del fields, field
    # If conditions is a string parse_conditions separate table instances.
    if isinstance(conditions, str):
        queries = parse_conditions(conditions)
    else:
        queries = None

    # Establish conection with server.
    common = xmlrpc.client.ServerProxy("{}/xmlrpc/2/common".format(url))
    uid = common.authenticate(odoo_db, odoo_username, odoo_password, {})
    models = xmlrpc.client.ServerProxy("{}/xmlrpc/2/object".format(url))

    # If limit is not an integer asign None value.
    if isinstance(limit, int):
        filters["limit"] = limit
        iterations = 1
    elif limit is None:
        # If no limit is provided data in batches of 500 rows.
        filters["limit"] = 500
        filters["offset"] = 0
        # Get total rows that will be queried.
        if isinstance(queries, list):
            total_rows = models.execute_kw(
                odoo_db,
                uid,
                odoo_password,
                table_name,
                "search_count",
                [[queries]],
            )
        elif queries is None:
            total_rows = models.execute_kw(
                odoo_db,
                uid,
                odoo_password,
                table_name,
                "search_count",
                [[]],
            )
        else:
            raise ValueError("Queries are not a list.")

        # Floor division for the ammount of iterations nedded t ocycle through
        # all the data.
        iterations = total_rows // filters["limit"]
        # If a remainder is found aadd an aditional iteration.
        if total_rows % filters["limit"] > 0:
            iterations += 1

    else:
        raise ValueError("Value passed in limit must be an integer or None.")

    # Query data from server.
    if queries is None:
        filters["offset"] = 0
        # Cycle through every iteration until data has been queried.
        for i in range(iterations):
            filters["offset"] = filters["limit"] * i
            if i > 0:
                val.append(
                    models.execute_kw(
                        odoo_db,
                        uid,
                        odoo_password,
                        table_name,
                        "search_read",
                        [[]],
                        filters,
                    )
                )
            else:
                val = models.execute_kw(
                    odoo_db,
                    uid,
                    odoo_password,
                    table_name,
                    "search_read",
                    [[]],
                    filters,
                )
    else:
        filters["offset"] = 0
        for i in range(iterations):
            filters["offset"] = filters["limit"] * i
            if i > 0:
                val.append(
                    models.execute_kw(
                        odoo_db,
                        uid,
                        odoo_password,
                        table_name,
                        "search_read",
                        [[queries]],
                        filters,
                    )
                )
            else:
                val = models.execute_kw(
                    odoo_db,
                    uid,
                    odoo_password,
                    table_name,
                    "search_read",
                    [[queries]],
                    filters,
                )
    return val


def check_connection(
    url: str,
    odoo_db: str,
    odoo_username: str,
    odoo_password: str,
) -> bool:
    """
    Check connection to xmlrpc server returns a valid version.

    Arguments
    ---------
    url : str, URL for Odoo server.
    odoo_db : str, Name of database within server to query.
    odoo_username: str, User name with access to data.
    odoo_password: str, Password for username to access data.

    Returns
    -------
    valid : bool, If True connection was established successfully.
    """

    # Clean URL by removing empty spaces.
    url = str(url).strip()
    odoo_db = str(odoo_db).strip()
    odoo_username = str(odoo_username).strip()
    # Establish conection with server.
    common = xmlrpc.client.ServerProxy("{}/xmlrpc/2/common".format(url))
    version = common.version()
    print(version)

    return valid


CONTEXT_SETTINGS = dict(help_option_names=("-h", "--help"))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    "-u",
    "--url",
    prompt=True,
    type=str,
    help="Url for Odoo instance. Exmaple: https://example.com .",
)
@click.option(
    "-d", "--db", prompt=True, type=str, help="Name of database. Example: my_db1125 ."
)
@click.option(
    "-U",
    "--user",
    prompt=True,
    type=str,
    help="Name of user in atabase. Example: myuser .",
)
@click.option(
    "-p",
    "--password",
    hide_input=True,
    prompt=True,
    help="Password for user in database. Example: mypassword .",
)
@click.option(
    "-t",
    "--table",
    prompt=True,
    type=str,
    help="Table name to query. Exmaple: res.partner .",
)
@click.option(
    "-c",
    "--conditions",
    default=None,
    show_default=True,
    type=str,
    help="""Text defining queries to execute on data. Exmaple: "data > 2024/12/1, name like 'sam%'" .""",
)
@click.option(
    "-f",
    "--fields",
    multiple=True,
    type=str,
    help="Name of fields in to query from table. Exmple: -f id -f name -f date",
)
@click.option(
    "-l",
    "--limit",
    default=None,
    show_default=True,
    type=int,
    help="If specifeid limit the number of rows to extract from database table. Example: -l 10.",
)
@click.option(
    "-o",
    "--output",
    default=None,
    show_default=True,
    prompt=True,
    type=str,
    help="Directory to save file to.",
)
def cli_exe(
    url: str,
    db: str,
    user: str,
    password: str,
    table: str,
    conditions: str = None,
    fields: tuple = None,
    limit: int = None,
    output: str = None,
):
    """
    Command line interface (CLI) to extract table data from a live Odoo instance
    using Odoo's external API via XMLRPC.

    Example:

    cli_query -u https://www.example.com -d example-db-8858 -U my_user
    -p mypassword -c "data > 2024/12/1, name like 'sam%'" -f id -f name -f date
    -l 30
    """
    text = query_data(
        url=url,
        odoo_db=db,
        odoo_username=user,
        odoo_password=password,
        table_name=table,
        conditions=conditions,
        fields=fields,
        limit=limit,
    )
    # If output is not none save data as JSON file.
    if output is None:
        print(text)
    else:
        if isdir(abspath(str(output))):
            output = join(abspath(output), f"{table}.json")
            with open(output, "w", encoding="utf-8") as f:
                f.write(str(json.dumps(text)))
                print(f"File saved to {output}.")
        elif isdir(join("~", "Downloads")):
            output = join("~", "Downloads", f"{table}.json")
            try:
                with open(output, "w", encoding="utf-8") as f:
                    f.write(str(json.dumps(text)))
                    print(f"File saved to {output}.")
            except Exception as e:
                repr(e)
                raise e
        else:
            print(
                "Value provided in output argument does not match a directory"
                "File was not saved.",
            )
    return text


if __name__ == "__main__":
    cli_exe()
