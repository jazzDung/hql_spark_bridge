from sqlglot import exp
from win32cryptcon import sz_CERT_STORE_PROV_PHYSICAL


def get_physical_dependencies(expression):
    # Return no dependencies if not alter table content node
    if not isinstance(expression, (exp.Insert, exp.Update, exp.Merge)):
        return {}


    # 1. Collect all CTE names to exclude them
    cte_names = set()
    with_node = expression.args.get("with")
    if with_node:
        for cte in with_node.expressions:
            cte_names.add(cte.alias_or_name.lower())

    # 2. Identify the target table to exclude
    target_table = expression.this.name.lower()

    dependencies = {}
    seen_tables = set()

    # 3. Traverse all Table nodes
    for table in expression.find_all(exp.Table):
        table_name = table.name.lower()

        # EXCLUSION CONDITIONS:
        # - Not the target table
        # - Not in the CTE list
        # - Not already processed (avoid duplicates)
        if table_name != target_table and table_name not in cte_names and table_name not in seen_tables:

            db_node = table.args.get("db")

            # Only consider it a physical table if a Database/Schema is defined
            if db_node:
                # Handle structure: Parameter(this=Var(this=Identifier(this='com_schema')))
                # SQLGlot parses environment variables ${...} into this structure
                schema_name = "unknown"

                # Deeply unpack Parameter/Var
                curr_node = db_node
                while hasattr(curr_node, 'this') and not isinstance(curr_node, exp.Identifier):
                    curr_node = curr_node.this

                if isinstance(curr_node, exp.Identifier) or isinstance(curr_node, str):
                    name = str(curr_node)
                    if isinstance(db_node, exp.Parameter):
                        var_name = name
                        schema_name = var_name.split('_')[0]
                    else:
                        schema_name = name
                else:
                    schema_name = db_node.sql()

                dependencies.update({
                    table.name: {  # Keep original table name format
                        "schema": schema_name
                    }
                })
                seen_tables.add(table_name)

    return dependencies