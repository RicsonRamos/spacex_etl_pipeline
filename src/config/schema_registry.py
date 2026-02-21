# Fonte única de verdade do pipeline

SCHEMAS = {

    "launches": {
        "api_endpoint": "launches",
        "bronze_table": "bronze_launches",
        "silver_table": "silver_launches",

        "pk": "launch_id",

        "columns": {
            "launch_id": "string",
            "name": "string",
            "date_utc": "timestamp",
            "launch_year": "int",
            "success": "boolean",
            "rocket_id": "string",
            "launchpad_id": "string"
        }
    },

    "rockets": {
        "api_endpoint": "rockets",
        "bronze_table": "bronze_rockets",
        "silver_table": "silver_rockets",

        "pk": "rocket_id",

        "columns": {
            "rocket_id": "string",
            "name": "string",
            "active": "boolean",
            "cost_per_launch": "int",
            "success_rate_pct": "float"
        }
    }

}