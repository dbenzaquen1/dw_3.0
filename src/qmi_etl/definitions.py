from pathlib import Path

from dagster import (
    AssetKey,
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    SourceAsset,
    define_asset_job,
    graph_asset,
    link_code_references_to_git,
    op,
    with_source_code_references,
)
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping

from qmi_etl.defs.dbt_assets import datawarehouse_2_dbt_assets, dbt_resource
from qmi_etl.defs.sql_server_assets import build_sql_server_table_assets
from qmi_etl.defs.bq_io_manager import bigquery_io_manager

SQL_SERVER_TABLES: list[str] = [
    "Addresses",
    "APCashJrn",
    "APCheckHist",
    "APComment",
    "APJrn",
    "ARJrn",
    "BankAccounts",
    "Comments",
    "Contacts",
    "Customer",
    "GeneralLedger_view",
    "GLAdjSource",
    "GLAdjSourceAccounts",
    "GLComment",
    "GLHistory",
    "GLLevel",
    "GLMaster",
    "GLStdAdj",
    "GLTransactions",
    "INInvoiceHead",
    "INInvoiceLine",
    "Inventory",
    "InventoryClassifications",
    "InventoryHist",
    "ItemMaster",
    "OrderLinesOpenPieces_View",
    "OrderStatus",
    "OrderStatusCodes",
    "PartNumber",
    "PartNumberProcessLine",
    "POHead",
    "POLine",
    "Processes",
    "RW_ProductionOrderFinishedMaterial",
    "RW_ProductionOrderHeader",
    "RW_ProductionOrderHistory",
    "RW_ProductionOrderPerformance",
    "RW_PurchaseOrders",
    "SalesPerson",
    "SOQT",
    "SOQtLine",
    "TaxExemptReasons",
    "Terms",
    "Vendor",
    "WorkStations",
    "RW_InventoryLocation",
    "RW_OpenOrders",
    "Warehouse",
    "ProductionOrderLines",
    "TaxCodes",
    "App_UIC_Category",
    "App_UIC_Gauge",
    "App_UIC_Grade",
    "App_UIC_Size",
    "App_UIC_Type"
]

sql_server_assets = build_sql_server_table_assets(
    SQL_SERVER_TABLES,
    schema="dbo",
    group_name="sql_server",
    key_prefix=["sql_server"],
)

supplies_export_bq = SourceAsset(
    key=AssetKey(["bigquery", "PO_export", "supplies_export"]),
    description=(
        "Existing BigQuery table PO_export.supplies_export, exposed as a Dagster "
        "source asset for downstream dependencies."
    ),
)

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 6,12,15,18 * * *", execution_timezone="US/Central"
)

dbt_only_job = define_asset_job(
    name="refresh_dbt_assets",
    selection=AssetSelection.groups(
        "base_dbt",
        "staging_dbt",
        "reporting_dbt",
        "seeds_dbt",
    ),
)

sql_server_refresh_job = define_asset_job(
    name="refresh_sql_server_assets",
    selection=AssetSelection.groups("sql_server"),
)

sql_server_refresh_schedule = ScheduleDefinition(
    job=sql_server_refresh_job,
    cron_schedule="0 0,4,6,12,15,18 * * *",
    execution_timezone="US/Central",
)






my_assets = with_source_code_references(
    [
        *sql_server_assets,
        supplies_export_bq,
        datawarehouse_2_dbt_assets,
    ]
)

my_assets = link_code_references_to_git(
    assets_defs=my_assets,
    git_url="https://github.com/dagster-io/dagster/",
    git_branch="master",
    file_path_mapping=AnchorBasedFilePathMapping(
        local_file_anchor=Path(__file__).parent,
        file_anchor_path_in_repository="examples/quickstart_etl/src/quickstart_etl/",
    ),
)

defs = Definitions(
    assets=my_assets,
    jobs=[dbt_only_job, sql_server_refresh_job],
    schedules=[daily_refresh_schedule, sql_server_refresh_schedule],
    resources={
        "dbt": dbt_resource,
        "bigquery_io_manager": bigquery_io_manager,
    },
)
