import os
import re
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import sys
#sys.path.append("/mapr/Edfdev.kenanga.local/EDF/py_script")
sys.path.append(os.environ["sys_path_py_script"])

from etl_common_function import run_etl, set_parameter, drop_partition_day


jdbc_url = (
    f"jdbc:sqlserver://{os.environ['MSSQL_HOST']}:{os.environ.get('MSSQL_PORT', '1433')};"
    f"databaseName={os.environ['MSSQL_DB']};encrypt=true;trustServerCertificate=true"
)
user = os.environ["MSSQL_USER"]
password = os.environ["MSSQL_PASSWORD"]

source_name = "m21"
table_name = "customer"
hive_table_name = source_name + "_" + table_name
partition_col = "etl_dt"

# spark session
spark, ext_start_time, ext_end_time, today_date, yesterday_date = run_etl(source_name, table_name)
batch_date = today_date

# set parameter, call parameter by params["<parameter name>"]
params = set_parameter(spark)

###########################################################################
## standard template
###########################################################################

query = f"""
    SELECT
    firm,
    code,
    nricnumber,
    shortname,
    fullname,
    groupaccount,
    contactperson,
    address,
    telephone,
    fax,
    mobilephone,
    pager,
    email,
    dateopened,
    dateclosed,
    CAST(diffmailaddress AS VARCHAR(50)) AS diffmailaddress,
    defaultaccexecutive,
    defaultdealer,
    CAST(relatedparty AS VARCHAR(50)) AS relatedparty,
    CAST(resident AS VARCHAR(50)) AS resident,
    baseccy,
    chargeexchlevy,
    chargechlevy,
    chargecommission,
    CAST(chargeinterest AS VARCHAR(50)) AS chargeinterest,
    interestrategroup,
    interestadjgroup,
    interestcomputedon,
    CAST(chargemargin AS VARCHAR(50)) AS chargemargin,
    marginadjgroup,
    commissiongroup,
    natureofaccount,
    finaccountgroup,
    updatesubledger,
    shortmarginamt,
    emaleveragefactor,
    printpriority,
    CAST(printinactive AS VARCHAR(50)) AS printinactive,
    CAST(active AS VARCHAR(50)) AS active,
    custcreditgroup,
    createuser,
    createdate,
    modifyuser,
    modifydate,
    custsttrptname,
    tradeconfrptname,
    branch,
    interestqlfamtbasedon,
    mininterestamtgroup,
    clienttype,
    race,
    occupation,
    employer,
    natureofbusiness,
    settlementbank,
    settlementbankbranch,
    settlementbankaccount,
    introducerae,
    oldnricnumber,
    country,
    countrystate,
    annualincome,
    contactperson2,
    telephone2,
    fax2,
    mobilephone2,
    email2,
    contpersondesgn,
    contpersondesgn2,
    contpersondept,
    contpersondept2,
    settlementbankaddress,
    pager2,
    remarks,
    commrebate,
    extref,
    dateofbirth,
    businessaddress,
    CAST(onlinetradingcust AS VARCHAR(50)) AS onlinetradingcust,
    otcommdiscmethod,
    riskprofile,
    CAST(candotx AS VARCHAR(50)) AS candotx,
    cif,
    postalcode,
    gstlocation,
    CAST(iscorporateacc AS VARCHAR(50)) AS iscorporateacc,
    CAST(gstregperson AS VARCHAR(50)) AS gstregperson,
    gstregno,
    CAST(postinterestwhenaccclosed AS VARCHAR(50)) AS postinterestwhenaccclosed,
    ref1,
    nationality,
    CAST(fatcausperson AS VARCHAR(50)) AS fatcausperson,
    fatcataxidno,
    addressline1,
    addressline2,
    addressline3,
    businessaddressline1,
    businessaddressline2,
    businessaddressline3,
    businesspostalcode,
    businesscountrystate,
    businesscountry,
    CAST(domrmborrowing AS VARCHAR(50)) AS domrmborrowing,
    domrmborrowingbnmrefno,
    domrmborrowingbnmapplimit,
    domrmborrowingbnmstartdate,
    domrmborrowingbnmexpirydate,
    networth,
    salutation,
    city,
    businesscity,
    gender,
    maritalstatus,
    CAST(pdpaprivacyconsent AS VARCHAR(50)) AS pdpaprivacyconsent,
    telcountry,
    telarea,
    telnumber,
    faxcountry,
    faxarea,
    faxnumber,
    mobcountry,
    mobarea,
    mobnumber,
    pgrcountry,
    pgrarea,
    pgrnumber,
    tel2country,
    tel2area,
    tel2number,
    fax2country,
    fax2area,
    fax2number,
    mob2country,
    mob2area,
    mob2number,
    pgr2country,
    pgr2area,
    pgr2number,
    CAST(accopenfacetoface AS VARCHAR(50)) AS accopenfacetoface,
    cdia,
    custsttlanguage
    FROM dbo.customer

    
    WHERE 
        CAST(createdate AS DATE) = DATEADD(DAY, -1, CAST('{ext_start_time}' AS DATE))
        OR CAST(modifydate AS DATE) = DATEADD(DAY, -1, CAST('{ext_start_time}' AS DATE))

"""

############################################################################


# housekeep partition if required
drop_partition_day(spark, batch_date, params["raw_schema"], hive_table_name, partition_col, params["retention_raw_delta"])

# Read from MSSQL
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("query", query)
    .option("user", user)
    .option("password", password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option("fetchsize", "10000")
    .load()
)

df = df.withColumn("etl_timestamp", current_timestamp().cast("string"))
df = df.withColumn("etl_dt", lit(batch_date))

# Write directly to Hive
target_table = f"{params['raw_schema']}.{hive_table_name}"
target_cols = spark.table(target_table).columns
df = df.select(*target_cols)
  
(
    df.write
    .mode("overwrite")
    .insertInto(f"{params['raw_schema']}.{hive_table_name}")
)

spark.stop()
