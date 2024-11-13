from pyspark.sql.types import StructType,StructField,DateType,IntegerType,StringType,TimestampType

def get_account_schema():
    account_schema = StructType([
        StructField('load_date', DateType(), True),
        StructField('active_ind', IntegerType(), True),
        StructField('account_id', StringType(), True),
        StructField('source_sys', StringType(), True),
        StructField('account_start_date', TimestampType(), True),
        StructField('legal_title_1', StringType(), True),
        StructField('legal_title_2', StringType(), True),
        StructField('tax_id_type', StringType(), True),
        StructField('tax_id', StringType(), True),
        StructField('branch_code', StringType(), True),
        StructField('country', StringType(), True),
    ])
    return account_schema

def get_party_schema():
    party_schema = StructType([
        StructField('load_date', DateType(), True),
        StructField('account_id', StringType(), True),
        StructField('party_id', StringType(), True),
        StructField('relation_type', StringType(), True),
        StructField('relation_start_date', TimestampType(), True)
    ])
    return party_schema

def get_address_schema():
    address_schema = StructType([
        StructField('load_date', DateType(), True),
        StructField('party_id', StringType(), True),
        StructField('address_line_1', StringType(), True),
        StructField('address_line_2', StringType(), True),
        StructField('city', StringType(), True),
        StructField('postal_code', StringType(), True),
        StructField('country_of_address', StringType(), True),
        StructField('address_start_date', TimestampType(), True)
    ])
    return address_schema

def read_accounts(spark):
    return (spark.read
        .format('csv')
        .option('header', 'true')
        .schema(get_account_schema())
        .load('test_data/accounts/')
    )

def read_parties(spark):
    return (spark.read
        .format('csv')
        .option('header', 'true')
        .schema(get_party_schema())
        .load('test_data/parties/')
    )

def read_address(spark):
    return (spark.read
        .format('csv')
        .option('header', 'true')
        .schema(get_address_schema())
        .load('test_data/party_address/')
    )