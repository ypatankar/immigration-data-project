import configparser
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, date_add, to_date
from pyspark.sql.types import StructType as R, StructField as Fld, FloatType as Fl, StringType as St, \
    IntegerType as In, ShortType as SInt, LongType as LInt, DoubleType as Dbl, TimestampType as Tst
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import pandas as pd
import boto3
from s3fs import S3FileSystem

# Access the dynamic setup information from config file
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

# s3fs library used to save files in S3
s3_client = S3FileSystem(anon=False,key=config['AWS']['AWS_ACCESS_KEY_ID'],secret=config['AWS']['AWS_SECRET_ACCESS_KEY'])

def create_spark_session():
    """
    Create spark session
    :return: spark session object
    """
    spark = SparkSession.builder.config("spark.jars.packages",
                                        "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.5").enableHiveSupport().getOrCreate()
    return spark


def extract_sas_labels(filename, outpath):
    """
    Extracts reference data from the SAS Labels Descriptions file.
    We will crawl this file and retrieve information about country of citizenship/residence, ports of entry,
    state of arrival, mode of transportation, visa types.
    :param filename: name of the file from which data will be extracted
    :param outpath: S3 path for cleaned data
    :return: None
    """
    d = dict()

    # List of tables to be created from the data extracted
    table_list = ['I94CIT', 'I94PORT', 'I94MODE', 'I94ADDR', 'I94VISA']
    flag = False

    # read lines from the file and save the information for each table as key, value pair in the dictionary
    with open(filename, 'r') as fh:
        for tab in table_list:
            for line in fh:
                if tab in line:
                    flag = True
                    d[tab] = list()
                elif '=' in line and 'proc' not in line:
                    x = line.split('=')
                    d[tab].append([x[0].replace("'", "").strip(), x[1].replace("'", "").replace(';', '').strip()])

                if flag and ';' in line:
                    flag = False
                    break

    # iterate over the dictionary to save individual items as separate files on S3
    for k, v in d.items():
        if k == table_list[0]:
            df = pd.DataFrame(v, columns=['cit_res_id', 'country_name'])
            df = df.astype({'cit_res_id': int})

            df.to_csv("reference_data/I94CITRES.csv", index=False)
            with s3_client.open(outpath + "I94CITRES.csv", 'w') as fh:
                df.to_csv(fh, index=False)
        elif k == table_list[1]:
            df = pd.DataFrame(v, columns=['poe_code', 'city_state'])
            df[["city", "state_or_country"]] = df.city_state.str.split(', ', expand=True, n=1)
            df.drop(columns=['city_state'], inplace=True)

            df.to_csv("reference_data/I94PORT.csv", index=False)
            with s3_client.open(outpath + "I94PORT.csv", 'w') as fh:
                df.to_csv(fh, index=False)
        elif k == table_list[2]:
            df = pd.DataFrame(v, columns=['travel_mode', 'mode_name'])

            df.to_csv("reference_data/I94MODE.csv", index=False)
            with s3_client.open(outpath + "I94MODE.csv", 'w') as fh:
                df.to_csv(fh, index=False)
        elif k == table_list[3]:
            df = pd.DataFrame(v, columns=['state_code', 'state_name'])

            # Data cleaning for uniformity
            df['state_name'] = df['state_name']. \
                str.replace('N\.', 'NORTH').str.replace('S\.', 'SOUTH').str.replace('W\.', 'WEST').str.replace('DIST\.',
                                                                                                               'DISTRICT'). \
                str.replace("WISCONSON", "WISCONSIN")

            df.to_csv("reference_data/I94ADDR.csv", index=False)
            with s3_client.open(outpath + "I94ADDR.csv", 'w') as fh:
                df.to_csv(fh, index=False)
        elif k == table_list[4]:
            df = pd.DataFrame(v, columns=['visa_code', 'visa_category'])

            df.to_csv("reference_data/I94VISA.csv", index=False)
            with s3_client.open(outpath + "I94VISA.csv", 'w') as fh:
                df.to_csv(fh, index=False)


def process_temperature_data(spark, filename, outpath):
    """
     Processes temperature data.
     To address the level of detail, this data set will be filtered to retrieve records 1995 onwards and then aggregating average temperature by month.
     The end product of this transformation will be a consolidated table of monthly average temperature information by state, country.
    :param spark: spark session
    :param filename: name of the file from which data will be extracted
    :param outpath: S3 path for cleaned data
    :return: None
    """
    # read temperature data file
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filename)

    # extract year and month from dt column
    df = df.withColumn("year", year(df.dt)).withColumn("month", month(df.dt))

    # filter data for year greater than 1995 so that the data is more relevant to the current date and only for United States
    df = df.select("AverageTemperature", "State", "year", "month").where(
        (f.col("year") >= 1995) & (f.col("Country") == "United States"))

    # aggregate the data
    df = df.groupBy("State", "month").avg("AverageTemperature").withColumnRenamed("avg(AverageTemperature)", "avg_temp")

    # clean data for extraneous text in state column
    df = df.withColumn("State", f.when(f.col("State") == "Georgia (State)", "Georgia").otherwise(f.col("State")))

    # enrich this data to include state code for the states in USA using the reference data files extracted from SAS labels description
    df_state = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        "reference_data/I94ADDR.csv")
    df = df.join(df_state, (f.upper(df.State) == f.upper(df_state.state_name)), how='left').drop("State")

    # write temperature data to parquet files and store it on S3
    df.write.parquet(outpath, mode="overwrite")

    print(df.count())


def process_us_demo_data(spark, filename, outpath):
    """
     Processes US demographic data.
     To address the level of detail, this data set will be rolled up to show state-wise demographic information by aggregrating and pivoting the data.
     The end product of this transformation will be a consolidated table of demographic information by state.
    :param spark: spark session
    :param filename: name of the file from which data will be extracted
    :param outpath: S3 path for cleaned data
    :return: None
    """
    # read US demographics data and delimit it to separate data in individual columns
    df = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").load(
        filename)

    # rename columns for ease of use
    df = df.withColumnRenamed("City", "city").withColumnRenamed("State", "state_name").withColumnRenamed("Median Age",
                                                                                                         "median_age"). \
        withColumnRenamed("Male Population", "male_pop").withColumnRenamed("Female Population", "female_pop"). \
        withColumnRenamed("Total Population", "total_pop").withColumnRenamed("Number of Veterans", "no_of_vets"). \
        withColumnRenamed("Foreign-born", "foreign_born").withColumnRenamed("Average Household Size",
                                                                            "avg_household_size"). \
        withColumnRenamed("State Code", "state_code").withColumnRenamed("Race", "race").withColumnRenamed("Count",
                                                                                                          "count")

    # aggregate and pivot data to adjust the level of granularity of this dataset
    df = df.groupBy("city", "state_name", "state_code", "median_age", "male_pop", "female_pop", "total_pop",
                    "no_of_vets", "foreign_born", "avg_household_size"). \
        pivot("race").agg(f.first("count"))

    # rename the aggregated columns
    df = df.withColumnRenamed("American Indian and Alaska Native", "amer_ind_ak_native").withColumnRenamed("Asian",
                                                                                                           "asian"). \
        withColumnRenamed("Black or African-American", "black").withColumnRenamed("Hispanic or Latino", "hisp_latino"). \
        withColumnRenamed("White", "white")

    # aggregate data by state and apply relevant functions to the numeric columns
    df = df.groupBy("state_name", "state_code").agg(f.avg("median_age"), f.sum("male_pop"), f.sum("female_pop"),
                                                    f.sum("total_pop"), f.sum("no_of_vets"), \
                                                    f.sum("foreign_born"), f.avg("avg_household_size"),
                                                    f.sum("amer_ind_ak_native"), f.sum("asian"), f.sum("black"), \
                                                    f.sum("hisp_latino"), f.sum("white"))

    # rename the numeric columns
    df = df.withColumnRenamed("avg(median_age)", "median_age"). \
        withColumnRenamed("sum(male_pop)", "male_pop").withColumnRenamed("sum(female_pop)", "female_pop"). \
        withColumnRenamed("sum(total_pop)", "total_pop").withColumnRenamed("sum(no_of_vets)", "no_of_vets"). \
        withColumnRenamed("sum(foreign_born)", "foreign_born").withColumnRenamed("avg(avg_household_size)",
                                                                                 "avg_household_size"). \
        withColumnRenamed("sum(amer_ind_ak_native)", "amer_ind_ak_native").withColumnRenamed("sum(asian)", "asian"). \
        withColumnRenamed("sum(black)", "black").withColumnRenamed("sum(hisp_latino)", "hisp_latino"). \
        withColumnRenamed("sum(white)", "white")

    # write demographic data to parquet files and store it on S3
    df.write.parquet(outpath, mode="overwrite")

    print(df.count())


# arrdate and depdate in immigration data are SAS date numeric fields.
# Since Jan 1, 1960 the starting point for the SAS date count, we convert arrdate and deptdate to MM-DD-YYYY string format using a udf
convert_sas_date = udf(lambda x: (timedelta(days=int(x)) + datetime(1960,1,1)).strftime("%m-%d-%Y") if x else x)


def process_immigration_data(spark, outpath):
    """
     Processes immigration data.
     There are 12 sas7bat files, one for each month of the year 2016.
     We notice that the data for June 2016 has more columns than the remaining files and hence need special processing as compared to the other months.
     In addition, a few columns will be dropped as they are not relevant to our project.
    :param spark: spark session
    :param filename: name of the file from which data will be extracted
    :param outpath: S3 path for cleaned data
    :return: None
    """
    # month names for placeholder in filename
    months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]

    # list of columns whose datatype needs to be changed from double to int
    double_to_int_columns = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'arrdate', 'i94mode', 'depdate', 'i94bir',
                             'biryear', 'i94visa', 'admnum']

    # dataframe variable
    df_spark = ''

    # iterate over each month and append data to the dataframe alongwith droping columns and casting columns
    for mon in months:
        # drop the generic columns across all the sas.dat files
        df = spark.read.format('com.github.saurfang.sas.spark').load(
            '../../data/18-83510-I94-Data-2016/i94_{}16_sub.sas7bdat'.format(mon)). \
            drop('count', 'entdepa', 'entdepd', 'entdepu', 'matflag', 'insnum', 'occup')

        # cast the columns
        for colu in double_to_int_columns:
            df = df.withColumn(colu, f.col(colu).cast("int"))

            # since the file for month jun has extra columns which are not very useful, we drop them
        if mon == 'jun':
            # since June data has 34 columns we drop them before UNION
            df = df.drop('validres', 'delete_days', 'delete_mexl', 'delete_dup', 'delete_visa', 'delete_recdup')

        # Duplicating columns to help with bulk loading parquet format to redshift
        df = df.withColumn("yr", df["i94yr"]).withColumn("mon", df["i94mon"])

        # if month is jan, then initiate the data frame else append subsequent months' data to the data frame
        if mon == 'jan':
            df_spark = df
        else:
            df_spark = df_spark.union(df)

    # using the udf defined above, convert sas date to gregorian date format and then stored as strings
    df_spark = df_spark.withColumn("arrdate", convert_sas_date(df_spark.arrdate)).withColumn("depdate",
                                                                                             convert_sas_date(
                                                                                                 df_spark.depdate))

    # rename the column for ease of understanding
    df_spark = df_spark.withColumnRenamed("i94bir", "age")

    print(df_spark.count())

    # write immigration data to parquet files and store it on S3 partitioned by year and month on I94 records for faster processing
    df_spark.write.parquet(outpath, mode="append", partitionBy=["yr", "mon"], compression=None)


def main():
    """
    Main method to call supporting data processing functions
    :return: None
    """
    spark = create_spark_session()

    extract_sas_labels(config['FILES']['SAS_LABELS'], config['OUTPUT_PATH']['SAS_LABELS_S3PATH'])
    process_us_demo_data(spark, config['FILES']['DEMOGRAPHICS'], config['OUTPUT_PATH']['DEMOGRAPHICS_S3PATH'])
    process_temperature_data(spark, config['FILES']['TEMPERATURE'], config['OUTPUT_PATH']['TEMPERATURE_S3PATH'])
    process_immigration_data(spark, config['OUTPUT_PATH']['IMMIGRATION_S3PATH'])

if __name__ == "__main__":
    main()

