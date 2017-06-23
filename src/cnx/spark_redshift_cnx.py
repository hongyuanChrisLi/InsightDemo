import sys

from pyspark.sql import SQLContext,SparkSession
from src.utility import url_parser
from src.utility.access import Access

""" Generic class that handles the read and write between Spark and Redshift"""


class SparkRedshiftCnx(Access):

    def __init__(self):

        super(SparkRedshiftCnx, self).__init__()

        url = self.get_redshift_url()
        key_str = self.get_aws_key_str()

        (host, port, dbname, user, password) = url_parser.parse(url)

        spark = SparkSession \
            .builder \
            .appName("Thalamus Analytics")\
            .getOrCreate()
        self.sql_context = SQLContext(spark.sparkContext)
        self.format = "com.databricks.spark.redshift"
        self.jdbcurl =  "jdbc:redshift://" + host + ":" + port + "/" + dbname + \
                        "?user=" + user + "&password=" + password
        self.tempdir = "s3n://" + key_str + "thalamus-0608/tmp/"

    def __read_df__ (self,table):
        df = self.sql_context.read \
            .format(self.format) \
            .option("url",self.jdbcurl) \
            .option("dbtable", table) \
            .option("tempdir", self.tempdir) \
            .load()
        return df

    def __read_dist_sort_df__(self,table, distkey, sortkeys):
        df = self.sql_context.read \
            .format(self.format) \
            .option("url", self.jdbcurl) \
            .option("dbtable", table) \
            .option("tempdir", self.tempdir) \
            .option("distkey", distkey) \
            .option("sortkeyspec", "COMPOUND SORTKEY(" + ",".join(sortkeys) + ")")\
            .load()
        return df

    def __write_df__ (self, df, table, mode="append"):
        df.write\
            .format(self.format) \
            .option("url", self.jdbcurl) \
            .option("dbtable", table) \
            .option("tempdir", self.tempdir) \
            .mode(mode) \
            .save()
