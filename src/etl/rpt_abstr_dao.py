from src.cnx.spark_redshift_cnx import SparkRedshiftCnx
from src.utility import constants as const
import pyspark.sql.functions as sf


class RptAbstrDao(SparkRedshiftCnx):
    """
    This class serves as an abstract class for generating report tables.

    """

    def __init__(self):
        super(RptAbstrDao, self).__init__()

        # full audit dim
        # (PROGRAM_ID, CALENDAR_SEASON_ID, CREATED_BY_USER_ID,
        # AUDIT_OPERATION_ID, INTERVIEW_EVENT_ID, CREATED_DATE)
        self.df_fa = self.__read_df__(const.TADW_AUDIT_DIM)\
            .select(const.PROGRAM_ID,
                    const.CREATED_BY_USER_ID,
                    const.CALENDAR_SEASON_ID,
                    const.AUDIT_OPERATION_ID,
                    const.INTERVIEW_EVENT_ID,
                    const.CREATED_DATE,
                    const.USER_ID,
                    const.APPLICATION_ID)


    @staticmethod
    def __get_join_condi__(df_a, df_b):
        return [df_a[const.PROGRAM_ID] == df_b[const.PROGRAM_ID],
                df_a[const.CALENDAR_SEASON_ID] == df_b[const.CALENDAR_SEASON_ID]]

    # if null then 0
    @staticmethod
    def __nvl__(col_name):
        return "nvl(" + col_name  + ", 0) AS " + col_name

    # division, round to default 2 digits
    @staticmethod
    def __round_div__(col_numer, col_denom, col_name, scale=2):
        return "round(" + col_numer + " / " + col_denom + "," + str(scale) + ") AS " + col_name

    # percentage
    @staticmethod
    def __percent__(col_numer, col_denom, col_name, scale=1):
        return "round((" + col_numer + ") * 100 / " + col_denom + "," + str(scale) + ") AS " + col_name