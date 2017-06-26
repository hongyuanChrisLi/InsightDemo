import logging

from src.cnx.spark_redshift_cnx import SparkRedshiftCnx
from src.utility import constants as const


class RptAbstrDao(SparkRedshiftCnx):
    """
    This class serves as an abstract class for generating report tables.

    """
    def __init__(self):
        super(RptAbstrDao, self).__init__()
        self.logger = logging.getLogger(const.TA_ROOT_LOGGER)

        # full audit dim
        # (PROGRAM_ID, CALENDAR_SEASON_ID, CREATED_BY_USER_ID,
        # AUDIT_OPERATION_ID, INTERVIEW_EVENT_ID, CREATED_DATE)

        self.df_fa = self.__read_dist_sort_df__(const.TADW_AUDIT_DIM,const.PROGRAM_ID,
                                           [const.PROGRAM_ID, const.CALENDAR_SEASON_ID, const.CREATED_DATE])\
            .select(const.PROGRAM_ID,
                    const.CREATED_BY_USER_ID,
                    const.CALENDAR_SEASON_ID,
                    const.AUDIT_OPERATION_ID,
                    const.INTERVIEW_EVENT_ID,
                    const.CREATED_DATE,
                    const.USER_ID,
                    const.APPLICATION_ID)

    def log_completion(self, name):
        """
        log task completion
        :param name: name of the object
        :return: N/A
        """
        self.logger.info("Task Completed: " + name)

    @staticmethod
    def __get_join_condi__(df_a, df_b):
        """
        Get common join condition
        :param df_a: Data frame A
        :param df_b: Data fram B
        :return: Common Join condiions: PROGRAM_ID and CALENDAR_SEASON_ID
        """
        return [df_a[const.PROGRAM_ID] == df_b[const.PROGRAM_ID],
                df_a[const.CALENDAR_SEASON_ID] == df_b[const.CALENDAR_SEASON_ID]]

    @staticmethod
    def __nvl__(col_name):
        """
        If null then 0
        :param col_name: column name
        :return: Spark SQL that converts null to zero
        """
        return "nvl(" + col_name  + ", 0) AS " + col_name


    @staticmethod
    def __round_div__(col_numer, col_denom, col_name, scale=2):
        """
        Division, round to default 2 digits
        :param col_numer: Column as numerator
        :param col_denom: Column as denominator
        :param col_name:  Final column name
        :param scale: Scale of the division result
        :return: N/A
        """
        return "round(" + col_numer + " / " + col_denom + "," + str(scale) + ") AS " + col_name

    @staticmethod
    def __percent__(col_numer, col_denom, col_name, scale=1):
        """
        Percentage: round to default 1 digit
        :param col_numer:  Column as numerator
        :param col_denom:  Column as denominator
        :param col_name: Finale Column Name
        :param scale: Scale of the percentage result
        :return: N/A
        """
        return "round((" + col_numer + ") * 100 / " + col_denom + "," + str(scale) + ") AS " + col_name