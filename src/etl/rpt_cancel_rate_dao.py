from rpt_abstr_dao import RptAbstrDao
from src.utility import constants as const
import pyspark.sql.functions as sf

DAILY_CANCEL = 'daily_cancel'


class RptCancelRateDao(RptAbstrDao):

    """
    This class calculates the daily cancel rate

    """
    def __base_table__(self):


        # Subset of Audit table
        # (PROGRAM_ID, USER_ID, SEASON_ID, CREATED_DATE, AUDIT_OPERATION_ID)
        df_pa = self.df_fa\
            .selectExpr(const.PROGRAM_ID,
                    const.CREATED_BY_USER_ID,
                    const.CALENDAR_SEASON_ID,
                    "CAST(" + const.CREATED_DATE + " AS date) as " + const.CREATED_DATE,
                    const.AUDIT_OPERATION_ID)

        # (PROGRAM_ID, USER_ID, SEASON_ID, CREATED_DATE)
        df_cancel = df_pa\
            .filter(const.AUDIT_OPERATION_ID + " = " + str(const.CANCEL_INTERVIEW_OPERATION))\
            .drop(const.AUDIT_OPERATION_ID)
        # (PROGRAM_ID, CALENDAR_SEASON_ID, CREATED_DATE, DAY_CANCEL)
        df_daily_cancel = df_cancel\
            .groupBy(const.PROGRAM_ID, const.CALENDAR_SEASON_ID, const.CREATED_DATE)\
            .agg(sf.count(const.CREATED_BY_USER_ID).alias(DAILY_CANCEL))

        """df_final_cancel = df_audit\
            .groupBy(const.CREATED_BY_USER_ID, const.PROGRAM_ID, const.CALENDAR_SEASON_ID)\
            .agg(sf.max(const.CREATED_DATE).alias(const.CREATED_DATE))"""

        # df_daily_cancel.show()
        self.__write_df__(df_daily_cancel, const.TARPT_DAILY_CANCEL_RPT)


    def load_cancel_rate(self):
        self.__base_table__()
