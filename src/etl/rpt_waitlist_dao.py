from rpt_abstr_dao import RptAbstrDao
from src.utility import constants as const
import pyspark.sql.functions as sf

FIRST_ACTION_DATE = 'first_action_date'
WAITLIST_DATE = 'waitlist_date'
SCHEDULE_DATE = 'schedule_date'
TOTAL_INVITES = 'total_invites'

TOTAL_WAITLISTED = 'total_waitlisted'
TOTAL_WAITLISTED_TO_SCHEDULED = 'total_waitlisted_to_scheduled'
TOTAL_WAITLISTED_TO_COMPLETED = 'total_waitlisted_to_completed'
TOTAL_WAITLISTED_TO_CANCELED = 'total_waitlisted_to_canceled'
TOTAL_WAITLISTED_TO_WITHDRAWN = 'total_waitlisted_to_withdrawn'

PERCENT_WAITLISTED = 'percent_waitlisted'
PERCENT_WAITLISTED_TO_SCHEDULED = 'percent_waitlisted_to_scheduled'
PERCENT_WAITLISTED_TO_COMPLETED = 'percent_waitlisted_to_completed'
PERCENT_WAITLISTED_TO_CANCELED = 'percent_waitlisted_to_canceled'
PERCENT_WAITLISTED_TO_WITHDRAWN = 'percent_waitlisted_to_withdrawn'

class RptWaitlistDao(RptAbstrDao):

    """
    This class populate the waitlist_rpt table
    """


    def get_waitlist_rpt(self):
        self.__base_table__()
        self.__full_table__()

    def __base_table__(self):

        df_pa = self.df_fa\
            .select(const.PROGRAM_ID,
                    const.CALENDAR_SEASON_ID,
                    const.CREATED_BY_USER_ID,
                    const.AUDIT_OPERATION_ID,
                    const.CREATED_DATE)

        # (PROGRAM_ID, CALENDAR_SEASON_ID, CREATED_DATE, USER_ID)
        df_invite = df_pa \
            .drop(const.CREATED_BY_USER_ID)\
            .where(df_pa[const.AUDIT_OPERATION_ID] == const.INVITE_OPERATION) \
            .drop(const.AUDIT_OPERATION_ID)

        # (PROGRAM_ID, CALENDAR_SEASON_ID,TOTAL_INVITES)
        df_tip = df_invite\
            .drop(const.SPECIALTY_ID)\
            .groupBy([const.PROGRAM_ID, const.CALENDAR_SEASON_ID])\
            .agg(sf.count(const.CREATED_DATE).alias(TOTAL_INVITES))

        # ( PROGRAM_ID, CALENDAR_SEASON_ID, CREATED_BY_USER_ID  FIRST_ACTION_DATE)
        # fad: First Action Date
        df_fad = self.__read_df__(const.TADW_AUDIT_DIM)\
            .select(const.PROGRAM_ID,
                    const.CALENDAR_SEASON_ID,
                    const.CREATED_BY_USER_ID,
                    const.CREATED_DATE)\
            .groupBy(const.PROGRAM_ID, const.CALENDAR_SEASON_ID, const.CREATED_BY_USER_ID)\
            .agg(sf.min(const.CREATED_DATE).alias(FIRST_ACTION_DATE))

        # ( PROGRAM_ID, CALENDAR_SEASON_ID, CREATED_BY_USER_ID, WAITLIST_DATE,APPLICATION_ID)
        # w: waitlist
        df_w = self.__read_df__(const.TADW_AUDIT_DIM) \
            .selectExpr(const.PROGRAM_ID,
                    const.CALENDAR_SEASON_ID,
                    const.AUDIT_OPERATION_ID,
                    const.CREATED_BY_USER_ID,
                    const.CREATED_DATE + " as " + WAITLIST_DATE,
                    const.APPLICATION_ID) \
            .filter(const.AUDIT_OPERATION_ID + " = " + str(const.SIGNUP_WAITLIST_OPERATION)
                    + " AND " + const.APPLICATION_ID + " IS NOT NULL") \
            .drop(const.AUDIT_OPERATION_ID) \
            .dropDuplicates()

        join_condi = [df_fad[const.PROGRAM_ID] == df_w[const.PROGRAM_ID],
                      df_fad[const.CALENDAR_SEASON_ID] == df_w[const.CALENDAR_SEASON_ID],
                      df_fad[const.CREATED_BY_USER_ID] == df_w[const.CREATED_BY_USER_ID],
                      df_fad[FIRST_ACTION_DATE] == df_w[WAITLIST_DATE]]


        #faw: First Action Waitlist
        # (PROGRAM_ID, CALENDAR_SEASOn_ID, CREATED_BY_USER_ID, APPLICATION_ID)
        df_faw = df_fad.join(df_w, join_condi, const.INNER)\
            .drop(df_fad[const.PROGRAM_ID])\
            .drop(df_fad[const.CALENDAR_SEASON_ID])\
            .drop(df_fad[const.CREATED_BY_USER_ID])\
            .drop(df_fad[FIRST_ACTION_DATE])\
            .drop(df_w[WAITLIST_DATE])


        # schdl: schedule
        # (PROGRAM_ID, CALENDAR_SEASON_ID, CREATED_BY_USER_ID)
        df_schdl = self.__read_df__(const.TADW_AUDIT_DIM) \
            .selectExpr(const.PROGRAM_ID,
                        const.CALENDAR_SEASON_ID,
                        const.AUDIT_OPERATION_ID,
                        const.CREATED_BY_USER_ID)\
            .filter(const.AUDIT_OPERATION_ID + " = " + str(const.SIGNUP_INTERVIEW_OPERATION))\
            .drop(const.AUDIT_OPERATION_ID)\
            .dropDuplicates()

        wts_join_condi = [df_faw[const.PROGRAM_ID] == df_schdl[const.PROGRAM_ID],
                          df_faw[const.CALENDAR_SEASON_ID] == df_schdl[const.CALENDAR_SEASON_ID],
                          df_faw[const.CREATED_BY_USER_ID] == df_schdl[const.CREATED_BY_USER_ID]]

        # wts: waitlist to scheduled
        # (PROGRAM_ID, CALENDAR_SEASOn_ID, CREATED_BY_USER_ID, APPLICATION_ID)
        df_wis = df_faw \
            .drop(df_faw[const.APPLICATION_ID])\
            .join(df_schdl, wts_join_condi, const.INNER)\
            .drop(df_schdl[const.PROGRAM_ID])\
            .drop(df_schdl[const.CALENDAR_SEASON_ID])\
            .drop(df_schdl[const.CREATED_BY_USER_ID])


        # as: application states
        # (APPLICATION_ID, INTERVIEW_STATUS_ID)
        df_as = self.__read_df__(const.TADW_APPLICATION_STATE_DIM)\
            .select(const.APPLICATION_ID,
                    const.INTERVIEW_STATUS_ID)

        # tic: interview completed
        # (APPLICATION_ID)
        df_ic = df_as\
            .filter(df_as[const.INTERVIEW_STATUS_ID] == const.INTERVIEW_COMPLETED)\
            .drop(const.INTERVIEW_STATUS_ID)

        # icm: interview canceled or missed
        # (APPLICATION_ID)
        df_icm = df_as \
            .filter(const.INTERVIEW_STATUS_ID + " IN ("
                    + str(const.INTERVIEW_COMPLETED) + ","
                    + str(const.INTERVIEW_MISSED_CAN_RESCHEDULE) + ","
                    + str(const.INTERVIEW_MISSED_CANNOT_RESCHEDULE) + ")") \
            .drop(const.INTERVIEW_STATUS_ID)

        #iw : interview withdrawn
        df_iw = df_as\
            .filter(df_as[const.INTERVIEW_STATUS_ID] == const.INTERVIEW_WITHDRAWN)\
            .drop(const.INTERVIEW_STATUS_ID)

        # pfaw: partial First Action Waitlist
        # ( PROGREAM_ID, CALENDAR_SEASON_ID, APPLICATION_ID)
        df_pfaw = df_faw.drop(const.CREATED_BY_USER_ID)

        # tw: total waitlisted
        # (PROGRAM_ID, CALENDAR_SEASON_ID, TOTAL_WAITLISTED)
        df_tw = df_pfaw\
            .groupBy(const.PROGRAM_ID, const.CALENDAR_SEASON_ID)\
            .agg(sf.count(const.APPLICATION_ID).alias(TOTAL_WAITLISTED))

        # tws: total waitlisted to scheduled
        # (PROGRAM_ID, CALENDAR_SEASON_ID, TOTAL_WAITLISTED_TO_SCHEDULED)
        df_twis = df_wis\
            .groupBy(const.PROGRAM_ID, const.CALENDAR_SEASON_ID)\
            .agg(sf.count(const.CREATED_BY_USER_ID).alias(TOTAL_WAITLISTED_TO_SCHEDULED))

        # twic: total number of waitlisted to interview completed
        # (PROGRAM_ID, CALENDAR_SEASON_ID, TOTAL_WAITLISTED_TO_COMPLETED)
        df_twic = df_pfaw \
            .join(df_ic, const.APPLICATION_ID, const.INNER) \
            .groupBy(const.PROGRAM_ID, const.CALENDAR_SEASON_ID) \
            .agg(sf.count(const.APPLICATION_ID).alias(TOTAL_WAITLISTED_TO_COMPLETED))

        # twicm: total number waitlisted to interview cancelled or missed
        # (PROGRAM_ID, CALENDAR_SEASON_ID, TOTAL_WAITLISTED_TO_CANCELED)
        df_twicm = df_pfaw\
            .join(df_icm, const.APPLICATION_ID, const.INNER)\
            .groupBy(const.PROGRAM_ID, const.CALENDAR_SEASON_ID)\
            .agg(sf.count(const.APPLICATION_ID).alias(TOTAL_WAITLISTED_TO_CANCELED))

        # twiw: total number of waitlisted to interview withdrawn
        # (PROGRAM_ID, CALENDAR_SEASON_ID, TOTAL_WAITLISTED_TO_WITHDRAWN)
        df_twiw = df_pfaw\
            .join(df_iw, const.APPLICATION_ID, const.INNER)\
            .groupBy(const.PROGRAM_ID, const.CALENDAR_SEASON_ID)\
            .agg(sf.count(const.APPLICATION_ID).alias(TOTAL_WAITLISTED_TO_WITHDRAWN))

        # base
        # (PROGRAM_ID, CALENDAR_SEASON_ID,TOTAL_INVITES, TOTAL_WAITLISTED,
        # TOTAL_WAITLISTED_TO_SCHEDULED, TOTAL_WAITLISTED_TO_COMPLETED,
        # TOTAL_WAITLISTED_TO_CANCELED, TOTAL_WAITLISTED_TO_WITHDRAWN)
        self.df_base = df_tip\
            .join(df_tw, self.__get_join_condi__(df_tip, df_tw), const.LEFT_OUTER)\
            .drop(df_tw[const.PROGRAM_ID]).drop(df_tw[const.CALENDAR_SEASON_ID])\
            .join(df_twis,self.__get_join_condi__(df_tip,df_twis),const.LEFT_OUTER)\
            .drop(df_twis[const.PROGRAM_ID]).drop(df_twis[const.CALENDAR_SEASON_ID])\
            .join(df_twic,self.__get_join_condi__(df_tip,df_twic),const.LEFT_OUTER)\
            .drop(df_twic[const.PROGRAM_ID]).drop(df_twic[const.CALENDAR_SEASON_ID]) \
            .join(df_twicm, self.__get_join_condi__(df_tip, df_twicm), const.LEFT_OUTER) \
            .drop(df_twicm[const.PROGRAM_ID]).drop(df_twicm[const.CALENDAR_SEASON_ID]) \
            .join(df_twiw, self.__get_join_condi__(df_tip, df_twiw), const.LEFT_OUTER) \
            .drop(df_twiw[const.PROGRAM_ID]).drop(df_twiw[const.CALENDAR_SEASON_ID])\
            .selectExpr(const.PROGRAM_ID,
                        const.CALENDAR_SEASON_ID,
                        self.__nvl__(TOTAL_INVITES),
                        self.__nvl__(TOTAL_WAITLISTED),
                        self.__nvl__(TOTAL_WAITLISTED_TO_SCHEDULED),
                        self.__nvl__(TOTAL_WAITLISTED_TO_COMPLETED),
                        self.__nvl__(TOTAL_WAITLISTED_TO_CANCELED),
                        self.__nvl__(TOTAL_WAITLISTED_TO_WITHDRAWN)
                        )

    def __full_table__(self):

        # pw: percent of waitlisted
        expr_pw = self.__percent__(TOTAL_WAITLISTED, TOTAL_INVITES, PERCENT_WAITLISTED)

        # pws: percent waitlisted to scheduled
        expr_pws = self.__percent__(TOTAL_WAITLISTED_TO_SCHEDULED, TOTAL_WAITLISTED, PERCENT_WAITLISTED_TO_SCHEDULED)

        # pwc: percent waitlisted to completed
        expr_pwc = self.__percent__(TOTAL_WAITLISTED_TO_COMPLETED, TOTAL_WAITLISTED, PERCENT_WAITLISTED_TO_COMPLETED)

        # pwcm: percent waitlisted to canceled or missed
        expr_pwcm = self.__percent__(TOTAL_WAITLISTED_TO_CANCELED, TOTAL_WAITLISTED, PERCENT_WAITLISTED_TO_CANCELED)

        # pww: percent waitlisted to withdrawn
        expr_pww = self.__percent__(TOTAL_WAITLISTED_TO_WITHDRAWN, TOTAL_WAITLISTED, PERCENT_WAITLISTED_TO_WITHDRAWN)

        df_full = self.df_base.selectExpr(
            const.PROGRAM_ID,
            const.CALENDAR_SEASON_ID,
            TOTAL_INVITES,
            TOTAL_WAITLISTED,
            TOTAL_WAITLISTED_TO_SCHEDULED,
            TOTAL_WAITLISTED_TO_COMPLETED,
            TOTAL_WAITLISTED_TO_CANCELED,
            TOTAL_WAITLISTED_TO_WITHDRAWN,
            expr_pw,
            expr_pws,
            expr_pwc,
            expr_pwcm,
            expr_pww
        )

        self.__write_df__(df_full, const.TARPT_WAITLIST_RPT)

