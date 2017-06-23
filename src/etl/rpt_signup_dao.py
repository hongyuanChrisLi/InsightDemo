from rpt_abstr_dao import RptAbstrDao
from src.utility import constants as const
import pyspark.sql.functions as sf


INVITE_DATE = 'invite_date'
SCHEDULE_DATE = 'schedule_date'
WAITLIST_DATE = 'waitlist_date'
SIGNUP_TIME = 'signup_time'

class RptSignupDao (RptAbstrDao):

    """
    This table generates report on the signup time for each invitation
    """

    def get_signup_time(self):
        self.__base_table__()

    def __base_table__(self):

        # (PROGRAM_ID, CALENDAR_SEASON_ID, AUDIT_OPERATION_ID, USER_ID)
        df_aud = self.df_fa \
            .select(const.PROGRAM_ID,
                    const.CREATED_DATE,
                    const.CALENDAR_SEASON_ID,
                    const.AUDIT_OPERATION_ID,
                    const.INTERVIEW_EVENT_ID,
                    const.USER_ID) \
            .repartition(const.PROGRAM_ID)

        # (PROGRAM_ID, CALENDAR_SEASON_ID, CREATED_DATE, USER_ID)
        df_invite = df_aud\
            .where(df_aud[const.AUDIT_OPERATION_ID] == const.INVITE_OPERATION)\
            .drop(const.AUDIT_OPERATION_ID)\
            .drop(const.INTERVIEW_EVENT_ID)

        # (PROGRAM_ID, CALENDAR_SEASON_ID, INTERVIEW_EVENT_ID, SCHEDULE_DATE, USER_ID)
        df_schdl_loc = df_aud \
            .filter(df_aud[const.AUDIT_OPERATION_ID] == const.SIGNUP_INTERVIEW_OPERATION) \
            .drop(const.AUDIT_OPERATION_ID) \
            .selectExpr(const.PROGRAM_ID,
                        const.CALENDAR_SEASON_ID,
                        const.INTERVIEW_EVENT_ID,
                        const.CREATED_DATE + " as " + SCHEDULE_DATE,
                        const.USER_ID)

        # (USER_ID, WAITLIST_DATE, INTERVIEW_EVENT_ID)
        df_waitlist = df_aud\
            .filter(const.AUDIT_OPERATION_ID + " = " + str(const.SIGNUP_WAITLIST_OPERATION)
                    + " AND " + const.INTERVIEW_EVENT_ID + " IS NOT NULL")\
            .drop(const.AUDIT_OPERATION_ID)\
            .selectExpr(const.USER_ID,
                        const.INTERVIEW_EVENT_ID,
                        const.CREATED_DATE + " as " + WAITLIST_DATE)


        condi = [df_schdl_loc[const.USER_ID] == df_waitlist[const.USER_ID],
                 df_schdl_loc[const.INTERVIEW_EVENT_ID] == df_waitlist[const.INTERVIEW_EVENT_ID]]

        # (PROGRAM_ID, CALENDAR_SEASON_ID, USER_ID, SCHEDULE_DATE)
        df_schedule = df_schdl_loc.join(df_waitlist, condi, const.INNER)\
            .drop(df_waitlist[const.USER_ID])\
            .drop(df_waitlist[const.INTERVIEW_EVENT_ID])\
            .drop(df_schdl_loc[const.INTERVIEW_EVENT_ID])\
            .filter(WAITLIST_DATE + " IS NULL OR " + WAITLIST_DATE + " > " +  SCHEDULE_DATE)\
            .drop(WAITLIST_DATE)



        condi = [df_invite[const.USER_ID] == df_schedule[const.USER_ID],
                 df_invite[const.PROGRAM_ID] == df_schedule[const.PROGRAM_ID],
                 df_invite[const.CALENDAR_SEASON_ID] == df_schedule[const.CALENDAR_SEASON_ID]]

        # (PROGRAM_ID, CALENDAR_SEASON_ID, USER_ID, SIGNUP_TIME)
        df_join = df_invite.join(df_schedule, condi, const.INNER) \
            .drop(df_invite[const.USER_ID]) \
            .drop(df_schedule[const.PROGRAM_ID]) \
            .drop(df_schedule[const.CALENDAR_SEASON_ID])\
            .filter(const.CREATED_DATE + " < " + SCHEDULE_DATE)\
            .selectExpr(const.PROGRAM_ID,
                        const.CALENDAR_SEASON_ID,
                        const.USER_ID,
                        "round((unix_timestamp(SCHEDULE_DATE) - unix_timestamp(CREATED_DATE))) as " + SIGNUP_TIME)

        self.__write_df__(df_join, const.TARPT_SIGNUP_TIME_RPT)
