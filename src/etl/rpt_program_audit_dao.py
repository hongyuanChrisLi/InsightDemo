from rpt_abstr_dao import RptAbstrDao
from src.utility import constants as const
import pyspark.sql.functions as sf

# Alias
TOTAL_SLOTS = 'total_slots' #tsp tss
TOTAL_INVITES = 'total_invites' #tip tis
TOTAL_WITHDRAW = 'total_withdraw' #twp tws
TOTAL_CANCEL = 'total_cancel' #tcp tcs
TOTAL_FIRST_DAY_INVITES = 'total_first_day_invites' #tfdip tfdis
TOTAL_INVITES_AFTER_FIRST_DAY = 'total_invites_after_first_day'
TOTAL_OVERAGE = 'total_overage'
TOTAL_OVERAGE_RATE = 'total_overage_rate'

FIRST_INVITE_DAY = 'first_invite_day' #fidp fids
FIRST_DAY_OVERAGE = 'first_day_overage' #fdop fdos
FIRST_DAY_OVERAGE_RATE = 'first_day_overage_rate' #fdorp fdors
AFTER_FIRST_DAY_OVERAGE = 'after_first_day_overage' #afdop afdos
AFTER_FIRST_DAY_OVERAGE_RATE = 'after_first_day_overage_rate' #afdorp afdors
PERCENT_INVITES_FIRST_DAY = 'percent_invites_first_day' #pifdp
PERCENT_INVITES_AFTER_FIRST_DAY = 'percent_invites_after_first_day'
PERCENT_INTERVIEWS_ACCEPTED = 'percent_interviews_accepted'


class RptProgramAuditDao(RptAbstrDao):

    """
    This class calculate the invites and related audit operations per programs and specialties

    """

    def __init__(self):
        super(RptProgramAuditDao, self).__init__()

        # tts track tier slot
        self.df_tts = self.__read_df__(const.TADW_TRACK_TIER_SLOT_DIM)\
            .select(const.CALENDAR_SEASON_ID, const.PROGRAM_ID,const.ALL_TIER_QUANTITY)

        # ie: interview event
        self.df_ie = self.__read_df__(const.TADW_INTERVIEW_EVENT_DIM)\
            .select(const.INTERVIEW_EVENT_ID, const.CALENDAR_SEASON_ID,const.DATE)

        # prg: program
        self.df_prg= self.__read_df__(const.TADW_PROGRAM_FACT) \
            .select(const.PROGRAM_ID, const.SPECIALTY_ID)

        # pa: partial audit
        self.df_pa = self.df_fa\
            .select(const.PROGRAM_ID, const.CALENDAR_SEASON_ID, const.AUDIT_OPERATION_ID, const.CREATED_DATE)\
            .join(self.df_prg, const.PROGRAM_ID, const.LEFT_OUTER)\
            # .repartition(const.PROGRAM_ID, 3)

        # invite
        # (PROGRAM_ID, SPECIALTY_ID, CALENDAR_SEASON_ID,CREATED_DATE)
        self.df_invite = self.df_pa\
            .where(self.df_pa[const.AUDIT_OPERATION_ID] == const.INVITE_OPERATION)\
            .drop(const.AUDIT_OPERATION_ID)\
            .withColumn(const.CREATED_DATE, self.df_pa[const.CREATED_DATE].cast('date'))

        # withdraw
        # (PROGRAM_ID, SPECIALTY_ID, CALENDAR_SEASON_ID,CREATED_DATE)
        self.df_withdraw = self.df_pa \
            .where(self.df_pa[const.AUDIT_OPERATION_ID] == const.WITHDRAW_OPERATION) \
            .drop(const.AUDIT_OPERATION_ID)

        # cancel
        # (PROGRAM_ID, SPECIALTY_ID, CALENDAR_SEASON_ID, CREATED_DATE)
        self.df_cancel = self.df_pa\
            .where(self.df_pa[const.AUDIT_OPERATION_ID] == const.CANCEL_INTERVIEW_OPERATION)\
            .drop(const.AUDIT_OPERATION_ID)


    """ Base Columns """

    def __cal_total_slots__(self):
        # tsp: total slots of program
        # (PROGRAM_ID, CALENDAR_SEASON_ID, TOTAL_SLOTS)
        self.df_tsp = self.df_tts\
            .groupBy([const.PROGRAM_ID, const.CALENDAR_SEASON_ID]) \
            .agg(sf.sum(const.ALL_TIER_QUANTITY).alias(TOTAL_SLOTS))

        # tss: total slots of specialty
        self.df_tss = self.df_tsp\
            .join(self.df_prg, const.PROGRAM_ID, const.INNER)\
            .drop(const.PROGRAM_ID)\
            .groupBy([const.SPECIALTY_ID,const.CALENDAR_SEASON_ID])\
            .agg(sf.sum(TOTAL_SLOTS).alias(TOTAL_SLOTS))

    def __cal_total_invites__(self):

        # tip: total invites of a program
        # (PROGRAM_ID, SPECIALTY_ID, CALENDAR_SEASON_ID,TOTAL_INVITES)

        # (PROGRAM_ID, CALENDAR_SEASON_ID,TOTAL_INVITES)
        self.df_tip = self.df_invite\
            .drop(const.SPECIALTY_ID)\
            .groupBy([const.PROGRAM_ID, const.CALENDAR_SEASON_ID])\
            .agg(sf.count(const.CREATED_DATE).alias(TOTAL_INVITES))

        # tis: total invites of a specialty
        # (SPECIALTY_ID, CALENDAR_SEASON_ID,TOTAL_INVITES)
        self.df_tis = self.df_invite\
            .drop(const.PROGRAM_ID)\
            .groupBy([const.SPECIALTY_ID, const.CALENDAR_SEASON_ID])\
            .agg(sf.count(const.CREATED_DATE).alias(TOTAL_INVITES))

    def __cal_total_withdraw__(self):

        # twp: total withdraw of a program
        # (PROGRAM_ID, CALENDAR_SEASON_ID,TOTAL_WITHDRAW)
        self.df_twp = self.df_withdraw\
            .drop(const.SPECIALTY_ID)\
            .groupBy([const.PROGRAM_ID, const.CALENDAR_SEASON_ID])\
            .agg(sf.count(const.CREATED_DATE).alias(TOTAL_WITHDRAW))

        # tws: total withdraw of a specialty
        self.df_tws = self.df_withdraw\
            .drop(const.PROGRAM_ID)\
            .groupBy([const.SPECIALTY_ID, const.CALENDAR_SEASON_ID])\
            .agg(sf.count(const.CREATED_DATE).alias(TOTAL_WITHDRAW))

    def __cal_total_cancel__(self):

        self.df_tcp = self.df_cancel\
            .drop(const.SPECIALTY_ID)\
            .groupBy([const.PROGRAM_ID, const.CALENDAR_SEASON_ID])\
            .agg(sf.count(const.CREATED_DATE).alias(TOTAL_CANCEL))


    def __cal_first_invite_date__(self):

        # fidp: first invite day of program
        # (PROGRAM_ID, CALENDAR_SEASON_ID,FIRST_DATE)
        self.df_fidp = self.df_invite\
            .drop(const.SPECIALTY_ID)\
            .groupBy([const.PROGRAM_ID, const.CALENDAR_SEASON_ID])\
            .agg(sf.min(const.CREATED_DATE).alias(FIRST_INVITE_DAY))

        # fids: first invite day of specialty
        # (SPECIALTY_ID, CALENDAR_SEASON_ID,FIRST_DATE)
        self.df_fids = self.df_invite \
            .drop(const.PROGRAM_ID) \
            .groupBy([const.SPECIALTY_ID, const.CALENDAR_SEASON_ID]) \
            .agg(sf.min(const.CREATED_DATE).alias(FIRST_INVITE_DAY))


    def __cal_first_date_invites__(self):

        prg_condi = [self.df_invite[const.PROGRAM_ID] == self.df_fidp[const.PROGRAM_ID],
                 self.df_invite[const.CALENDAR_SEASON_ID] == self.df_fidp[const.CALENDAR_SEASON_ID],
                 self.df_invite[const.CREATED_DATE] == self.df_fidp[FIRST_INVITE_DAY]]

        # tfdip: first date invites of program
        # (PROGRAM_ID, CALENDAR_SEASON_ID,FIRST_DATE_INVITES)
        self.df_tfdip = self.df_invite\
            .drop(const.SPECIALTY_ID)\
            .join(self.df_fidp, prg_condi, const.INNER)\
            .drop(self.df_fidp[const.PROGRAM_ID])\
            .drop(self.df_fidp[const.CALENDAR_SEASON_ID])\
            .drop(self.df_invite[const.CREATED_DATE])\
            .groupBy([const.PROGRAM_ID, const.CALENDAR_SEASON_ID])\
            .agg(sf.count(FIRST_INVITE_DAY).alias(TOTAL_FIRST_DAY_INVITES))

        # (CALENDAR_SEASON_ID,CREATED_DATE, SPECIALTY_ID)
        spcl_condi = [self.df_invite[const.SPECIALTY_ID] == self.df_fids[const.SPECIALTY_ID],
                      self.df_invite[const.CALENDAR_SEASON_ID] == self.df_fids[const.CALENDAR_SEASON_ID],
                      self.df_invite[const.CREATED_DATE] == self.df_fids[FIRST_INVITE_DAY]]

        self.df_tfdis = self.df_invite\
            .drop(const.PROGRAM_ID)\
            .join(self.df_fids, spcl_condi, const.INNER)\
            .drop(self.df_fids[const.SPECIALTY_ID])\
            .drop(self.df_fids[const.CALENDAR_SEASON_ID])\
            .drop(self.df_invite[const.CREATED_DATE]) \
            .groupBy([const.SPECIALTY_ID, const.CALENDAR_SEASON_ID])\
            .agg(sf.count(FIRST_INVITE_DAY).alias(TOTAL_FIRST_DAY_INVITES))


    def __base_table__(self):
        self.__cal_total_slots__()
        self.__cal_total_invites__()
        self.__cal_total_withdraw__()
        self.__cal_total_cancel__()
        self.__cal_first_invite_date__()
        self.__cal_first_date_invites__()
        self.df_base = self.df_tsp\
            .join(self.df_tip,RptProgramAuditDao.__get_join_condi__(self.df_tsp,self.df_tip ), const.LEFT_OUTER) \
            .drop(self.df_tip[const.PROGRAM_ID]).drop(self.df_tip[const.CALENDAR_SEASON_ID]) \
            .join(self.df_twp, RptProgramAuditDao.__get_join_condi__(self.df_tsp, self.df_twp), const.LEFT_OUTER) \
            .drop(self.df_twp[const.PROGRAM_ID]).drop(self.df_twp[const.CALENDAR_SEASON_ID]) \
            .join(self.df_tcp, RptProgramAuditDao.__get_join_condi__(self.df_tsp, self.df_tcp), const.LEFT_OUTER) \
            .drop(self.df_tcp[const.PROGRAM_ID]).drop(self.df_tcp[const.CALENDAR_SEASON_ID])\
            .join(self.df_fidp, RptProgramAuditDao.__get_join_condi__(self.df_tsp, self.df_fidp), const.LEFT_OUTER) \
            .drop(self.df_fidp[const.PROGRAM_ID]).drop(self.df_fidp[const.CALENDAR_SEASON_ID])\
            .join(self.df_tfdip, RptProgramAuditDao.__get_join_condi__(self.df_tsp, self.df_tfdip), const.LEFT_OUTER) \
            .drop(self.df_tfdip[const.PROGRAM_ID]).drop(self.df_tfdip[const.CALENDAR_SEASON_ID])\
            .selectExpr(const.PROGRAM_ID,
                        const.CALENDAR_SEASON_ID,
                        self.__nvl__(TOTAL_SLOTS),
                        self.__nvl__(TOTAL_INVITES),
                        self.__nvl__(TOTAL_WITHDRAW),
                        self.__nvl__(TOTAL_CANCEL),
                        FIRST_INVITE_DAY,
                        self.__nvl__(TOTAL_FIRST_DAY_INVITES))

    """ Derived Columns """

    def __full_table__(self):

        # to: total overage
        expr_top = TOTAL_INVITES + " - " + TOTAL_SLOTS + " - " + TOTAL_WITHDRAW

        # tor : total overage rate (program)
        expr_tor = "round(" + TOTAL_INVITES + " / " + TOTAL_SLOTS + ", 2)"

        # tiafd: total invites after first day (program)
        expr_tiafd = TOTAL_INVITES + " - " + TOTAL_FIRST_DAY_INVITES

        # fdo: first day overage (program)
        expr_fdo = TOTAL_FIRST_DAY_INVITES + " - " + TOTAL_SLOTS

        # fdor: first day overage ratio (program)
        expr_fdor = "round(" + TOTAL_FIRST_DAY_INVITES + "/" + TOTAL_SLOTS + ", 2)"

        # afdo: after first day overage (program)
        expr_afdo = expr_tiafd + " - " + TOTAL_SLOTS

        # afdopr: after first day overage rate (program)
        expr_afdor = "round((" + expr_tiafd + ")/" + TOTAL_SLOTS + ", 2)"

        # pifd: percent invites first day (program)
        expr_pifd = "round(" + TOTAL_FIRST_DAY_INVITES + " * 100 / " + TOTAL_INVITES + ", 1)"

        # piafd: percent invites after first day
        expr_piafd = "round((" + expr_tiafd + ") * 100 / " + TOTAL_INVITES + ", 1)"

        # pia: percent interview accepted
        expr_pia = "round((" + TOTAL_INVITES + " - " + TOTAL_WITHDRAW + " - " + TOTAL_CANCEL + ") * 100 / " \
                   + TOTAL_INVITES + ", 1)"


        self.df_full = self.df_base.selectExpr(
            const.PROGRAM_ID,
            const.CALENDAR_SEASON_ID,
            TOTAL_SLOTS,
            TOTAL_INVITES,
            TOTAL_WITHDRAW,
            TOTAL_CANCEL,
            TOTAL_FIRST_DAY_INVITES,
            expr_tiafd + " as " + TOTAL_INVITES_AFTER_FIRST_DAY,
            expr_top + " as " + TOTAL_OVERAGE,
            expr_tor + " as " + TOTAL_OVERAGE_RATE,
            FIRST_INVITE_DAY,
            expr_fdo + " as " + FIRST_DAY_OVERAGE,
            expr_fdor + " as " + FIRST_DAY_OVERAGE_RATE,
            expr_afdo + " as " + AFTER_FIRST_DAY_OVERAGE,
            expr_afdor + " as " + AFTER_FIRST_DAY_OVERAGE_RATE,
            expr_pifd + " as " + PERCENT_INVITES_FIRST_DAY,
            expr_piafd + " as " + PERCENT_INVITES_AFTER_FIRST_DAY,
            expr_pia + " as " + PERCENT_INTERVIEWS_ACCEPTED)

        self.__write_df__(self.df_full, const.TARPT_PROGRAM_AUDIT_RPT)

    def load_program_invite_rpt(self):

        self.__base_table__()
        self.__full_table__()

