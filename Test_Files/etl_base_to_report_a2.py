#########################################################################################
# Date           : 16/10/2018
# Author         : asfad
# Company        : asdxc
# Purpose        : Spark job for generating A2 report tables.
# Usage          : e.g. spark-submit etl_base_to_report_a2.py 2019-06-01
#########################################################################################
# Revision History:
#
# version   author          date        description of change
# 0.1       asdasd asf    16/10/2018  first draft of script
# 0.8       asfd            24/06/2018  Refactor
# 1.0       fasfasd           01/07/2019  Production Ready for C&A!
# 1.1       fasfasd           05/07/2019  Seperate SVT out
#########################################################################################
# Input tables:
#   rpt_udp.rpt_non_financials
#   rpt_udp.ref_branch_code_ctd
# Output table(s):
#   rpt_udp.rpt_assessment_clubs_association (append)
#   rpt_udp.rpt_assessment_clubs_association_aggr (append)
#########################################################################################


import sys
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark_llap import HiveWarehouseSession

spark_origin = SparkSession.builder.enableHiveSupport().getOrCreate()
spark_origin.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
spark_origin.conf.set('hive.exec.dynamic.partition.mode', 'nonstrict')
spark = HiveWarehouseSession.session(spark_origin).build()
sc = spark.sparkContext

current_run = str(sys.argv[1])
dt_current_run = datetime.datetime.strptime(current_run, '%Y-%m-%d').date()
#First day of current month
dt_end = dt_current_run.replace(day = 1)
#Last day of previous month
dt_period_month = dt_end - datetime.timedelta(days = 1)
#First day of previous month
dt_start = dt_period_month.replace(day = 1)

#########################################################################################
# Clubs & Associations. (Cognos Report: IRIN_DWER_AM212b)
#########################################################################################
branch_ref = spark.table('rpt_udp.ref_branch_code_ctd').select(
    col('branch_code').alias('cd_grade'),
    col('branch_desc')
)

rpt_assessment_clubs_association_df1 = spark.table('rpt_udp.rpt_non_financials').filter(
    (col('dt_effective') >= dt_start) & (col('dt_effective') < dt_end)
).filter("""
    cd_trans_status = 6
    AND cd_transaction IN (623,624,625,626,627,628,629)
    AND cd_entity_class IN (410,420,430,431,440,450,451,452,453,454,470,471,480,490)
""")

rpt_assessment_clubs_association = rpt_assessment_clubs_association_df1.alias('a').join(
    branch_ref.alias('b'), 'cd_grade', 'left'
).select(
    col('a.id_entity').alias('entity_id'),
    col('a.id_transaction'),
    year(col('dt_tax_pd_begin')).alias('ya'),
    year(col('a.dt_effective')).alias('fy'),
    col('a.dt_effective').alias('period_month'),
    col('b.branch_desc').alias('branch'),
    col('a.cd_transaction'),
    col('a.dt_effective'),
    col('a.am_prev_tax'),
    col('a.am_nettaxpayable'),
    col('a.am_nta'),
    col('a.cd_grade').alias('grade_code'),
    col('a.cd_entity_class').alias('class_code'),
    col('a.in_record_type').alias('record_type'),
    col('a.type_of_fund')
).withColumn(
    'period_month',last_day('period_month')
).withColumn(
    'branch', when(col('branch').isNull(), 'Others').otherwise(col('branch'))
).withColumn(
    "fy",
    when(
        month("period_month") >= 4, year("period_month")
    ).otherwise(
        year("period_month") - 1
    )
).withColumn(
    "period_quarter",
    when(
        month("a.dt_effective").isin([4,5,6]), lit(1)
    ).when(
        month("a.dt_effective").isin([7,8,9]), lit(2)
    ).when(
        month("a.dt_effective").isin([10,11,12]), lit(3)
    ).when(
        month("a.dt_effective").isin([1,2,3]), lit(4)
    ).otherwise(
        lit(None)
    )
).withColumn(
    'cdtransaction', # same name as the variable in Cognos report
    expr("""
        CASE
            when cd_transaction IN (623, 624) AND am_nta <> 0 THEN 1
            WHEN cd_transaction IN (625, 626) THEN 2
            WHEN cd_transaction IN (623, 624) AND am_nta = 0 THEN 3
            WHEN cd_transaction IN (627, 628) THEN 4
            WHEN cd_transaction IN (629) THEN 5
            ELSE 6
        END
    """)
).withColumnRenamed('cd_transaction', 'transaction_code')

trans_df = rpt_assessment_clubs_association.withColumn(
    'assessment_type',
    when(
        col('cdtransaction') == 1, 'Original Asst with tax'
    ).when(
        col('cdtransaction') == 2, 'Additional Assessments'
    ).when(
        col('cdtransaction') == 3, 'Original Asst non-tax'
    ).when(
        col('cdtransaction') == 4, 'Amended Assessments'
    ).when(
        col('cdtransaction') == 5, 'Repayment'
    ).otherwise(lit(None))
).select(
    'fy',
    'period_month',
    'period_quarter',
    'assessment_type',
    'branch',
    'entity_id',
    'am_prev_tax',
    'am_nettaxpayable',
    'am_nta',
    'dt_effective',
    'transaction_code',
    'cdtransaction',
    'grade_code',
    'class_code',
    'record_type',
    'type_of_fund'
).withColumn(
    'create_timestamp',current_timestamp()
)

trans_target = 'rpt_udp.rpt_assessment_clubs_association'
trans_df \
    .select(spark.table(trans_target).columns) \
    .write \
    .format(HiveWarehouseSession().HIVE_WAREHOUSE_CONNECTOR) \
    .option('table', trans_target) \
    .option('partition', 'period_month') \
    .save()

final_aggr_df = trans_df.groupBy(
    col('fy'),
    col('period_month'),
    col('period_quarter'),
    col('assessment_type'),
    col('branch'),
    col('transaction_code'),
    col('cdtransaction'),
    col('grade_code'),
    col('class_code'),
    col('record_type'),
    col('type_of_fund')
).agg(
    sum(
        when(
            (col('am_prev_tax') > 0) & (col('cdtransaction') == 5), col('am_nettaxpayable')
        ).when(
            (col('am_prev_tax') < 0) & (col('cdtransaction') == 5), (col('am_nettaxpayable') - col('am_prev_tax'))
        ).otherwise(col('am_nta'))
    ).alias('tax_amount'),
    sum(lit(1)).alias('no_of_assessment')
).withColumn(
    'source_system',lit('IRIN')
).withColumn(
    'create_timestamp',current_timestamp()
).select(
    'fy',
    'period_month',
    'period_quarter',
    'assessment_type',
    'branch',
    'tax_amount',
    'no_of_assessment',
    'transaction_code',
    'grade_code',
    'class_code',
    'record_type',
    'type_of_fund',
    'source_system',
    'create_timestamp'
)

aggr_target = 'rpt_udp.rpt_assessment_clubs_association_aggr'
final_df \
    .select(spark.table(aggr_target).columns) \
    .write \
    .format(HiveWarehouseSession().HIVE_WAREHOUSE_CONNECTOR) \
    .option('table', aggr_target) \
    .option('partition', 'period_month') \
    .save()
