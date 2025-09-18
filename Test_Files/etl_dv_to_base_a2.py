#########################################################################################
# Date           : 20/06/2019
# Author         : Zasd
# Company        : asd
# Purpose        : Spark job for generating A2 base table.
# Usage          : e.g. spark-submit etl_dv_to_base_a2.py 2019-06-01
#########################################################################################
# Revision History:
#
# ver   author      date        description of change  
# 0.1   sadasd        20/06/2019  Spark job for generating A2 base table.
# 0.2   ac       20/06/2019  Continue
# 1.0   ac       01/07/2019  Production Ready!
#########################################################################################
# Input tables:
#   tax_dv.sat_non_fin_req_irin
#   tax_dv.hub_non_fin_req
#   tax_dv.sat_forms_hdr_irin
#   tax_dv.sat_forms_dtl_irin
#   tax_dv.sat_entity_id_irin
#   tax_dv.sat_entity_irin
#   tax_dv.hub_entity
#   tax_dv.sat_acct_notice_req_irin
#   tax_dv.hub_acct_notice_req
#
# Output table(s):
#   rpt_udp.rpt_non_financials (append)
#
#########################################################################################

import sys
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
spark.conf.set('hive.exec.dynamic.partition.mode', 'nonstrict')

current_run = str(sys.argv[1]) # e.g. '2018-12-04'

dt_current_run = datetime.datetime.strptime(current_run, '%Y-%m-%d').date()
#First day of current month
delta_end = dt_current_run.replace(day = 1)
#Last day of previous month
dt_period_month = delta_end - datetime.timedelta(days = 1)
#First day of previous month
delta_start = dt_period_month.replace(day = 1)

current_year = dt_current_run.year
year_start = current_year - 7

# NOTE: DELTA filter should be [delta_start, delta_end)

last_update_tms = concat_ws(' ', 'dt_last_update', 'tm_last_update').cast('timestamp')

#########################################################################################
# Load non_fin_req
#########################################################################################
sat_non_fin_req_irin_df = spark.table('tax_dv.sat_non_fin_req_irin').filter(
    (col('dt_effective') >= delta_start) & (col('dt_effective') < delta_end)
).withColumn(
    "tm_last_update", last_update_tms
).withColumn(
    'row_no',
    row_number().over(
        Window.partitionBy('hub_id_non_fin_req', 'id_transaction').orderBy(desc('tm_last_update'), desc('sat_load_dts'))
    )
).filter('row_no = 1').select(
    'hub_id_non_fin_req',
    'id_trans_source',
    'am_tax_liab',
    'id_transaction',
    col('am_liability').alias('am_nta'),
    'cd_transaction',
    'dt_last_update',
    'tm_last_update',
    'dt_effective',
    'versionnumber'
)

stg_non_financials_df = spark.table('tax_dv.hub_non_fin_req').alias('hub').filter(
    col('hub.cd_tax') == 107
).join(
    sat_non_fin_req_irin_df.alias('sat'), 'hub_id_non_fin_req', 'inner'
).select(
    'hub.id_internal',
    'hub.dt_tax_pd_begin',
    'hub.cd_tax',
    'sat.id_trans_source',
    'sat.am_tax_liab',
    'sat.id_transaction',
    'sat.am_nta',
    'sat.cd_transaction',
    'sat.dt_effective'
)

#########################################################################################
# Load forms data
#########################################################################################
sat_forms_hdr_df = spark.table('tax_dv.sat_forms_hdr_irin').withColumn(
    'tm_last_update', last_update_tms
).filter(
    col('cd_tax').isin([107, 170]) &
    (col('cd_asmt_status') != 11) &
    (col('dt_last_update') >= delta_start) & (col('dt_last_update') < delta_end) &
    (col('yr_form') <= current_year) & (col('yr_form') >= year_start)
).withColumn(
    'row_no',
    row_number().over(Window.partitionBy('hub_id_forms').orderBy(
        desc('tm_last_update'), desc('sat_load_dts'))
    )
).filter(
    'row_no = 1 AND sat_rec_src != "IRIN_DELETE"'
)

full_forms_hdr_df = sat_forms_hdr_df.alias('sat').join(
    spark.table('tax_dv.hub_forms').alias('hub'),
    'hub_id_forms',
    'inner'
).select(
    'hub.id_forms_seq',
    'hub.id_internal',
    'sat.hub_id_forms',
    'sat.dt_tax_pd_begin',
    'sat.dt_tax_pd_end',
    'sat.cd_update_source',
    'sat.cd_update_reason',
    'sat.id_updated_by',
    'sat.id_transaction',
    'sat.yr_form',
    'sat.id_rtn_seq',
    'sat.cd_asmt_status',
    'sat.dt_created',
    'sat.cd_form',
    'sat.cd_tax',
    'sat.sq_assessment',
    'sat.dt_last_update',
    'sat.tm_last_update',
    'sat.versionnumber'
)

sat_forms_detail_df = spark.table('tax_dv.sat_forms_dtl_irin').withColumn(
    'sq_split', expr("CASE WHEN sq_split = 0 OR sq_split > 3 THEN 4 ELSE sq_split END")
).filter(
    col('sq_split').isin([1, 2, 3, 4])
)

full_forms_df = full_forms_hdr_df.alias('hdr').join(
    sat_forms_detail_df.alias('dtl'),
    'hub_id_forms',
    'inner'
).select(
    'hdr.*',
    'hdr.id_internal',
    'dtl.sq_split',
    'dtl.tx_line_items',
    col('dtl.versionnumber').alias('dtl_vernum'),
    'dtl.sat_load_dts'
)

forms_splited_xml_df = full_forms_df.withColumn(
    'row_no',
    row_number().over(Window.partitionBy('hub_id_forms', 'sq_split').orderBy(desc('sat_load_dts'), desc('dtl_vernum')))
).filter('row_no = 1')

# Collect tx_line_items as an array ordered by sq_split ASC within the same hub_id_forms.
# Then concat the list elements to one whole xml string.
window = Window.partitionBy('hub_id_forms').orderBy('sq_split').rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing # IMPORTANT to make window work.
)
ca_form_df = forms_splited_xml_df.withColumn(
    'whole_xml', concat_ws('', collect_list('tx_line_items').over(window))
).filter('sq_split = 1').withColumn(
    'whole_xml', regexp_replace('whole_xml', '\s+', '')  # Remove all whitespaces.
)

ca_form_xml_tags = [
    ('NetTaxPayable', 'am_nettaxpayable'), 
    ('GenericLessPrevAsst', 'am_prev_tax')
]

for (tag, col_name) in ca_form_xml_tags:
    pattern = '<' + tag + '>(.*?)</' + tag + '>'
    ca_form_df = ca_form_df.withColumn(col_name, regexp_extract('whole_xml', pattern, 1))

stg_net_tax_payable_ref_df = ca_form_df.select(
    'hub_id_forms',
    'id_internal',
    'id_transaction',
    'dt_tax_pd_begin',
    'am_nettaxpayable',
    'am_prev_tax'
).withColumn(
    'am_nettaxpayable',
    regexp_replace('am_nettaxpayable', 'S', '').cast(DecimalType(13, 2))
).withColumn(
    'am_prev_tax', 
    regexp_replace('am_prev_tax', 'S', '').cast(DecimalType(13, 2))
).fillna(
    # [20221216] - update fillna col from net_tax_payable to am_nettaxpayable
    0, subset = [
                #'net_tax_payable',
                'am_nettaxpayable', 
                'am_prev_tax'
                ]
)


#########################################################################################
# Load entity
#########################################################################################
entity_window = Window.partitionBy('hub_id_entity').orderBy(desc('tm_last_update'), desc('sat_load_dts'))

sat_entity_id = spark.table('tax_dv.sat_entity_id_irin').filter(
    col('in_primary_id') == 'Y'
).withColumn(
    'tm_last_update', last_update_tms
).withColumn(
    'row_no', row_number().over(entity_window)
).filter('row_no = 1')

sat_entity = spark.table('tax_dv.sat_entity_irin').withColumn(
    'tm_last_update', last_update_tms
).withColumn(
    'row_no', row_number().over(entity_window)
).filter('row_no = 1')

stg_entity_ref = spark.table('tax_dv.hub_entity').alias('hub').join(
    sat_entity.alias('sat_1'), 'hub_id_entity', 'inner'
).join(
    sat_entity_id.alias('sat_2'), 'hub_id_entity', 'inner'
).select(
    'hub.id_internal',
    'sat_1.cd_entity_class',
    'sat_1.in_record_type',
    'sat_1.cd_grade',
    'sat_2.id_entity'
)

#########################################################################################
# Load stg_acct_notice_req_ref
#########################################################################################
sat_acct_notice_req_irin_df = spark.table('tax_dv.sat_acct_notice_req_irin').withColumn(
    'tm_last_update', last_update_tms
).withColumn(
    'row_no',
    row_number().over(Window.partitionBy('hub_id_acct_notice_req').orderBy(
        desc('tm_last_update'), desc('sat_load_dts'))
    )
).filter('row_no = 1')

stg_acct_notice_req_ref = spark.table('tax_dv.hub_acct_notice_req').alias('hub').join(
    sat_acct_notice_req_irin_df.alias('sat'), 'hub_id_acct_notice_req', 'inner'
).select(
    'hub.id_internal',
    'hub.id_transaction',
    'sat.cd_trans_status'
)

#########################################################################################
# Join together
#########################################################################################

rpt_non_financials_df1 = stg_non_financials_df.alias('main').join(
    stg_net_tax_payable_ref_df.alias('t2'),
    ['id_transaction', 'id_internal', 'dt_tax_pd_begin'],
    'left'
).join(
    stg_entity_ref.alias('t3'), 'id_internal', 'left'
).join(
    stg_acct_notice_req_ref.alias('t4'), 'id_transaction', 'left'
).select(
    'main.*',
    't2.am_nettaxpayable',
    't2.am_prev_tax',
    't3.cd_entity_class',
    't3.in_record_type',
    't3.cd_grade',
    't3.id_entity',
    't4.cd_trans_status'
)

#########################################################################################
# Append extra columns and Persistence
#########################################################################################
final_df = rpt_non_financials_df1.withColumn(
    "in_complete", lit("Y")
).withColumn(
    "type_of_fund", lit("Clubs & Associations")
).withColumn(
    'period_month', lit(dt_period_month)
).withColumn(
    "create_timestamp", current_timestamp()
).select(
    'id_transaction',
    'id_internal',
    'dt_tax_pd_begin',
    'cd_tax',
    'id_trans_source',
    'am_tax_liab',
    'am_nta',
    'cd_transaction',
    'dt_effective',
    'am_nettaxpayable',
    'am_prev_tax',
    'cd_entity_class',
    'in_record_type',
    'cd_grade',
    'id_entity',
    'cd_trans_status',
    'in_complete',
    'type_of_fund',
    'period_month',
    'create_timestamp'
)

target = 'rpt_udp.rpt_non_financials'
final_df.select(spark.table(target).columns).write.insertInto(target, overwrite=True)

