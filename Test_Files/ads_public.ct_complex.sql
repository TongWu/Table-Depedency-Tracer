--#######################################################################################
--Date           : 18/02/2020
--Author         : wadcwaqewqdasdasdasdwqwre
--Company        : asdacxxcasdasd
--Purpose        : DDL for creating view ads_public.ct_complex
--Usage          : beeline -f ads_public.ct_complex.sql
--#######################################################################################
--Revision History:
--ver  author    date        description of change
--1.0  asdih    25/02/2020  Initial schema.
--2.0  wafdas     22/06/2020  Modified the tax_dv tables naming convention as udp_**_src
--
--#######################################################################################


CREATE VIEW ads_public.ct_complex AS
SELECT 
    ct_name,
    ct_pattern,
    ct_num_cd1,
    ct_num_cd2,
    ct_alp_cd1,
    ct_short_desc,
    ct_long_desc,
    ct_indicator1,
    ct_indicator2,
    ct_amount1,
    ct_amount2,
    ct_int_cd1,
    ct_int_cd2,
    ct_text1,
    ct_text2,
    ct_date1,
    ct_date2,
    ct_sort_order,
    ct_effective_st_dt,
    ct_effective_ed_dt,
    ct_varchar1,
    ct_varchar2,
    ct_varchar3,
    ct_varchar4,
    ct_varchar5,
    ct_varchar6,
    ct_varchar7,
    ct_varchar8,
    ct_varchar9,
    ct_varchar10,
    ct_varchar11,
    ct_varchar12,
    ct_varchar13,
    ct_varchar14,
    ct_varchar15,
    ct_varchar16,
    ct_varchar17,
    id_updated_by     
FROM tax_dv.udp_ct_complex_irin
WHERE dv_partition_dt = (
    SELECT MAX(dv_partition_dt) FROM tax_dv.udp_ct_complex_irin
)
;
