-- *****************************************************************************
-- Order Select Assignments
-- *****************************************************************************
SELECT aassg.ASSG_ID,
  aassg.PHYS_WHSE_ID,
  aassg.RPTG_ID,
  aassg.asgt_id,
  aassg.asta_id,
  aassg.ASSOC_ID,
  sasso.fname,
  sasso.lname,
  round((aassg.end_dtim - aassg.start_dtim) * 86400) as act_time,
  (substr(nvl(aassg.std_tim,'000:00:00'), 1, 3) * 60 * 60
  + substr(nvl(aassg.std_tim, '000:00:00'), 5, 2) * 60 
  + substr(nvl(aassg.std_tim,'000:00:00'), 8, 2)) as std,
  to_char(aassg.start_dtim, 'DD-MM-YYYY HH24:MI:SS') as aassgstart,
  to_char(aassg.end_dtim, 'DD-MM-YYYY HH24:MI:SS') as aassgend,
  sum((aseld.prod_qty - nvl(aseld.out_qty,0))/decode(aseld.unit_ship_cse,0,1,aseld.unit_ship_cse)) as picks,
  sum(nvl(shorth_ext1.not_ship_qty,0)/decode(shorth_ext1.unit_ship_cse,0,1,shorth_ext1.unit_ship_cse)) as shorts,
  max(aseld.jcfn_id) as jcfn_id,
  max(aseld.jcsf_id) as jcfs_id,
  (null) as lhac_id,
  aassg.last_sect_id,
  max(aseld.spmd_id) as spmd_id,
  (null) as wust_id,
  (null) as rpln_mthd,
  (null) as lhty_id,
  (null) as prod_id,
  (null) as Cases,
  (null) as route,
  (null) as customer,
  (null) as close_time,
  (null) as Name,
  (null) as cp, (null) as lo, (null) as os, (null) as rc, (null) as dc, (null) as cr, (null) as ec, (null) as ot, (null) as pc, (null) as p7, (null) as f2, (null) as c1, (null) as oc, (null) as ol, (null) as bc, (null) as bd,
  (null) as plt_spc,
  (null) as lp_cnt,
  (null) as rec_name,
  (null) as rec_verify_time,
  (null) as rec_pallets,
  (null) as rec_cases
from aassg,
     sasso,
     aseld left outer join shorth_ext1 on shorth_ext1.seld_id = aseld.seld_id
where aassg.assoc_id = sasso.assoc_id
and aassg.asgt_id = 'S'
and trunc(aassg.end_dtim) = :dateddmmmyy
and aseld.assg_id = aassg.assg_id
group by aassg.assg_id,
  aassg.PHYS_WHSE_ID,
  aassg.RPTG_ID,
  aassg.asgt_id,
  aassg.asta_id,
  aassg.assoc_id,
  sasso.fname,
  sasso.lname,
  aassg.end_dtim,
  aassg.start_dtim,
  aassg.std_tim,
  aassg.last_sect_id

union all

-- *****************************************************************************
-- All indirect assignments plus fork and receiving assignments
-- *****************************************************************************
select aothd.assg_id,
  aothd.phys_whse_id as wh,
  sasso.rptg_id as RPTG_ID,
  aothd.asgt_id,
  (null) as asta_id,
  aothd.ASSOC_ID,
  sasso.FNAME,
  sasso.lname,
  round((aothd.complete_dtim - aothd.start_dtim) * 86400) as act_time,
  (SUBSTR(NVL(aothd.std_tim,'000:00:00'), 1, 3) * 60 * 60
  + substr(nvl(aothd.std_tim, '000:00:00'), 5, 2) * 60 
  + substr(nvl(aothd.std_tim,'000:00:00'), 8, 2)) as std,
  to_char(aothd.start_dtim, 'DD-MM-YYYY HH24:MI:SS') as aassgstart,
  to_char(aothd.complete_dtim, 'DD-MM-YYYY HH24:MI:SS') as aassgend,
  (null) as picks,
  (null) as shorts,
  aothd.jcfn_id,
  aothd.jcsf_id,
  aothd.lhac_id,
  (null) as last_sect_id,
  (null) as spmd_id,
  wust_id,
  rpln_mthd,
  lhty_id,
  prod_id,
  ROUND((aothd.prod_qty/decode(aothd.unit_ship_cse,0,1,aothd.unit_ship_cse)),0) Cases,
  (null) as route,
  (null) as customer,
  (null) as close_time,
  (null) as Name,
  (null) as cp, (null) as lo, (null) as os, (null) as rc, (null) as dc, (null) as cr, (null) as ec, (null) as ot, (null) as pc, (null) as p7, (null) as f2, (null) as c1, (null) as oc, (null) as ol, (null) as bc, (null) as bd,
  (null) as plt_spc,
  (null) as lp_cnt,
  (null) as rec_name,
  (null) as rec_verify_time,
  (null) as rec_pallets,
  (null) as rec_cases
 
from aothd left outer join sasso on sasso.assoc_id = aothd.assoc_id
where (aothd.jcfn_id != 'SH' or aothd.jcfn_id is null)
and trunc(aothd.complete_dtim) = :dateddmmmyy

union all

-- *****************************************************************************
-- Shipping assignments
-- *****************************************************************************
select 
  aothd.assg_id as assg_id,
  aothd.phys_whse_id as wh,
  sasso.rptg_id as rptg_id,
  aothd.asgt_id as asgt_id,
  (null) as asta_id,
  aothd.assoc_id as assoc_id,
  sasso.fname as fname,
  sasso.lname as lname,
  round((aassg.end_dtim - aassg.start_dtim) * 86400) as act_time,
  (null) as std,
  to_char(aassg.start_dtim, 'DD-MM-YYYY HH24:MI:SS') as aassgstart,
  to_char(aassg.end_dtim, 'DD-MM-YYYY HH24:MI:SS') as aassgend,
  (null) as picks,
  (null) as shorts,
  aothd.jcfn_id as jcfn_id,
  aothd.jcsf_id as jcsf_id,
  (null) as lhac_id,
  (null) as last_sect_id,
  (null) as spmd_id,
  aothd.wust_id as wust_id,
  aothd.rpln_mthd as rpln_mthd,
  (null) as lhty_id,
  (null) as prod_id,
  (null) as cases,
  (null) as route,
  (null) as customer,
  (null) as close_time,
  (null) as Name,
  (null) as cp, (null) as lo, (null) as os, (null) as rc, (null) as dc, (null) as cr, (null) as ec, (null) as ot, (null) as pc, (null) as p7, (null) as f2, (null) as c1, (null) as oc, (null) as ol, (null) as bc, (null) as bd,
  (null) as plt_spc,
  (null) as lp_cnt,
  (null) as rec_name,
  (null) as rec_verify_time,
  (null) as rec_pallets,
  (null) as rec_cases

from aothd ,
     sasso, aassg
where sasso.assoc_id = aothd.assoc_id
and trunc(aothd.complete_dtim) = :dateddmmmyy
and jcfn_id = 'SH'
and aothd.start_dtim is not null
and aassg.assg_id = aothd.assg_id
group by aothd.assg_id,
aothd.phys_whse_id,
sasso.rptg_id,
aothd.asgt_id,
aothd.assoc_id,
sasso.fname,
sasso.lname,
aothd.jcfn_id,
aothd.jcsf_id,
aothd.wust_id,
aothd.rpln_mthd,
aassg.end_dtim,
aassg.start_dtim

union all

-- *****************************************************************************
-- Load Details
-- *****************************************************************************
select
  (null) as assg_id,
  wh_id as wh,
  (null) as rptg_id,
  (null) as asgt_id,
  (null) as asta_id,
  (null) as assoc_id,
  (null) as fname,
  (null) as lname,
  (null) as act_time,
  (null) as std,
  (null) as aassgstart,
  (null) as aassgend,
  (null) as picks,
  (null) as shorts,
  (null) as jcfn_id,
  (null) as jcsf_id,
  (null) as lhac_id,
  (null) as last_sect_id,
  (null) as spmd_id,
  (null) as wust_id,
  (null) as rpln_mthd,
  (null) as lhty_id,
  (null) as prod_id,
  (null) as cases,
  Route,
  Customer,
  Close_time,
  Name,
  sum(cp) as cp, sum(lo) as lo, sum(os) as os, sum(rc) as rc, sum(dc) as dc, sum(cr) as cr, sum(ec) as ec, sum(ot) as ot, sum(pc) as pc, sum(p7) as p7, sum(f2) as f2, sum(c1) as c1, sum(oc) as oc, sum(ol) as ol, sum(bc) as bc, sum(bd) as bd,
  total_pallet_spaces as plt_spc,
  sum(Qty) as LP_CNT,
  (null) as rec_name,
  (null) as rec_verify_time,
  (null) as rec_pallets,
  (null) as rec_cases
  
from
(SELECT 
irtst.whse_id as WH_id,
irtst.route_id as Route,
irtst.cust_id as Customer,
to_char(irtst.change_dtim,'HH24:MI:SS')  as Close_time,
trim(suser.fname)  || ' '  || trim(suser.lname) as Name,
case when trim(idtnd.cnty_id) like('CP') then idtnd.trans_qty end as CP,
case when trim(idtnd.cnty_id) like('LO') then idtnd.trans_qty end as LO,
case when trim(idtnd.cnty_id) like('OS') then idtnd.trans_qty end as OS,
case when trim(idtnd.cnty_id) like('RC') then idtnd.trans_qty end as RC,
case when trim(idtnd.cnty_id) like('DC') then idtnd.trans_qty end as DC,
case when trim(idtnd.cnty_id) like('CR') then idtnd.trans_qty end as CR,
case when trim(idtnd.cnty_id) like('EC') then idtnd.trans_qty end as EC,
case when trim(idtnd.cnty_id) like('OT') then idtnd.trans_qty end as OT,
case when trim(idtnd.cnty_id) like('PC') then idtnd.trans_qty end as PC,
case when trim(idtnd.cnty_id) like('P7') then idtnd.trans_qty end as P7,
case when trim(idtnd.cnty_id) like('F2') then idtnd.trans_qty end as F2,
case when trim(idtnd.cnty_id) like('C1') then idtnd.trans_qty end as C1,
case when trim(idtnd.cnty_id) like('OC') then idtnd.trans_qty end as OC,
case when trim(idtnd.cnty_id) like('OL') then idtnd.trans_qty end as OL,
case when trim(idtnd.cnty_id) like('BC') then idtnd.trans_qty end as BC,
case when trim(idtnd.cnty_id) like('BD') then idtnd.trans_qty end as BD,
irtst_ext1.tot_pal_spaces as Total_pallet_spaces,
idtnd.trans_qty as Qty
FROM irtst_ext1, irtst, suser, idtnd_ext1, idtnd, icnty
where irtst.rtst_id = irtst_ext1.rtst_id
AND irtst.change_user = suser.user_id
AND trunc(irtst.actual_depdt) = :dateddmmmyy
AND irtst.rtst_id = idtnd_ext1.rtst_id
AND idtnd_ext1.idtnd_key = idtnd.key
AND idtnd.cnty_id = icnty.cnty_id
AND icnty.lang_id = 'E') z
group by
wh_id,  route, customer, close_time, name, total_pallet_spaces

union all

-- *****************************************************************************
-- Receiving Details
-- *****************************************************************************
select 
  (null) as assg_id,
  irct.whse_id as wh,
  (null) as rptg_id,
  (null) as asgt_id,
  (null) as asta_id,
  (null) as assoc_id,
  (null) as fname,
  (null) as lname,
  (null) as act_time,
  (null) as std,
  (null) as aassgstart,
  (null) as aassgend,
  (null) as picks,
  (null) as shorts,
  (null) as jcfn_id,
  (null) as jcsf_id,
  (null) as lhac_id,
  (null) as last_sect_id,
  (null) as spmd_id,
  (null) as wust_id,
  (null) as rpln_mthd,
  (null) as lhty_id,
  (null) as prod_id,
  (null) as cases,
  (null) as route,
  (null) as customer,
  (null) as close_time,
  (null) as Name,
  (null) as cp, (null) as lo, (null) as os, (null) as rc, (null) as dc, (null) as cr, (null) as ec, (null) as ot, (null) as pc, (null) as p7, (null) as f2, (null) as c1, (null) as oc, (null) as ol, (null) as bc, (null) as bd,
  (null) as plt_spc,
  (null) as lp_cntm,
  trim(suser.fname)  || ' '  || trim(suser.lname) as rec_name,
  to_char(irct.verify_dtim,'DD-MM-YYYY HH24:MI:SS')  as rec_verify_time,
  count(irctd.lic_plt_id) as rec_pallets,
  sum(irctd.rct_qty/iprdd.unit_ship_cse) as rec_cases

FROM irct,irctd,suser,iprdd
WHERE irct.po_id = irctd.po_id
AND irct.rcpt_id = irctd.rcpt_id
AND irct.dc_id = irctd.dc_id
AND irct.whse_id = irctd.whse_id
and irct.verify_user = suser.user_id
AND trunc(irct.verify_dtim) = :dateddmmmyy
and iprdd.prod_id = irctd.prod_id
and iprdd.prdd_id = irctd.prdd_id
group by
irct.whse_id,
trim(suser.fname)  || ' '  || trim(suser.lname),
to_char(irct.verify_dtim,'DD-MM-YYYY HH24:MI:SS'),
irct.po_id

order by
assoc_id,
aassgstart
