// import OverViewMaster from "../models/overview.model.js";
// import pool from "../lib/postgressConnect.js";

// export const handleoverviewdata = async ( transaction_date ) => {
//     try {
//       let dataObject = {};

//       const isdataPresent = await OverViewMaster.findOne({
//         date: transaction_date,
//       });
//       if (isdataPresent !== null) {
//         let dataObject = isdataPresent.dataObject;
//          return dataObject;
//       } else {

//         const query2 = `select distinct * from ((select asset_class,'pending_txn_count' as status,
//         substring(trdt, 1, 10) as trdt,
//         count(
//           distinct(
//             concat(
//               coalesce(scheme, ''),
//               coalesce(plan, ''),
//               coalesce(branch, ''),
//               coalesce(trno, ''),
//               coalesce(folio, '')
//             )
//           )
//         ) as count,
//         sum(cast(cramt as decimal)) as amount
//       from dbo.non_batchclosed_12_23_s
//       where substring(trdt, 1, 10) <= '${transaction_date}'
//       group by 1,
//         2,3
//       order by trdt desc
//       limit 4)
//       union
//       (select asset_class,'completed_txn_count' as status,
//         substring(trdt, 1, 10) as trdt,
//         count(
//           distinct(
//             concat(
//               coalesce(scheme, ''),
//               coalesce(plan, ''),
//               coalesce(branch, ''),
//               coalesce(trno, ''),
//               coalesce(folio, '')
//             )
//           )
//         ) as count,
//         sum(cast(cramt as decimal)) as amount
//       from dbo.batchclosed_11_24_s
//       where substring(trdt, 1, 10) <= '${transaction_date}'
//       group by 1,
//         2,3
//       order by trdt desc
//       limit 4)
//            )x
//     order by trdt desc` //transaction history

//         //optimize
//         const query4 = `with appl3_partition AS (
//           SELECT *,
//                  ROW_NUMBER() OVER (PARTITION BY app_fund, app_acno
//       ORDER BY concat(cdc_timestamp,ar_h_change_seq) DESC) AS rn
//             FROM dbo.appl3_partition
//       )

//       select distinct a.*,
//         concat(
//           coalesce(b.app_fname, ''),
//           coalesce(b.app_mname, ''),
//           coalesce(b.app_lname, '')
//         ) as InvName
//       from (
//           (
//             Select distinct 'l_50' as tag,
//               a.asset_class,
//               a.fund,
//               a.scheme,
//               a.plan,
//               a.branch,
//               a.trno,
//               a.folio,
//               replace(a.ihno, '.0', '') as ihno,
//               sum(cast(a.cramt as decimal)) as amount
//             from dbo.non_batchclosed_12_23_s a
//             where substring(a.trdt, 1, 10) = '2024-03-28'
//               and asset_class = 'LIQUID'
//             group by 1,
//               2,
//               3,
//               4,
//               5,
//               6,
//               7,
//               8,
//               9
//             order by 9 desc
//             limit 50
//           )
//           union
//           (
//             Select distinct 'nl_50' as tag,
//               a.asset_class,
//               a.fund,
//               a.scheme,
//               a.plan,
//               a.branch,
//               a.trno,
//               a.folio,
//               replace(a.ihno, '.0', '') as ihno,
//               sum(cast(a.cramt as decimal)) as amount
//             from dbo.non_batchclosed_12_23_s a
//             where substring(a.trdt, 1, 10) = '2024-03-28'
//               and asset_class = 'NON LIQUID'
//             group by 1,
//               2,
//               3,
//               4,
//               5,
//               6,
//               7,
//               8,
//               9
//             order by 9 desc
//             limit 50
//           )
//         ) a
//         left join appl3_partition b on a.fund=b.app_fund and a.folio=b.app_acno and b.rn=1 order by 9 desc`; //pending transattion count

//         const query5 = `select distinct asset_class,
//         (
//           case
//             when (
//               (asset_class) = 'LIQUID'
//               and (diff - h_count) <= 0
//             ) then 'with in TAT'
//             when (
//               (asset_class) = 'LIQUID'
//               and (diff - h_count) >= 1
//             ) then 'beyond TAT'
//             when (
//               (asset_class) = 'NON LIQUID'
//               and (diff - h_count) <= 1
//             ) then 'with in TAT'
//             when (
//               (asset_class) = 'NON LIQUID'
//               and (diff - h_count) = 2
//             ) then 'beyond TAT'
//             when (
//               (asset_class) = 'NON LIQUID'
//               and (diff - h_count) > 2
//             ) then 'beyond TAT' else 'other'
//           end
//         ),
//         trtype,
//         sum(count)
//       from (
//           select distinct asset_class,
//             DATE_PART(
//               'day',
//               nullif(cast(batchclosedt as text), '')::timestamp - nullif(navdt, '')::timestamp
//             ) diff,
//             trtype,
//             count(
//               distinct(
//                 concat(
//                   coalesce(scheme, ''),
//                   coalesce(plan, ''),
//                   coalesce(branch, ''),
//                   coalesce(trno, ''),
//                   coalesce(folio, '')
//                 )
//               )
//             ) as count,
//             count(distinct hm_hdate) h_count
//           from dbo.batchclosed_11_24_s d
//             left join dbo.holiday_master_prod b on cast(hm_hdate as date) between cast(navdt as date)
//             and cast(batchclosedt as date)
//             and hm_hdate >= '2023-04-01'
//             and cast(hm_hdate as date) <= current_date
//           where substring(trdt, 1, 10) = '${transaction_date}'
//           group by 1,
//             2,
//             3
//         ) a
//       group by 1,
//         2,
//         3`; //trsanctionclass bifurcation

//         const query6 = `select distinct a.*,concat(coalesce(b.app_fname,''),coalesce(b.app_mname,''),coalesce(b.app_lname,'')) as InvName
//       from
//       (Select distinct a.asset_class,a.fund,a.scheme,a.plan,a.branch,a.trno,a.folio,replace(a.ihno,'.0',''),
//       sum(cast(a.cramt as decimal)) as amount
//       from dbo.non_batchclosed_12_23_s a
//       where substring(a.trdt,1,10) = '${transaction_date}'
//       group by 1,2,3,4,5,6,7,8
//       order by 9 desc
//       limit 50) a
//         left join dbo.appl3_partition b on a.fund=b.app_fund and a.folio=b.app_acno`; //top5 pending

//         const query7 = `select asset_class,
//       credit_tat,
//       endorse_tat,
//       qc_tat,
//       count(
//           distinct(
//               concat(
//                   coalesce(scheme, ''),
//                   coalesce(plan, ''),
//                   coalesce(branch, ''),
//                   coalesce(trno, ''),
//                   coalesce(folio, '')
//               )
//           )
//       ) as count,
//       sum(cast(cramt as decimal)) as amount
//     from (
//           select asset_class,
//               trdt,
//               cleardt,
//               enddt,
//               qcenddt,
//               fund,
//               scheme,
//               plan,
//               trno,
//               branch,
//               folio,
//               cramt,
//               (
//                   case
//                       when substring(cleardt, 1, 10) = '${transaction_date}' then (
//                           case
//                               when(
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) <= 0
//                               ) then 'Same day'
//                               when(
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) <= 2
//                               ) then 'Within T+2'
//                               when (
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) > 2
//                               ) then 'Beyond T+2'
//                           end
//                       )
//                   when (substring(trdt, 1, 10) <= '${transaction_date}' and (cleardt is null or cleardt = '')) then 'pending'
//                   end
//               ) as credit_tat,
//               (
//                   case
//                       when substring(enddt, 1, 10) = '${transaction_date}' then (
//                           case
//                               when(
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) = 0
//                               ) then 'Same day'
//                               when(
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) <= 2
//                               ) then 'Within T+2'
//                               when (
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) > 2
//                               ) then 'Beyond T+2'
//                           end
//                       )
//                   when (substring(trdt, 1, 10) <= '${transaction_date}' and (enddt is null or enddt = '')) then 'pending'
//                   end
//               ) as endorse_tat,
//               (
//                   case
//                       when substring(qcenddt, 1, 10) = '${transaction_date}' then (
//                           case
//                               when(
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) = 0
//                               ) then 'Same day'
//                               when(
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) <= 2
//                               ) then 'Within T+2'
//                               when (
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) > 2
//                               ) then 'Beyond T+2'
//                           end
//                       )
//                   when (substring(trdt, 1, 10) <= '${transaction_date}' and (qcenddt is null or qcenddt = '')) then 'pending'
//                   end
//               ) as qc_tat
//           from dbo.non_batchclosed_12_23_s
//           where (
//                   substring(cleardt, 1, 10) = '${transaction_date}'
//                   or substring(enddt, 1, 10) = '${transaction_date}'
//                   or substring(qcenddt, 1, 10) = '${transaction_date}'
//               )
//           union
//           select asset_class,
//               trdt,
//               cleardt,
//               enddt,
//               qcenddt,
//               fund,
//               scheme,
//               plan,
//               trno,
//               branch,
//               folio,
//               cramt,
//               (
//                   case
//                       when substring(cleardt, 1, 10) = '${transaction_date}' then (
//                           case
//                               when(
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) = 0
//                               ) then 'Same day'
//                               when(
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) <= 2
//                               ) then 'Within T+2'
//                               when (
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) > 2
//                               ) then 'Beyond T+2'
//                           end
//                       )
//                   when (substring(trdt, 1, 10) <= '${transaction_date}' and (cleardt is null or cleardt = '')) then 'pending'
//                   end
//               ) as credit_tat,
//               (
//                   case
//                       when substring(enddt, 1, 10) = '${transaction_date}' then (
//                           case
//                               when(
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) = 0
//                               ) then 'Same day'
//                               when(
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) <= 2
//                               ) then 'Within T+2'
//                               when (
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) > 2
//                               ) then 'Beyond T+2'
//                           end
//                       )
//                   when (substring(trdt, 1, 10) <= '${transaction_date}' and (enddt is null or enddt = '')) then 'pending'
//                   end
//               ) as endorse_tat,
//               (
//                   case
//                       when substring(qcenddt, 1, 10) = '${transaction_date}' then (
//                           case
//                               when(
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) = 0
//                               ) then 'Same day'
//                               when(
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) <= 2
//                               ) then 'Within T+2'
//                               when (
//                                   DATE_PART(
//                                       'day',
//                                       nullif('${transaction_date}', '')::timestamp - nullif(trdt, '')::timestamp
//                                   ) > 2
//                               ) then 'Beyond T+2'
//                           end
//                       )
//                   when (substring(trdt, 1, 10) <= '${transaction_date}' and (qcenddt is null or qcenddt = '')) then 'pending'
//                   end
//               ) as qc_tat
//           from dbo.batchclosed_11_24_s
//           where (
//                   substring(cleardt, 1, 10) = '${transaction_date}'
//                   or substring(enddt, 1, 10) = '${transaction_date}'
//                   or substring(qcenddt, 1, 10) ='${transaction_date}'
//               )
//       ) a
//     group by 1,
//       2,
//       3,
//         4`; //trsanction stage bifurcation

//         const query8 = `select asset_class,trdt,(diff-h_count) date_part,txn_count
//         from(
//         select asset_class,substring(trdt,1,10) as trdt,(DATE_PART('day',nullif(cast(current_date as text), '')::timestamp - nullif(trdt, '')::timestamp) ) as diff,count(distinct hm_hdate) h_count,
//         count(distinct(concat(coalesce(scheme,''),coalesce(plan,''),coalesce(branch,''),coalesce(trno,''),coalesce(folio,'')))) as txn_count
//         from dbo.non_batchclosed_12_23_s d
//         left join dbo.holiday_master_prod b on cast(hm_hdate as date) between cast(trdt as date) and current_date
//         and  hm_hdate >= '2023-04-01' and cast(hm_hdate as date) <= current_date
//         where substring(trdt,1,10) <= '${transaction_date}' and active_flag = 'P'
//         group by 1,2,3
//           )x`;

//         const query9 = `select asset_class, cast(count(distinct(concat(coalesce(scheme,''), coalesce(plan,''),coalesce(branch,''),coalesce(trno,''),coalesce(folio,''))))/480.0 as decimal) as avg_count
//           from dbo.batchclosed_11_24_s where substring(batchclosedt,1,10) ='${transaction_date}' group by 1`;

//         const query10 = `select distinct a.*, concat( coalesce(b.app_fname, ''), coalesce(b.app_mname, ''), coalesce(b.app_lname, '') ) as InvName, concat(a.branch, '-', bm.bm_name) as branch_name
//           from ( Select distinct a.asset_class, a.fund, a.scheme, a.plan, a.branch, a.trno, a.trtype, a.folio, replace(a.ihno, '.0', '') as ihno, sum(cast(a.cramt as decimal)) as amount
//           from dbo.non_batchclosed_12_23_s a
//           where substring(a.trdt, 1, 10) = '${transaction_date}' and a.fund = '102' and a.asset_class = 'NON LIQUID' and a.active_flag = 'P'
//           group by 1, 2, 3, 4, 5, 6, 7, 8,9
//           order by 10 desc limit 50 ) a
//           left join dbo.appl3_partition b on a.fund = b.app_fund and a.folio = b.app_acno and b.app_fund = '102'
//           left join dbo.branch_master_partition bm on a.fund = bm.bm_fund and a.branch = bm.bm_branch and bm.bm_fund = '102' order by 10 desc`;

//         const query11 = `select distinct a.*, concat( coalesce(b.app_fname, ''), coalesce(b.app_mname, ''), coalesce(b.app_lname, '') ) as InvName, concat(a.branch, '-', bm.bm_name) as branch_name
//           from ( Select distinct a.asset_class, a.fund, a.scheme, a.plan, a.branch, a.trno, a.trtype, a.folio, replace(a.ihno, '.0', '') as ihno, sum(cast(a.cramt as decimal)) as amount
//           from dbo.non_batchclosed_12_23_s a
//           where substring(a.trdt, 1, 10) = '${transaction_date}' and a.fund = '102' and a.asset_class = 'LIQUID' and a.active_flag = 'P'
//           group by 1, 2, 3, 4, 5, 6, 7, 8,9
//           order by 10 desc limit 50 ) a
//           left join dbo.appl3_partition b on a.fund = b.app_fund and a.folio = b.app_acno and b.app_fund = '102'
//           left join dbo.branch_master_partition bm on a.fund = bm.bm_fund and a.branch = bm.bm_branch and bm.bm_fund = '102' order by 10 desc`;

//         const query12 = `select distinct a.*, concat( coalesce(b.app_fname, ''), coalesce(b.app_mname, ''), coalesce(b.app_lname, '') ) as InvName, concat(a.branch, '-', bm.bm_name) as branch_name
//           from ( Select distinct a.asset_class, a.fund, a.scheme, a.plan, a.branch, a.trno, a.trtype, a.folio, replace(a.ihno, '.0', '') as ihno, sum(cast(a.cramt as decimal)) as amount
//           from dbo.non_batchclosed_12_23_s a
//           where substring(a.trdt, 1, 10) = '${transaction_date}' and a.fund = '102' and a.active_flag = 'P'
//           group by 1, 2, 3, 4, 5, 6, 7, 8,9
//           order by 10 desc limit 50 ) a
//           left join dbo.appl3_partition b on a.fund = b.app_fund and a.folio = b.app_acno and b.app_fund = '102'
//           left join dbo.branch_master_partition bm on a.fund = bm.bm_fund and a.branch = bm.bm_branch and bm.bm_fund = '102' order by 10 desc
//            `;

//         const query13 = `select distinct a.*,concat(coalesce(b.app_fname,''),coalesce(b.app_mname,''),coalesce(b.app_lname,'')) as InvName,
//           concat(a.branch, '-', bm.bm_name) as branch_name
//           from
//           (Select distinct a.asset_class,a.fund,a.scheme,a.plan,a.branch,a.trno, a.trtype,a.folio,replace(a.ihno,'.0','') as ihno,
//           sum(cast(a.cramt as decimal)) as amount
//           from dbo.non_batchclosed_12_23_s a
//           where substring(a.trdt,1,10) <= '${transaction_date}' and a.fund='102' and a.asset_class = 'NON LIQUID'
//           group by 1,2,3,4,5,6,7,8,9
//           order by 10 desc
//           limit 50) a
//           left join dbo.appl3_partition b on a.fund=b.app_fund and a.folio=b.app_acno and b.app_fund='102'
//           left join dbo.branch_master_partition bm on a.fund=bm.bm_fund and a.branch = bm.bm_branch and bm.bm_fund='102' order by 10 desc
//            `;

//         const query14 = `Select source,asset_class,sum(amount) as amount,sum(count_of_txn) as count_of_txn from
//           ((select distinct
//           CASE
//               WHEN (
//                   branch in (
//                       'WB99',
//                       'MB88',
//                       'MA88',
//                       'A888',
//                       'I888',
//                       'W888',
//                       'DT99'
//                   )
//               ) THEN 'AMC Digital Assets'
//               WHEN (branch = 'MU99') THEN 'MFU Online'
//               WHEN (branch in ('BS77', 'NS77', 'IX77', 'BS88','NS88','IX88')) THEN 'Exchange'
//               WHEN (branch in ('KR99', 'KW99', 'DA99')) THEN 'Kfintech Digital Assets'
//               WHEN (
//                   (branch LIKE '%99')
//                   AND (
//                       NOT (
//                           branch IN ('MC99', 'WB99', 'MU99', 'KR99', 'DA99', 'DT99')
//                       )
//                   )
//               ) THEN 'Channel Partner'
//               WHEN (branch = 'MC99') THEN 'MFCentral' ELSE 'Physical Branches'
//           END as source,
//      asset_class,
//           count(distinct(concat(coalesce(scheme, ''),coalesce(plan, ''),coalesce(branch, ''),coalesce(trno, ''),coalesce(folio, '')))) as count_of_txn,
//           sum(cast(cramt as decimal)) as amount
//           from dbo.non_batchclosed_12_23_s
//           where substring(trdt, 1, 10) = '${transaction_date}'
//           group by 1,2
//           )
//           union
//           (select distinct
//           CASE
//               WHEN (
//                   branch in (
//                       'WB99',
//                       'MB88',
//                       'MA88',
//                       'A888',
//                       'I888',
//                       'W888',
//                       'DT99'
//                   )
//               ) THEN 'AMC Digital Assets'
//               WHEN (branch = 'MU99') THEN 'MFU Online'
//               WHEN (branch in ('BS77', 'NS77', 'IX77', 'BS88','NS88','IX88')) THEN 'Exchange'
//               WHEN (branch in ('KR99', 'KW99', 'DA99')) THEN 'Kfintech Digital Assets'
//               WHEN (
//                   (branch LIKE '%99')
//                   AND (
//                       NOT (
//                           branch IN ('MC99', 'WB99', 'MU99', 'KR99', 'DA99', 'DT99')
//                       )
//                   )
//               ) THEN 'Channel Partner'
//               WHEN (branch = 'MC99') THEN 'MFCentral' ELSE 'Physical Branches'
//           END as source,
//      asset_class,
//           count(distinct(concat(coalesce(scheme, ''),coalesce(plan, ''),coalesce(branch, ''),coalesce(trno, ''),coalesce(folio, '')))) as count_of_txn,
//           sum(cast(cramt as decimal)) as amount
//           from dbo.batchclosed_11_24_s
//           where substring(trdt, 1, 10) = '${transaction_date}'
//           group by 1,2
//           )) p
//           group by 1,2
//           order by 3 desc
//           limit 15`;

//         const query15 = `Select source,asset_class,sum(amount) as amount,sum(count_of_txn) as count_of_txn from
//           ((select distinct
//           CASE
//               WHEN (
//                   branch in (
//                       'WB99',
//                       'MB88',
//                       'MA88',
//                       'A888',
//                       'I888',
//                       'W888',
//                       'DT99'
//                   )
//               ) THEN 'AMC Digital Assets'
//               WHEN (branch = 'MU99') THEN 'MFU Online'
//               WHEN (branch in ('BS77', 'NS77', 'IX77', 'BS88','NS88','IX88')) THEN 'Exchange'
//               WHEN (branch in ('KR99', 'KW99', 'DA99')) THEN 'Kfintech Digital Assets'
//               WHEN (
//                   (branch LIKE '%99')
//                   AND (
//                       NOT (
//                           branch IN ('MC99', 'WB99', 'MU99', 'KR99', 'DA99', 'DT99')
//                       )
//                   )
//               ) THEN 'Channel Partner'
//               WHEN (branch = 'MC99') THEN 'MFCentral' ELSE 'Physical Branches'
//           END as source,
//      asset_class,
//           count(distinct(concat(coalesce(scheme, ''),coalesce(plan, ''),coalesce(branch, ''),coalesce(trno, ''),coalesce(folio, '')))) as count_of_txn,
//           sum(cast(cramt as decimal)) as amount
//           from dbo.non_batchclosed_12_23_s
//           where substring(trdt, 1, 10) = '${transaction_date}'and asset_class = 'LIQUID'
//           group by 1,2
//           )
//           union
//           (select distinct
//           CASE
//               WHEN (
//                   branch in (
//                       'WB99',
//                       'MB88',
//                       'MA88',
//                       'A888',
//                       'I888',
//                       'W888',
//                       'DT99'
//                   )
//               ) THEN 'AMC Digital Assets'
//               WHEN (branch = 'MU99') THEN 'MFU Online'
//               WHEN (branch in ('BS77', 'NS77', 'IX77', 'BS88','NS88','IX88')) THEN 'Exchange'
//               WHEN (branch in ('KR99', 'KW99', 'DA99')) THEN 'Kfintech Digital Assets'
//               WHEN (
//                   (branch LIKE '%99')
//                   AND (
//                       NOT (
//                           branch IN ('MC99', 'WB99', 'MU99', 'KR99', 'DA99', 'DT99')
//                       )
//                   )
//               ) THEN 'Channel Partner'
//               WHEN (branch = 'MC99') THEN 'MFCentral' ELSE 'Physical Branches'
//           END as source,
//      asset_class,
//           count(distinct(concat(coalesce(scheme, ''),coalesce(plan, ''),coalesce(branch, ''),coalesce(trno, ''),coalesce(folio, '')))) as count_of_txn,
//           sum(cast(cramt as decimal)) as amount
//           from dbo.batchclosed_11_24_s
//           where substring(trdt, 1, 10) = '${transaction_date}' and asset_class = 'LIQUID'
//           group by 1,2
//           )) p
//           group by 1,2
//           order by 3 desc
//           limit 15`;

//         const query16 = `Select source,asset_class,sum(amount) as amount,sum(count_of_txn) as count_of_txn from
//           ((select distinct
//           CASE
//               WHEN (
//                   branch in (
//                       'WB99',
//                       'MB88',
//                       'MA88',
//                       'A888',
//                       'I888',
//                       'W888',
//                       'DT99'
//                   )
//               ) THEN 'AMC Digital Assets'
//               WHEN (branch = 'MU99') THEN 'MFU Online'
//               WHEN (branch in ('BS77', 'NS77', 'IX77', 'BS88','NS88','IX88')) THEN 'Exchange'
//               WHEN (branch in ('KR99', 'KW99', 'DA99')) THEN 'Kfintech Digital Assets'
//               WHEN (
//                   (branch LIKE '%99')
//                   AND (
//                       NOT (
//                           branch IN ('MC99', 'WB99', 'MU99', 'KR99', 'DA99', 'DT99')
//                       )
//                   )
//               ) THEN 'Channel Partner'
//               WHEN (branch = 'MC99') THEN 'MFCentral' ELSE 'Physical Branches'
//           END as source,
//      asset_class,
//           count(distinct(concat(coalesce(scheme, ''),coalesce(plan, ''),coalesce(branch, ''),coalesce(trno, ''),coalesce(folio, '')))) as count_of_txn,
//           sum(cast(cramt as decimal)) as amount
//           from dbo.non_batchclosed_12_23_s
//           where substring(trdt, 1, 10) = '${transaction_date}'and asset_class = 'NON LIQUID'
//           group by 1,2
//           )
//           union
//           (select distinct
//           CASE
//               WHEN (
//                   branch in (
//                       'WB99',
//                       'MB88',
//                       'MA88',
//                       'A888',
//                       'I888',
//                       'W888',
//                       'DT99'
//                   )
//               ) THEN 'AMC Digital Assets'
//               WHEN (branch = 'MU99') THEN 'MFU Online'
//               WHEN (branch in ('BS77', 'NS77', 'IX77', 'BS88','NS88','IX88')) THEN 'Exchange'
//               WHEN (branch in ('KR99', 'KW99', 'DA99')) THEN 'Kfintech Digital Assets'
//               WHEN (
//                   (branch LIKE '%99')
//                   AND (
//                       NOT (
//                           branch IN ('MC99', 'WB99', 'MU99', 'KR99', 'DA99', 'DT99')
//                       )
//                   )
//               ) THEN 'Channel Partner'
//               WHEN (branch = 'MC99') THEN 'MFCentral' ELSE 'Physical Branches'
//           END as source,
//      asset_class,
//           count(distinct(concat(coalesce(scheme, ''),coalesce(plan, ''),coalesce(branch, ''),coalesce(trno, ''),coalesce(folio, '')))) as count_of_txn,
//           sum(cast(cramt as decimal)) as amount
//           from dbo.batchclosed_11_24_s
//           where substring(trdt, 1, 10) = '${transaction_date}' and asset_class = 'NON LIQUID'
//           group by 1,2
//           )) p
//           group by 1,2
//           order by 3 desc
//           limit 15`;

//         const query17 = `select distinct a.*, concat( coalesce(b.app_fname, ''), coalesce(b.app_mname, ''), coalesce(b.app_lname, '') ) as InvName, concat(a.branch, '-', bm.bm_name) as branch_name
//           from ( Select distinct a.asset_class, a.fund, a.scheme, a.plan, a.branch, a.trno, a.trtype, a.folio, replace(a.ihno, '.0', '') as ihno, sum(cast(a.cramt as decimal)) as amount
//           from dbo.non_batchclosed_12_23_s a
//           where substring(a.trdt, 1, 10) = '${transaction_date}' and a.fund = '102' and a.asset_class = 'LIQUID' and a.active_flag = 'P'
//           group by 1, 2, 3, 4, 5, 6, 7, 8,9
//           order by 10 desc limit 50 ) a
//           left join dbo.appl3_partition b on a.fund = b.app_fund and a.folio = b.app_acno and b.app_fund = '102'
//           left join dbo.branch_master_partition bm on a.fund = bm.bm_fund and a.branch = bm.bm_branch and bm.bm_fund = '102' order by 10 desc`;

//         const query7_tr_stage_bifurcation_QualityCheck = `select distinct
//         'qc' as stage,
//         asset_class,
//           (
//                 case
//                   when (asset_class = 'LIQUID')
//                   and (qcstatus = 'qc done') then (
//                     case
//                       when ((diff - h_count <= 0) or (qcenddt is null)) then 'within TAT'
//                       when diff - h_count >= 1 then 'beyond TAT'
//                     end
//                   )
//                   when (asset_class = 'NON LIQUID')
//                   and (qcstatus = 'qc done') then (
//                     case
//                       when ((diff - h_count <= 1) or (qcenddt is null)) then 'within TAT'
//                       when diff - h_count >= 2 then 'beyond TAT'
//                     end
//                   ) else 'pending'
//                 end
//               ) tat,
//           sum(cast(cramt as decimal)) as amount,
//           sum(count)
//         from (
//             select distinct asset_class,
//               DATE_PART(
//                 'day',
//                 nullif(qcenddt , '')::timestamp - nullif(trdt, '')::timestamp
//               ) diff,
//               qcstatus,
//               trdt,
//               qcenddt,
//               cramt,
//               count(
//                 distinct(
//                   concat(
//                     coalesce(scheme, ''),
//                     coalesce(plan, ''),
//                     coalesce(branch, ''),
//                     coalesce(trno, ''),
//                     coalesce(folio, '')
//                   )
//                 )
//               ) as count,
//               count(distinct hm_hdate) h_count
//             from dbo.batchclosed_11_24_s d
//               left join dbo.holiday_master_partition b on d.fund=b.hm_fund and d.scheme=b.hm_scheme and d.plan=b.hm_plan and cast(hm_hdate as date) between cast(nullif(trdt,'') as date)
//               and cast(nullif(qcenddt,'') as date)
//               and hm_hdate >= '2023-04-01'
//             and b.hm_fund='102'
//           and d.fund='102'
//               and cast(hm_hdate as date) <= current_date
//             where substring(trdt, 1, 10) = '${transaction_date}'
//             group by 1,
//               2,
//               3,4,5,6
//           ) a
//         group by 1,
//           2,3`;

//         const query7_tr_stage_bifurcation_CreditMarked = `select distinct
//         'credit' as stage,
//         asset_class,
//           (
//                 case
//                   when (asset_class = 'LIQUID')
//                   and (
//                     cleardt is not null
//                     and cleardt != ''
//                     and cleardt not like '1900%'
//                   ) then (
//                     case
//                       when diff - h_count <= 0 then 'within TAT'
//                       when diff - h_count >= 1 then 'beyond TAT'
//                       else 'credit update pending'
//                     end
//                   )
//                   when (asset_class = 'NON LIQUID')
//                   and (
//                     cleardt is not null
//                     and cleardt != ''
//                     and cleardt not like '1900%'
//                   ) then (
//                     case
//                       when diff - h_count <= 1 then 'within TAT'
//                       when diff - h_count >= 2 then 'beyond TAT'
//                       else 'credit update pending'
//                     end
//                   ) else 'pending'
//                 end
//               ) tat,
//           sum(cast(cramt as decimal)) as amount,
//           sum(count)
//         from (
//             select distinct asset_class,
//               DATE_PART(
//                 'day',
//                 nullif(cast(coalesce(cleardtupdt,enddt) as text), '')::timestamp - nullif(cleardt, '')::timestamp
//               ) diff,
//               cleardt,
//               cleardtupdt,
//               cramt,
//               count(
//                 distinct(
//                   concat(
//                     coalesce(scheme, ''),
//                     coalesce(plan, ''),
//                     coalesce(branch, ''),
//                     coalesce(trno, ''),
//                     coalesce(folio, '')
//                   )
//                 )
//               ) as count,
//               count(distinct hm_hdate) h_count
//             from dbo.batchclosed_11_24_s d
//               left join dbo.holiday_master_partition b on d.fund=b.hm_fund and d.scheme=b.hm_scheme and d.plan=b.hm_plan and cast(hm_hdate as date) between cast(nullif(cleardt,'') as date)
//               and cast(nullif(cleardtupdt,'') as date)
//               and hm_hdate >= '2023-04-01'
//             and b.hm_fund='102'
//           and d.fund='102'
//               and cast(hm_hdate as date) <= current_date
//             where substring(trdt, 1, 10) = '${transaction_date}'
//             group by 1,
//               2,
//               3,4,5
//           ) a
//         group by 1,
//           2,3`;

//         const query7_tr_stage_bifurcation_Endorsement = `select distinct
//         'endorsement' as stage,
//         asset_class,
//           (
//                 case
//                   when (asset_class = 'LIQUID')
//                   and (
//                     enddt is not null
//                     and enddt != ''
//                     and enddt not like '1900%'
//                     and active_flag = 'Y'
//                   ) then (
//                     case
//                       when diff - h_count <= 0 then 'within TAT'
//                       when diff - h_count >= 1 then 'beyond TAT'
//                     end
//                   )
//                   when (asset_class = 'NON LIQUID')
//                   and (
//                     enddt is not null
//                     and enddt != ''
//                     and enddt not like '1900%'
//                     and active_flag = 'Y'
//                   ) then (
//                     case
//                       when diff - h_count <= 1 then 'within TAT'
//                       when diff - h_count >= 2 then 'beyond TAT'
//                     end
//                   ) else 'pending'
//                 end
//               ) tat,
//           sum(cast(cramt as decimal)) as amount,
//           sum(count)
//         from (
//             select distinct asset_class,
//               DATE_PART(
//                 'day',
//                 nullif(cast(enddt as text), '')::timestamp - nullif(navdt, '')::timestamp
//               ) diff,
//               enddt,
//               navdt,
//               cramt,
//               active_flag,
//               count(
//                 distinct(
//                   concat(
//                     coalesce(scheme, ''),
//                     coalesce(plan, ''),
//                     coalesce(branch, ''),
//                     coalesce(trno, ''),
//                     coalesce(folio, '')
//                   )
//                 )
//               ) as count,
//               count(distinct hm_hdate) h_count
//             from dbo.batchclosed_11_24_s d
//               left join dbo.holiday_master_partition b on d.fund=b.hm_fund and d.scheme=b.hm_scheme and d.plan=b.hm_plan and cast(hm_hdate as date) between cast(nullif(navdt,'') as date)
//               and cast(nullif(enddt,'') as date)
//               and hm_hdate >= '2023-04-01'
//           and b.hm_fund='102'
//           and d.fund='102'
//               and cast(hm_hdate as date) <= current_date
//             where substring(trdt, 1, 10) = '${transaction_date}'
//             group by 1,
//               2,
//               3,4,5,6
//           ) a
//         group by 1,
//           2,3`;

//         const query18 = `select distinct a.*, concat( coalesce(b.app_fname, ''), coalesce(b.app_mname, ''), coalesce(b.app_lname, '') ) as InvName
//       ,
//       concat(a.branch, '-', bm.bm_name) as branch_name
//           from ( Select distinct a.asset_class, a.fund, a.scheme, a.plan, a.branch, a.trno, a.trtype, a.folio, replace(a.ihno, '.0', '') as ihno, sum(cast(a.cramt as decimal)) as amount
//           from dbo.non_batchclosed_12_23_s a
//           where substring(a.trdt, 1, 10) <= '${transaction_date}' and a.fund = '102' and a.active_flag = 'P'
//           group by 1, 2, 3, 4, 5,6, 7, 8,9
//        order by 10 desc
//          limit 50 ) a
//           left join dbo.appl3_partition b on a.fund = b.app_fund and a.folio = b.app_acno and b.app_fund = '102'
//           left join dbo.branch_master_partition bm on a.fund = bm.bm_fund and a.branch = bm.bm_branch and bm.bm_fund = '102'
//    order by 10 desc`;

//         const query19 = `select distinct asset_class,
//           substring(trdt, 1, 10) as trdt,
//           (
//             case
//               when enddt is not null
//               and enddt != ''
//               and active_flag = 'Y'
//               and enddt not like '1900%' then 'Yes' else 'No'
//             end
//           ) as endorsed,
//           (
//             case
//               when cleardt is not null
//               and cleardt != ''
//             and cleardt not like '1900%' then 'Yes' else 'No'
//             end
//           ) as credit_marked,
//           (
//             case
//               when cleardtupdt is not null
//               and cleardtupdt != ''
//             and cleardt not like '1900%' then 'Yes' else 'No'
//             end
//           ) as credit_updated,
//           (
//             case
//               when qcenddt is not null
//               and qcenddt != '' then 'Yes' else 'No'
//             end
//           ) as qcmarked,
//           count(
//             distinct(
//               concat(
//                 coalesce(scheme, ''),
//                 coalesce(plan, ''),
//                 coalesce(branch, ''),
//                 coalesce(trno, ''),
//                 coalesce(folio, '')
//               )
//             )
//           ) as count,
//           sum(cast(cramt as decimal)) as amount
//         from (
//             (
//               select distinct asset_class,
//                 fund,
//                 scheme,
//                 plan,
//                 branch,
//                 trno,
//                 folio,
//                 trdt,
//                 enddt,
//                 active_flag,
//                 cleardt,
//                 cleardtupdt,
//                 qcenddt,
//                 cramt
//               from dbo.non_batchclosed_12_23_s
//               where substring(trdt, 1, 10) = '${transaction_date}'
//             )
//             union
//             (
//               select distinct asset_class,
//                 fund,
//                 scheme,
//                 plan,
//                 branch,
//                 trno,
//                 folio,
//                 trdt,
//                 enddt,
//                 active_flag,
//                 cleardt,
//                 cleardtupdt,
//                 qcenddt,
//                 cramt
//               from dbo.batchclosed_11_24_s
//               where substring(trdt, 1, 10) = '${transaction_date}'
//             )
//           ) a
//         group by 1,
//           2,
//           3,
//           4,
//           5,
//           6`;

//         const query20 = `select distinct

//           concat('IT IS ',upper(hm_reason)) as msg,

//           hm_fund,hm_hdate,upper(hm_reason) from dbo.holiday_master_partition

//           where

//           hm_hdate >= '2023-04-01' and hm_hdate <= '2026-04-01'

//           and cast(substring(hm_hdate,1,10) as date) = '${transaction_date}'`;

//         const fetch_query = async (array) => {
//           const all_promises = array.map((ele, ind) => {
//             let allresponse = [];
//             return new Promise(async (resolve, reject) => {
//               let response_obj = {};
//               const res = await pool.query(ele.query);
//               response_obj = {
//                 name: ele.name,
//                 result: res.rows,
//               };

//               allresponse.push(response_obj);

//               resolve(response_obj);
//             });
//           });
//           return all_promises;
//         };

//         const calculate_result = (query_results) =>
//           new Promise(async (resolve, reject) => {
//             query_results.forEach((item, ind) => {
//               const rows = item.result;

//               if (item.name == "tr history") {
//                 let object = {
//                   combined: {
//                     total_transaction_count: 0,
//                     total_transaction_amount: 0,
//                     pending_transaction_count: 0,
//                     previous_total_transaction_count: 0,
//                     within_t2_transaction: 0,
//                     beyond_t2_transaction: 0,
//                     total_completed_transaction_count: 0,
//                     transaction_status: "",
//                     percentage_difference: 0,
//                   },
//                   liquid: {
//                     total_transaction_count: 0,
//                     total_transaction_amount: 0,
//                     pending_transaction_count: 0,
//                     previous_total_transaction_count: 0,
//                     within_t2_transaction: 0,
//                     beyond_t2_transaction: 0,
//                     total_completed_transaction_count: 0,
//                     transaction_status: "",
//                     percentage_difference: 0,
//                   },
//                   nonliquid: {
//                     total_transaction_count: 0,
//                     total_transaction_amount: 0,
//                     pending_transaction_count: 0,
//                     previous_total_transaction_count: 0,
//                     within_t2_transaction: 0,
//                     beyond_t2_transaction: 0,
//                     total_completed_transaction_count: 0,
//                     transaction_status: "",
//                     percentage_difference: 0,
//                   },
//                 };
//                 let previous_date

//                 for(let i = 0; i < rows.length; i++) {
//                   if(rows[i].trdt != transaction_date) {
//                     previous_date = rows[i].trdt
//                     break;
//                   }
//                 }

//                 rows.forEach((ele, ind) => {
//                   if (ele.asset_class == "LIQUID") {
//                     if (ele.trdt == transaction_date) {
//                       object.liquid.total_transaction_count += Number(ele.count);
//                       object.liquid.total_transaction_amount += Number(
//                         ele.amount
//                       );
//                       if (ele.status == "pending_txn_count")
//                         object.liquid.pending_transaction_count += Number(
//                           ele.count
//                         );
//                     } else if (ele.trdt == previous_date) {
//                       object.liquid.previous_total_transaction_count += Number(
//                         ele.count
//                       );
//                       if (ele.status == "pending_txn_count") {
//                         object.liquid.previous_pending_total_transaction_count +=
//                           Number(ele.count);
//                       }
//                     }
//                   } else {
//                     if (ele.trdt == transaction_date) {
//                       object.nonliquid.total_transaction_count += Number(
//                         ele.count
//                       );
//                       object.nonliquid.total_transaction_amount += Number(
//                         ele.amount
//                       );
//                       if (ele.status == "pending_txn_count")
//                         object.nonliquid.pending_transaction_count += Number(
//                           ele.count
//                         );
//                     } else if (ele.trdt == previous_date) {
//                       object.nonliquid.previous_total_transaction_count += Number(
//                         ele.count
//                       );
//                       if (ele.status == "pending_txn_count") {
//                         object.nonliquid.previous_pending_total_transaction_count +=
//                           Number(ele.count);
//                       }
//                     }
//                   }
//                 });

//                 object.combined.previous_total_transaction_count =
//                   object.liquid.previous_total_transaction_count +
//                   object.nonliquid.previous_total_transaction_count;
//                 object.combined.pending_transaction_count =
//                   object.liquid.pending_transaction_count +
//                   object.nonliquid.pending_transaction_count;
//                 object.combined.total_transaction_amount =
//                   object.liquid.total_transaction_amount +
//                   object.nonliquid.total_transaction_amount;
//                 object.combined.total_transaction_count =
//                   object.liquid.total_transaction_count +
//                   object.nonliquid.total_transaction_count;
//                 object.liquid.total_completed_transaction_count =
//                   object.liquid.total_transaction_count -
//                   object.liquid.pending_transaction_count;
//                 object.nonliquid.total_completed_transaction_count =
//                   object.nonliquid.total_transaction_count -
//                   object.nonliquid.pending_transaction_count;
//                 object.combined.total_completed_transaction_count =
//                   object.liquid.total_completed_transaction_count +
//                   object.nonliquid.total_completed_transaction_count -
//                   (object.liquid.pending_transaction_count +
//                     object.nonliquid.pending_transaction_count);
//                 if (object.liquid.pending_transaction_count > 0)
//                   object.liquid.transaction_status = "Pending";
//                 else object.liquid.transaction_status = "Completed";
//                 if (object.nonliquid.pending_transaction_count > 0)
//                   object.nonliquid.transaction_status = "Pending";
//                 else object.nonliquid.transaction_status = "Completed";
//                 if (object.combined.pending_transaction_count > 0)
//                   object.combined.transaction_status = "Pending";
//                 else object.combined.transaction_status = "Completed";

//                 let liquid_percentage =
//                   ((object.liquid.total_transaction_count -
//                     object.liquid.previous_total_transaction_count) /
//                     object.liquid.previous_total_transaction_count) *
//                   100;
//                 let nonliquid_percentage =
//                   ((object.nonliquid.total_transaction_count -
//                     object.nonliquid.previous_total_transaction_count) /
//                     object.nonliquid.previous_total_transaction_count) *
//                   100;
//                 let combined_percentage =
//                   ((object.combined.total_transaction_count -
//                     object.combined.previous_total_transaction_count) /
//                     object.combined.previous_total_transaction_count) *
//                   100;
//                 object.liquid.percentage_difference = liquid_percentage;
//                 object.nonliquid.percentage_difference = nonliquid_percentage;
//                 object.combined.percentage_difference = combined_percentage;

//                 dataObject.trhistory = object;
//               } else if (item.name == "tr bifurcation") {
//                 let rows = item.result;
//                 let transactionClassBifurcation = {
//                   liquid: {
//                     ADD: {
//                       withinTAT: 0,
//                       beyondTAT: 0,
//                     },
//                     NEW: {
//                       withinTAT: 0,
//                       beyondTAT: 0,
//                     },
//                     SIN: {
//                       withinTAT: 0,
//                       beyondTAT: 0,
//                     },

//                     IPO: {
//                       withinTAT: 0,
//                       beyondTAT: 0,
//                     },
//                   },
//                   nonLiquid: {
//                     ADD: {
//                       withinTAT: 0,
//                       beyondTAT: 0,
//                     },
//                     NEW: {
//                       withinTAT: 0,
//                       beyondTAT: 0,
//                     },
//                     SIN: {
//                       withinTAT: 0,
//                       beyondTAT: 0,
//                     },
//                     IPO: {
//                       withinTAT: 0,
//                       beyondTAT: 0,
//                     },
//                   },
//                 };

//                 rows.forEach((items) => {
//                   if (items.asset_class == "LIQUID") {
//                     if (items.trtype == "NEW") {
//                       transactionClassBifurcation.liquid.NEW.total += Number(
//                         items.sum
//                       );
//                       if (items.case == "with in TAT") {
//                         transactionClassBifurcation.liquid.NEW.withinTAT = Number(
//                           items.sum
//                         );
//                       }
//                       if (items.case == "beyond TAT") {
//                         transactionClassBifurcation.liquid.NEW.beyondTAT = Number(
//                           items.sum
//                         );
//                       }
//                     } else if (items.trtype == "ADD") {
//                       transactionClassBifurcation.liquid.ADD.total += Number(
//                         items.sum
//                       );
//                       if (items.case == "with in TAT") {
//                         transactionClassBifurcation.liquid.ADD.withinTAT = Number(
//                           items.sum
//                         );
//                       }
//                       if (items.case == "beyond TAT") {
//                         transactionClassBifurcation.liquid.ADD.beyondTAT = Number(
//                           items.sum
//                         );
//                       }
//                     } else if (items.trtype == "SIN") {
//                       transactionClassBifurcation.liquid.SIN.total += Number(
//                         items.sum
//                       );
//                       if (items.case == "with in TAT") {
//                         transactionClassBifurcation.liquid.SIN.withinTAT = Number(
//                           items.sum
//                         );
//                       }
//                       if (items.case == "beyond TAT") {
//                         transactionClassBifurcation.liquid.SIN.beyondTAT = Number(
//                           items.sum
//                         );
//                       }
//                     } else if (items.trtype == "IPO") {
//                       transactionClassBifurcation.liquid.IPO.total += Number(
//                         items.sum
//                       );
//                       if (items.case == "with in TAT") {
//                         transactionClassBifurcation.liquid.IPO.withinTAT = Number(
//                           items.sum
//                         );
//                       }
//                       if (items.case == "beyond TAT") {
//                         transactionClassBifurcation.liquid.IPO.beyondTAT = Number(
//                           items.sum
//                         );
//                       }
//                     }
//                   } else if (items.asset_class == "NON LIQUID") {
//                     if (items.trtype == "NEW") {
//                       transactionClassBifurcation.liquid.NEW.total += Number(
//                         items.sum
//                       );
//                       if (items.case == "with in TAT") {
//                         transactionClassBifurcation.nonLiquid.NEW.withinTAT =
//                           Number(items.sum);
//                       }
//                       if (items.case == "beyond TAT") {
//                         transactionClassBifurcation.nonLiquid.NEW.beyondTAT =
//                           Number(items.sum);
//                       }
//                     } else if (items.trtype == "ADD") {
//                       transactionClassBifurcation.nonLiquid.ADD.total += Number(
//                         items.sum
//                       );

//                       if (items.case == "with in TAT") {
//                         transactionClassBifurcation.nonLiquid.ADD.withinTAT =
//                           Number(items.sum);
//                       }
//                       if (items.case == "beyond TAT") {
//                         transactionClassBifurcation.nonLiquid.ADD.beyondTAT =
//                           Number(items.sum);
//                       }
//                     } else if (items.trtype == "SIN") {
//                       transactionClassBifurcation.nonLiquid.SIN.total += Number(
//                         items.sum
//                       );
//                       if (items.case == "with in TAT") {
//                         transactionClassBifurcation.nonLiquid.SIN.withinTAT =
//                           Number(items.sum);
//                       }
//                       if (items.case == "beyond TAT") {
//                         transactionClassBifurcation.nonLiquid.SIN.beyondTAT =
//                           Number(items.sum);
//                       }
//                     } else if (items.trtype == "IPO") {
//                       transactionClassBifurcation.nonLiquid.IPO.total += Number(
//                         items.sum
//                       );

//                       if (items.case == "with in TAT") {
//                         transactionClassBifurcation.nonLiquid.IPO.withinTAT =
//                           Number(items.sum);
//                       }
//                       if (items.case == "beyond TAT") {
//                         transactionClassBifurcation.nonLiquid.IPO.beyondTAT =
//                           Number(items.sum);
//                       }
//                     }
//                   }
//                 });

//                 
//                 dataObject.transactionClassBifurcation =
//                   transactionClassBifurcation;
//               } else if (item.name == "penindg tr count") {
//                 let obj = {
//                   combined_array: [],
//                   liquid_array: [],
//                   nonliquid_array: [],
//                 };

//                 obj.nonliquid_array = item.result.filter(
//                   (ele, ind) => ele.tag == "nl_50"
//                 );
//                 obj.liquid_array = item.result.filter(
//                   (ele, ind) => ele.tag == "l_50"
//                 );

//                 dataObject.topPendingTransactionArray = obj;
//               } else if (item.name == "Pending count with TAT") {
//                 const rows = item.result;

//                 let tillDateObject = {
//                   combine: {
//                     pending_count: 0,
//                     within: 0,
//                     beyond: 0,
//                     total_pending_count: 0,
//                   },
//                   liquid: {
//                     pending_count: 0,
//                     within: 0,
//                     beyond: 0,
//                     total_pending_count: 0,
//                   },
//                   nonliquid: {
//                     pending_count: 0,
//                     within: 0,
//                     beyond: 0,
//                     total_pending_count: 0,
//                   },
//                 };

//                 let specificDateObject = {
//                   combine: {
//                     pending_count: 0,
//                     within: 0,
//                     beyond: 0,
//                     total_pending_count: 0,
//                   },
//                   liquid: {
//                     pending_count: 0,
//                     within: 0,
//                     beyond: 0,
//                     total_pending_count: 0,
//                   },
//                   nonliquid: {
//                     pending_count: 0,
//                     within: 0,
//                     beyond: 0,
//                     total_pending_count: 0,
//                   },
//                 };

//                 rows.forEach((ele, ind) => {
//                   if (ele.trdt <= transaction_date) {
//                    
//                     const assetType =
//                       ele.asset_class === "LIQUID" ? "liquid" : "nonliquid";
//                     const transactionType =
//                       Number(ele.date_part) <= 2 ? "within" : "beyond";
//                     tillDateObject[assetType][transactionType] += Number(
//                       ele.txn_count
//                     );
//                     tillDateObject[assetType]["total_pending_count"] += Number(
//                       ele.txn_count
//                     );
//                     
//                   }
//                   if (ele.trdt == transaction_date) {
//                     const assetType =
//                       ele.asset_class === "LIQUID" ? "liquid" : "nonliquid";
//                     const transactionType =
//                       Number(ele.date_part) <= 2 ? "within" : "beyond";

//                     specificDateObject[assetType][transactionType] += Number(
//                       ele.txn_count
//                     );
//                     specificDateObject[assetType]["total_pending_count"] +=
//                       Number(ele.txn_count);
//                   }
//                 });

//                 tillDateObject["combine"]["within"] =
//                   tillDateObject["liquid"]["within"] +
//                   tillDateObject["nonliquid"]["within"];
//                 specificDateObject["combine"]["within"] =
//                   specificDateObject["liquid"]["within"] +
//                   specificDateObject["nonliquid"]["within"];
//                 tillDateObject["combine"]["beyond"] =
//                   tillDateObject["liquid"]["beyond"] +
//                   tillDateObject["nonliquid"]["beyond"];
//                 specificDateObject["combine"]["beyond"] =
//                   specificDateObject["liquid"]["beyond"] +
//                   specificDateObject["nonliquid"]["beyond"];
//                 tillDateObject["combine"]["total_pending_count"] =
//                   tillDateObject["liquid"]["total_pending_count"] +
//                   tillDateObject["nonliquid"]["total_pending_count"];
//                 specificDateObject["combine"]["total_pending_count"] =
//                   specificDateObject["liquid"]["total_pending_count"] +
//                   specificDateObject["nonliquid"]["total_pending_count"];

//                 dataObject.tillDatePendingTATObject = tillDateObject;
//                 dataObject.specificDatePendingTATObject = specificDateObject;

//                 // let object ={
//                 //   combine:{
//                 //     pending_count:0,
//                 //     within:0,
//                 //     beyond:0
//                 //   },
//                 //   liquid:{
//                 //     pending_count:0,
//                 //     within:0,
//                 //     beyond:0
//                 //   },
//                 //   nonliquid:{
//                 //     pending_count:0,
//                 //     within:0,
//                 //     beyond:0
//                 //   }
//                 // }

//                 // const current_data = rows.filter((ele, ind) => ele.trdt == '${transaction_date}')
//                 // const tilldate_data = rows.filter((ele, ind) => ele.trdt < '2024-03-28')

//                 // tilldate_data.forEach((ele, ind) =>{
//                 //   if (ele.asset_class == 'LIQUID') {
//                 //     if (Number(ele.date_part) > 2) object.liquid.beyond_t2_transaction += Number(ele.count)
//                 //     else object.liquid.within_t2_transaction += Number(ele.count)
//                 //   } else {
//                 //     if (Number(ele.date_part) > 2) object.nonliquid.beyond_t2_transaction += Number(ele.count)
//                 //     else object.nonliquid.within_t2_transaction += Number(ele.count)
//                 //   }
//                 // })

//                 // rows.forEach((ele, ind) =>{

//                 // })
//               } else if (item.name == "top cities") {
//                 const rows = item.result;
//                 dataObject.topBranches = rows;
//               } else if (item.name == "Average Transaction(per minute)") {
//                 let obj = {
//                   LIQUID: "",
//                   NONLIQUID: "",
//                   Combine: "",
//                 };

//                 rows.forEach((items) => {
//                   if (items.asset_class == "LIQUID") {
//                     obj.LIQUID = parseInt(Number(items.avg_count));
//                   } else if (items.asset_class == "NON LIQUID") {
//                     obj.NONLIQUID = parseInt(Number(items.avg_count));
//                   }
//                 });

//                 obj.Combine = Number(obj.LIQUID) + Number(obj.NONLIQUID);

//                 dataObject.avgTraxCompletedPerMin = obj;
//               } else if (item.name == "tr stage bifurcation") {
//                 let obj = {
//                   liquid: {
//                     total_creditmarked_count: 0,
//                     total_creditmarked_amount: 0,
//                     total_endorsement_count: 0,
//                     total_endorsement_amount: 0,
//                     total_qc_count: 0,
//                     total_qc_amount: 0,
//                     creditmark: {
//                       within_count: 0,
//                       within_amount: 0,
//                       beyond_count: 0,
//                       beyond_amount: 0,
//                       sameday_count: 0,
//                       sameday_amount: 0,
//                       pending_count: 0,
//                       pending_amount: 0,
//                     },
//                     qualityCheck: {
//                       within_count: 0,
//                       within_amount: 0,
//                       beyond_count: 0,
//                       beyond_amount: 0,
//                       sameday_count: 0,
//                       sameday_amount: 0,
//                       pending_count: 0,
//                       pending_amount: 0,
//                     },
//                     endorsement: {
//                       within_count: 0,
//                       within_amount: 0,
//                       beyond_count: 0,
//                       beyond_amount: 0,
//                       sameday_count: 0,
//                       sameday_amount: 0,
//                       pending_count: 0,
//                       pending_amount: 0,
//                     },
//                   },
//                   nonLiquid: {
//                     total_creditmarked_count: 0,
//                     total_creditmarked_amount: 0,
//                     total_endorsement_count: 0,
//                     total_endorsement_amount: 0,
//                     total_qc_count: 0,
//                     total_qc_amount: 0,
//                     creditmark: {
//                       within_count: 0,
//                       within_amount: 0,
//                       beyond_count: 0,
//                       beyond_amount: 0,
//                       sameday_count: 0,
//                       sameday_amount: 0,
//                       pending_count: 0,
//                       pending_amount: 0,
//                     },
//                     qualityCheck: {
//                       within_count: 0,
//                       within_amount: 0,
//                       beyond_count: 0,
//                       beyond_amount: 0,
//                       sameday_count: 0,
//                       sameday_amount: 0,
//                       pending_count: 0,
//                       pending_amount: 0,
//                     },
//                     endorsement: {
//                       within_count: 0,
//                       within_amount: 0,
//                       beyond_count: 0,
//                       beyond_amount: 0,
//                       sameday_count: 0,
//                       sameday_amount: 0,
//                       pending_count: 0,
//                       pending_amount: 0,
//                     },
//                   },
//                   total_liquid_completed_percentage: 0,
//                   total_nonliquid_completed_percentage: 0,
//                 };

//                 rows.forEach((ele, ind) => {
//                   if (ele.asset_class == "NON LIQUID") {
//                     if (ele.credit_tat != null) {
//                       obj.nonLiquid.total_creditmarked_count += Number(ele.count);
//                       obj.nonLiquid.total_creditmarked_amount += Number(
//                         ele.amount
//                       );
//                       if (ele.credit_tat == "Same day") {
//                         obj.nonLiquid.creditmark.sameday_count += Number(
//                           ele.count
//                         );
//                         obj.nonLiquid.creditmark.sameday_amount += Number(
//                           ele.amount
//                         );
//                       } else if (ele.credit_tat == "Within T+2") {
//                         obj.nonLiquid.creditmark.within_count += Number(
//                           ele.count
//                         );
//                         obj.nonLiquid.creditmark.within_amount += Number(
//                           ele.amount
//                         );
//                       } else if (ele.credit_tat == "Beyond T+2") {
//                         obj.nonLiquid.creditmark.beyond_count += Number(
//                           ele.count
//                         );
//                         obj.nonLiquid.creditmark.beyond_amount += Number(
//                           ele.amount
//                         );
//                       } else {
//                         obj.nonLiquid.creditmark.pending_count += Number(
//                           ele.count
//                         );
//                         obj.nonLiquid.creditmark.pending_amount += Number(
//                           ele.amount
//                         );
//                       }
//                     }
//                     if (ele.qc_tat != null) {
//                       obj.nonLiquid.total_qc_count += Number(ele.count);
//                       obj.nonLiquid.total_qc_amount += Number(ele.amount);
//                       if (ele.qc_tat == "Same day") {
//                         obj.nonLiquid.qualityCheck.sameday_count += Number(
//                           ele.count
//                         );
//                         obj.nonLiquid.qualityCheck.sameday_amount += Number(
//                           ele.amount
//                         );
//                       } else if (ele.qc_tat == "Within T+2") {
//                         obj.nonLiquid.qualityCheck.within_count += Number(
//                           ele.count
//                         );
//                         obj.nonLiquid.qualityCheck.within_amount += Number(
//                           ele.amount
//                         );
//                       } else if (ele.qc_tat == "Beyond T+2") {
//                         obj.nonLiquid.qualityCheck.beyond_count += Number(
//                           ele.count
//                         );
//                         obj.nonLiquid.qualityCheck.beyond_amount += Number(
//                           ele.amount
//                         );
//                       } else {
//                         obj.nonLiquid.qualityCheck.beyond_count += Number(
//                           ele.count
//                         );
//                         obj.nonLiquid.qualityCheck.beyond_amount += Number(
//                           ele.amount
//                         );
//                       }
//                     }
//                     if (ele.endorse_tat != null) {
//                       obj.nonLiquid.total_endorsement_amount += Number(
//                         ele.amount
//                       );
//                       obj.nonLiquid.total_endorsement_count += Number(ele.count);
//                       if (ele.endorse_tat == "Same day") {
//                         obj.nonLiquid.endorsement.sameday_count += Number(
//                           ele.count
//                         );
//                         obj.nonLiquid.endorsement.sameday_amount += Number(
//                           ele.amount
//                         );
//                       } else if (ele.endorse_tat == "Within T+2") {
//                         obj.nonLiquid.endorsement.within_count += Number(
//                           ele.count
//                         );
//                         obj.nonLiquid.endorsement.within_amount += Number(
//                           ele.amount
//                         );
//                       } else if (ele.endorse_tat == "Beyond T+2") {
//                         obj.nonLiquid.endorsement.beyond_count += Number(
//                           ele.count
//                         );
//                         obj.nonLiquid.endorsement.beyond_amount += Number(
//                           ele.amount
//                         );
//                       } else {
//                         obj.nonLiquid.endorsement.pending_count += Number(
//                           ele.count
//                         );
//                         obj.nonLiquid.endorsement.pending_amount += Number(
//                           ele.amount
//                         );
//                       }
//                     }
//                   } else {
//                     if (ele.credit_tat != null) {
//                       obj.liquid.total_creditmarked_count += Number(ele.count);
//                       obj.liquid.total_creditmarked_amount += Number(ele.amount);
//                       if (ele.credit_tat == "Same day") {
//                         obj.liquid.creditmark.sameday_count += Number(ele.count);
//                         obj.liquid.creditmark.sameday_amount += Number(
//                           ele.amount
//                         );
//                       } else if (ele.credit_tat == "Within T+2") {
//                         obj.liquid.creditmark.within_count += Number(ele.count);
//                         obj.liquid.creditmark.within_amount += Number(ele.amount);
//                       } else if (ele.credit_tat == "Beyond T+2") {
//                         obj.liquid.creditmark.beyond_count += Number(ele.count);
//                         obj.liquid.creditmark.beyond_amount += Number(ele.amount);
//                       } else {
//                         obj.liquid.creditmark.pending_count += Number(ele.count);
//                         obj.liquid.creditmark.pending_amount += Number(
//                           ele.amount
//                         );
//                       }
//                     }
//                     if (ele.qc_tat != null) {
//                       obj.liquid.total_qc_count += Number(ele.count);
//                       obj.liquid.total_qc_amount += Number(ele.amount);
//                       if (ele.qc_tat == "Same day") {
//                         obj.liquid.qualityCheck.sameday_count += Number(
//                           ele.count
//                         );
//                         obj.liquid.qualityCheck.sameday_amount += Number(
//                           ele.amount
//                         );
//                       } else if (ele.qc_tat == "Within T+2") {
//                         obj.liquid.qualityCheck.within_count += Number(ele.count);
//                         obj.liquid.qualityCheck.within_amount += Number(
//                           ele.amount
//                         );
//                       } else if (ele.qc_tat == "Beyond T+2") {
//                         obj.liquid.qualityCheck.beyond_count += Number(ele.count);
//                         obj.liquid.qualityCheck.beyond_amount += Number(
//                           ele.amount
//                         );
//                       } else {
//                         obj.liquid.qualityCheck.pending_count += Number(
//                           ele.count
//                         );
//                         obj.liquid.qualityCheck.pending_amount += Number(
//                           ele.amount
//                         );
//                       }
//                     }
//                     if (ele.endorse_tat != null) {
//                       obj.liquid.total_endorsement_amount += Number(ele.amount);
//                       obj.liquid.total_endorsement_count += Number(ele.count);
//                       if (ele.endorse_tat == "Same day") {
//                         obj.liquid.endorsement.sameday_count += Number(ele.count);
//                         obj.liquid.endorsement.sameday_amount += Number(
//                           ele.amount
//                         );
//                       } else if (ele.endorse_tat == "Within T+2") {
//                         obj.liquid.endorsement.within_count += Number(ele.count);
//                         obj.liquid.endorsement.within_amount += Number(
//                           ele.amount
//                         );
//                       } else if (ele.endorse_tat == "Beyond T+2") {
//                         obj.liquid.endorsement.beyond_count += Number(ele.count);
//                         obj.liquid.endorsement.beyond_amount += Number(
//                           ele.amount
//                         );
//                       } else {
//                         obj.liquid.endorsement.pending_count += Number(ele.count);
//                         obj.liquid.endorsement.pending_amount += Number(
//                           ele.amount
//                         );
//                       }
//                     }
//                   }
//                 });

//                 dataObject.assetClassBifurcation1 = obj;
//               } else if (item.name == "top cities(combine)") {
//                 let sortedByAmount = rows
//                   .slice()
//                   .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
//                 let sortedByCount = rows
//                   .slice()
//                   .sort(
//                     (a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn)
//                   );

//                 dataObject.topcitiescombine = {
//                   sortedByAmount: sortedByAmount,
//                   sortedByCount: sortedByCount,
//                 };
//               } else if (item.name == "top cities(liquid)") {
//                 let sortedByAmount = rows
//                   .slice()
//                   .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
//                 let sortedByCount = rows
//                   .slice()
//                   .sort(
//                     (a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn)
//                   );

//                 dataObject.topcitiesliquid = {
//                   sortedByAmount: sortedByAmount,
//                   sortedByCount: sortedByCount,
//                 };
//               } else if (item.name == "tr stage bifurcation_Endorsement") {

//                 let tr_Stage_Bifurcation_Endorsement = {
//                   liquid: {
//                     beyond_TAT: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                     pending: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                     within_TAT: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                   },
//                   nonLiquid: {
//                     beyond_TAT: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                     pending: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                     within_TAT: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                   },
//                 };

//                 rows.forEach((ele, ind) => {
//                   if (ele.asset_class == "NON LIQUID") {

//                     if (ele.tat == "beyond TAT") {
//                       tr_Stage_Bifurcation_Endorsement.nonLiquid.beyond_TAT.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_Endorsement.nonLiquid.beyond_TAT.sum =
//                         ele.sum;
//                     } else if (ele.tat == "pending") {
//                       tr_Stage_Bifurcation_Endorsement.nonLiquid.pending.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_Endorsement.nonLiquid.pending.sum =
//                         ele.sum;
//                     } else if (ele.tat == "within TAT") {
//                       tr_Stage_Bifurcation_Endorsement.nonLiquid.within_TAT.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_Endorsement.nonLiquid.within_TAT.sum =
//                         ele.sum;
//                     }
//                   } else {
//                     if (ele.tat == "beyond TAT") {

//                       tr_Stage_Bifurcation_Endorsement.liquid.beyond_TAT.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_Endorsement.liquid.beyond_TAT.sum =
//                         ele.sum;
//                     } else if (ele.tat == "pending") {
//                       tr_Stage_Bifurcation_Endorsement.liquid.pending.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_Endorsement.liquid.pending.sum =
//                         ele.sum;
//                     } else if (ele.tat == "within TAT") {
//                       tr_Stage_Bifurcation_Endorsement.liquid.within_TAT.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_Endorsement.liquid.within_TAT.sum =
//                         ele.sum;
//                     }
//                   }
//                 });

//                 dataObject.tr_Stage_Bifurcation_Endorsement =
//                   tr_Stage_Bifurcation_Endorsement;
//               } else if (item.name == "tr stage bifurcation_CreditMarked") {
//                 let tr_Stage_Bifurcation_CreditMarked = {
//                   liquid: {
//                     beyond_TAT: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                     pending: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                     within_TAT: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                   },
//                   nonLiquid: {
//                     beyond_TAT: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                     pending: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                     within_TAT: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                   },
//                 };

//                 rows.forEach((ele, ind) => {
//                   if (ele.asset_class == "NON LIQUID") {

//                     if (ele.tat == "beyond TAT") {

//                       tr_Stage_Bifurcation_CreditMarked.nonLiquid.beyond_TAT.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_CreditMarked.nonLiquid.beyond_TAT.sum =
//                         ele.sum;
//                     } else if (ele.tat == "pending") {
//                       tr_Stage_Bifurcation_CreditMarked.nonLiquid.pending.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_CreditMarked.nonLiquid.pending.sum =
//                         ele.sum;
//                     } else if (ele.tat == "within TAT") {
//                       tr_Stage_Bifurcation_CreditMarked.nonLiquid.within_TAT.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_CreditMarked.nonLiquid.within_TAT.sum =
//                         ele.sum;
//                     }
//                   } else {

//                     if (ele.tat == "beyond TAT") {

//                       tr_Stage_Bifurcation_CreditMarked.liquid.beyond_TAT.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_CreditMarked.liquid.beyond_TAT.sum =
//                         ele.sum;
//                     } else if (ele.tat == "pending") {
//                       tr_Stage_Bifurcation_CreditMarked.liquid.pending.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_CreditMarked.liquid.pending.sum =
//                         ele.sum;
//                     } else if (ele.tat == "within TAT") {
//                       tr_Stage_Bifurcation_CreditMarked.liquid.within_TAT.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_CreditMarked.liquid.within_TAT.sum =
//                         ele.sum;
//                     }
//                   }
//                 });

//                 dataObject.tr_Stage_Bifurcation_CreditMarked =
//                   tr_Stage_Bifurcation_CreditMarked;
//               } else if (item.name == "tr stage bifurcation_QualityCheck") {
//                 let tr_Stage_Bifurcation_QualityCheck = {
//                   liquid: {
//                     beyond_TAT: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                     pending: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                     within_TAT: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                   },
//                   nonLiquid: {
//                     beyond_TAT: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                     pending: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                     within_TAT: {
//                       amount: 0,
//                       sum: 0,
//                     },
//                   },
//                 };

//                 rows.forEach((ele, ind) => {
//                   if (ele.asset_class == "NON LIQUID") {

//                     if (ele.tat == "beyond TAT") {
//                       tr_Stage_Bifurcation_QualityCheck.nonLiquid.beyond_TAT.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_QualityCheck.nonLiquid.beyond_TAT.sum =
//                         ele.sum;
//                     } else if (ele.tat == "pending") {
//                       tr_Stage_Bifurcation_QualityCheck.nonLiquid.pending.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_QualityCheck.nonLiquid.pending.sum =
//                         ele.sum;
//                     } else if (ele.tat == "within TAT") {
//                       tr_Stage_Bifurcation_QualityCheck.nonLiquid.within_TAT.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_QualityCheck.nonLiquid.within_TAT.sum =
//                         ele.sum;
//                     }
//                   } else {

//                     if (ele.tat == "beyond TAT") {

//                       tr_Stage_Bifurcation_QualityCheck.liquid.beyond_TAT.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_QualityCheck.liquid.beyond_TAT.sum =
//                         ele.sum;
//                     } else if (ele.tat == "pending") {
//                       tr_Stage_Bifurcation_QualityCheck.liquid.pending.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_QualityCheck.liquid.pending.sum =
//                         ele.sum;
//                     } else if (ele.tat == "within TAT") {
//                       tr_Stage_Bifurcation_QualityCheck.liquid.within_TAT.amount =
//                         ele.amount;
//                       tr_Stage_Bifurcation_QualityCheck.liquid.within_TAT.sum =
//                         ele.sum;
//                     }
//                   }
//                 });

//                 dataObject.tr_Stage_Bifurcation_QualityCheck =
//                   tr_Stage_Bifurcation_QualityCheck;

//               } else if (item.name == "top cities(nonliquid)") {
//                 let sortedByAmount = rows
//                   .slice()
//                   .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
//                 let sortedByCount = rows
//                   .slice()
//                   .sort(
//                     (a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn)
//                   );

//                 dataObject.topcitiesnonliquid = {
//                   sortedByAmount: sortedByAmount,
//                   sortedByCount: sortedByCount,
//                 };
//               } else if (item.name == "Radial Chart") {
//                 // dataObject["radialchart"] = rows;
//                 const obj = {
//                   creditmark: {
//                     combine: 0,
//                     liquid: 0,
//                     nonLiquid: 0,
//                   },
//                   endorsed: {
//                     combine: 0,
//                     liquid: 0,
//                     nonLiquid: 0,
//                   },
//                   qualityCheck: {
//                     combine: 0,
//                     liquid: 0,
//                     nonLiquid: 0,
//                   },
//                   totalcount: 0,
//                   endorsed_liquid_percentage: 0,
//                   endorsed_noniquid_percentage: 0,
//                   endorsed_combine_percentage: 0,
//                 };
//                 rows.forEach((ele, ind) => {
//                   obj.totalcount += Number(ele.count);
//                   if (ele.asset_class == "LIQUID") {
//                     if (ele["endorsed"] == "Yes")
//                       obj["endorsed"]["liquid"] += Number(ele.count);
//                     if (ele["credit_marked"] == "Yes")
//                       obj["creditmark"]["liquid"] += Number(ele.count);
//                     if (ele["qcmarked"] == "Yes")
//                       obj["qualityCheck"]["liquid"] += Number(ele.count);
//                   } else {
//                     if (ele["endorsed"] == "Yes")
//                       obj["endorsed"]["nonLiquid"] += Number(ele.count);
//                     if (ele["credit_marked"] == "Yes")
//                       obj["creditmark"]["nonLiquid"] += Number(ele.count);
//                     if (ele["qcmarked"] == "Yes")
//                       obj["qualityCheck"]["nonLiquid"] += Number(ele.count);
//                   }
//                 });

//                 obj.creditmark.combine =
//                   obj.creditmark.liquid + obj.creditmark.nonLiquid;
//                 obj.endorsed.combine =
//                   obj.endorsed.liquid + obj.endorsed.nonLiquid;
//                 obj.qualityCheck.combine =
//                   obj.qualityCheck.liquid + obj.qualityCheck.nonLiquid;

//                 obj.endorsed_combine_percentage =
//                   (obj.endorsed.combine /
//                     dataObject.trhistory.combined.total_transaction_count) *
//                   100;
//                 obj.endorsed_liquid_percentage =
//                   (obj.endorsed.liquid /
//                     dataObject.trhistory.liquid.total_transaction_count) *
//                   100;
//                 obj.endorsed_noniquid_percentage =
//                   (obj.endorsed.nonLiquid /
//                     dataObject.trhistory.nonliquid.total_transaction_count) *
//                   100;

//                 if (isNaN(obj.endorsed_combine_percentage))
//                   obj.endorsed_combine_percentage = 0;
//                 if (isNaN(obj.endorsed_liquid_percentage))
//                   obj.endorsed_liquid_percentage = 0;
//                 if (isNaN(obj.endorsed_noniquid_percentage))
//                   obj.endorsed_noniquid_percentage = 0;

//                 dataObject.radialchart = obj;
//               } else if (item.name == "Top 50 Transactions(nonliquid)") {
//                 dataObject["Top 50 Transactions(nonliquid"] = rows;
//               } else if (item.name == "Top 50 Transactions(liquid)") {
//                 dataObject["Top 50 Transactions(liquid"] = rows;
//               } else if (item.name == "Top 50 Transactions(combine)") {
//                 dataObject["Top 50 Transactions(combine"] = rows;
//               } else if (
//                 item.name == "Top 50 Transactions(till Date)(nonliquid)"
//               ) {
//                 dataObject["Top 50 Transactions(till Date)(nonliquid)"] = rows;
//               } else if (item.name == "Top 50 Transactions(till Date)(liquid)") {
//                 dataObject["Top 50 Transactions(till Date)(liquid)"] = rows;
//               } else if (item.name == "Top 50 Transactions(till Date)(combine)") {
//                 dataObject["Top 50 Transactions(till Date)(combine)"] = rows;
//               } else if (item.name == "Holiday") {
//                 dataObject.isHoliday = rows[0];
//               }
//               resolve(dataObject);
//             });
//           });

//         const main = async () => {
//           const array = [
//             {
//               name: "tr history",
//               query: query2,
//             },
//             //  {
//             //   name:"penindg tr count",
//             //   query: query4
//             //  },
//             {
//               name: "tr bifurcation",
//               query: query5,
//             },
//             {
//               name: "top pending count",
//               query: query6,
//             },
//             {
//               name: "tr stage bifurcation",
//               query: query7,
//             },
//             {
//               name: "Pending count with TAT",
//               query: query8,
//             },
//             {
//               name: "Average Transaction(per minute)",
//               query: query9,
//             },
//             {
//               name: "Top 50 Transactions(nonliquid)",
//               query: query10,
//             },
//             {
//               name: "Top 50 Transactions(liquid)",
//               query: query11,
//             },
//             {
//               name: "Top 50 Transactions(combine)",
//               query: query12,
//             },
//             {
//               name: "Top 50 Transactions(till Date)(nonliquid)",
//               query: query13,
//             },
//             {
//               name: "Top 50 Transactions(till Date)(liquid)",
//               query: query17,
//             },
//             {
//               name: "tr stage bifurcation_QualityCheck",
//               query: query7_tr_stage_bifurcation_QualityCheck,
//             },
//             {
//               name: "tr stage bifurcation_CreditMarked",
//               query: query7_tr_stage_bifurcation_CreditMarked,
//             },
//             {
//               name: "tr stage bifurcation_Endorsement",
//               query: query7_tr_stage_bifurcation_Endorsement,
//             },
//             {
//               name: "Top 50 Transactions(till Date)(combine)",
//               query: query18,
//             },
//             {
//               name: "top cities(combine)",
//               query: query14,
//             },
//             {
//               name: "top cities(liquid)",
//               query: query15,
//             },
//             {
//               name: "top cities(nonliquid)",
//               query: query16,
//             },
//             {
//               name: "Radial Chart",
//               query: query19,
//             },
//             {
//               name: "Holiday",
//               query: query20,
//             },
//           ];

//           const all_promises = await fetch_query(array);

//           const all_result = await Promise.all(all_promises);
//           dataObject = await calculate_result(all_result);

//           const cachedData = {
//             trhistory: {
//               combined: dataObject.trhistory.combined,
//               liquid: dataObject.trhistory.liquid,
//               nonliquid: dataObject.trhistory.nonliquid,
//             },
//             assetClassBifurcation1: {
//               combine: dataObject.assetClassBifurcation1.combine,
//               liquid: dataObject.assetClassBifurcation1.liquid,
//               nonLiquid: dataObject.assetClassBifurcation1.nonLiquid,
//               total_liquid_completed_percentage:
//                 dataObject.assetClassBifurcation1
//                   .total_liquid_completed_percentage,
//               total_nonliquid_completed_percentage:
//                 dataObject.assetClassBifurcation1
//                   .total_nonliquid_completed_percentage,
//             },

//             tr_Stage_Bifurcation_Endorsement:
//               dataObject.tr_Stage_Bifurcation_Endorsement,
//             tr_Stage_Bifurcation_CreditMarked:
//               dataObject.tr_Stage_Bifurcation_CreditMarked,
//             tr_Stage_Bifurcation_QualityCheck:
//               dataObject.tr_Stage_Bifurcation_QualityCheck,
//             tillDatePendingTATObject: {
//               combine: dataObject.tillDatePendingTATObject.combine,
//               liquid: dataObject.tillDatePendingTATObject.liquid,
//               nonliquid: dataObject.tillDatePendingTATObject.nonliquid,
//             },
//             specificDatePendingTATObject: {
//               combine: dataObject.specificDatePendingTATObject.combine,
//               liquid: dataObject.specificDatePendingTATObject.liquid,
//               nonliquid: dataObject.specificDatePendingTATObject.nonliquid,
//             },
//             avgTraxCompletedPerMin: {
//               LIQUID: dataObject.avgTraxCompletedPerMin.LIQUID,
//               NONLIQUID: dataObject.avgTraxCompletedPerMin.NONLIQUID,
//               Combine: dataObject.avgTraxCompletedPerMin.Combine,
//             },
//             topcitiescombine: {
//               sortedByAmount: dataObject.topcitiescombine.sortedByAmount,
//               sortedByCount: dataObject.topcitiescombine.sortedByCount,
//             },
//             topcitiesliquid: {
//               sortedByAmount: dataObject.topcitiesliquid.sortedByAmount,
//               sortedByCount: dataObject.topcitiesliquid.sortedByCount,
//             },
//             topcitiesnonliquid: {
//               sortedByAmount: dataObject.topcitiesnonliquid.sortedByAmount,
//               sortedByCount: dataObject.topcitiesnonliquid.sortedByCount,
//             },

//             radialchart: {
//               creditmark: dataObject.radialchart.creditmark,
//               endorsed: dataObject.radialchart.endorsed,
//               qualityCheck: dataObject.radialchart.qualityCheck,
//               totalcount: dataObject.radialchart.totalcount,
//               endorsed_liquid_percentage:
//                 dataObject.radialchart.endorsed_liquid_percentage,
//               endorsed_noniquid_percentage:
//                 dataObject.radialchart.endorsed_noniquid_percentage,
//               endorsed_combine_percentage:
//                 dataObject.radialchart.endorsed_combine_percentage,
//             },
//             transactionClassBifurcation: {
//               combine: dataObject.transactionClassBifurcation.combine,
//               liquid: dataObject.transactionClassBifurcation.liquid,
//               nonLiquid: dataObject.transactionClassBifurcation.nonLiquid,
//             },
//             "Top 50 Transactions(nonliquid":
//               dataObject["Top 50 Transactions(nonliquid"],
//             "Top 50 Transactions(liquid":
//               dataObject["Top 50 Transactions(liquid"],
//             "Top 50 Transactions(combine":
//               dataObject["Top 50 Transactions(combine"],
//             "Top 50 Transactions(till Date)(nonliquid)":
//               dataObject["Top 50 Transactions(till Date)(nonliquid)"],
//             "Top 50 Transactions(till Date)(liquid)":
//               dataObject["Top 50 Transactions(till Date)(liquid)"],
//             "Top 50 Transactions(till Date)(combine)":
//               dataObject["Top 50 Transactions(till Date)(combine)"],
//             isHoliday: dataObject.isHoliday,
//           };

//           const cached_data = await OverViewMaster.findOneAndUpdate(
//             { date: transaction_date },
//             { $set: { date: transaction_date, dataObject: cachedData } },
//             {
//               upsert: true,
//               new: true,
//             }
//           );

//           dataObject = cached_data.dataObject;

//         };
//         await main();

//         return dataObject
//       }ā
//     } catch (e) {
//       NonLiquid.log("Error while loverview controller", e);
//     }
//   };
