import { getPoolByFund } from "../../lib/postgressConnect.js";
import OverViewMasterNew from "../../models/newoverview.model.js";
import CachedData from "../../models/cacheddata.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";

export const handle_stage_bifurcation = async (
  trdate,
  fundcode,
  snsstatus = false
) => {
  try {
    const transaction_date = trdate;
    let fund = fundcode;
    let fund_schema = getFundWiseSchema(fund);
    const pool = getPoolByFund(fund);
    if (pool == null) console.log("No Pool Connections For This AMC", fund);
    const keysToCheck = [
      // "tr_Stage_Bifurcation_Endorsement",
      // "tr_Stage_Bifurcation_CreditMarked",
      // "tr_Stage_Bifurcation_QualityCheck",
      "radialchart",
      "trhistory",
      "tillDatePendingTATObject",
      "specificDatePendingTATObject",
    ];
    const isDataPresent = await CachedData.findOne({
      $and: [
        { fund: fund, date: transaction_date, type: "overview" },
        {
          $or: keysToCheck.map((key) => ({
            [`dataObject.${key}`]: { $exists: true },
          })),
        },
      ],
    });
    if (!snsstatus && isDataPresent != null) {
      return isDataPresent.dataObject;
    } else {
      let dataObject = {};

      const result = await OverViewMasterNew.find({ date: transaction_date });

      //   const query1 = `select distinct 'qc' as stage,
      //   asset_class,
      //   (
      //     case
      //       when (asset_class = 'LIQUID')
      //       and (qcstatus = 'qc done') then (
      //         case
      //           when (
      //             (diff - h_count <= 0)
      //             or (qcenddt is null)
      //           ) then 'within TAT'
      //           when diff - h_count >= 1 then 'beyond TAT'
      //         end
      //       )
      //       when (asset_class = 'NON LIQUID')
      //       and (qcstatus = 'qc done') then (
      //         case
      //           when (
      //             (diff - h_count <= 1)
      //             or (qcenddt is null)
      //           ) then 'within TAT'
      //           when diff - h_count >= 2 then 'beyond TAT'
      //         end
      //       ) else 'pending'
      //     end
      //   ) tat,
      //   sum(cast(cramt as decimal)) as amount,
      //   sum(count)
      // from (
      //     select distinct asset_class,
      //       DATE_PART(
      //         'day',
      //         nullif(qcenddt, '')::timestamp - nullif(trdt, '')::timestamp
      //       ) diff,
      //       qcstatus,
      //       trdt,
      //       qcenddt,
      //       cramt,
      //       count(
      //         distinct(
      //           concat(
      //             coalesce(scheme, ''),
      //             coalesce(plan, ''),
      //             coalesce(branch, ''),
      //             coalesce(trno, ''),
      //             coalesce(folio, '')
      //           )
      //         )
      //       ) as count,
      //       count(distinct hm_hdate) h_count
      //     from schema.final_batchclosed d
      //       left join schema.holiday_master_mirror b on d.fund = b.hm_fund
      //       and d.scheme = b.hm_scheme
      //       and d.plan = b.hm_plan
      //       and cast(hm_hdate as date) between cast(nullif(trdt, '') as date)
      //       and cast(nullif(qcenddt, '') as date)
      //       and hm_hdate >= '2023-04-01'
      //       and b.hm_fund = '${fund}'
      //       and d.fund = '${fund}'
      //       and cast(hm_hdate as date) <= current_date
      //     where substring(trdt, 1, 10) = '${transaction_date}'
      //     group by 1,
      //       2,
      //       3,
      //       4,
      //       5,
      //       6
      //   ) a
      // group by 1,
      //   2,
      //   3 `;

      //   const query2 = `select distinct 'credit' as stage,
      //   asset_class,
      //   (
      //     case
      //       when (asset_class = 'LIQUID')
      //       and (
      //         cleardt is not null
      //         and cleardt != ''
      //         and cleardt not like '1900%'
      //       ) then (
      //         case
      //           when diff - h_count <= 0 then 'within TAT'
      //           when diff - h_count >= 1 then 'beyond TAT' else 'credit update pending'
      //         end
      //       )
      //       when (asset_class = 'NON LIQUID')
      //       and (
      //         cleardt is not null
      //         and cleardt != ''
      //         and cleardt not like '1900%'
      //       ) then (
      //         case
      //           when diff - h_count <= 1 then 'within TAT'
      //           when diff - h_count >= 2 then 'beyond TAT' else 'credit update pending'
      //         end
      //       ) else 'pending'
      //     end
      //   ) tat,
      //   sum(cast(cramt as decimal)) as amount,
      //   sum(count)
      // from (
      //     select distinct asset_class,
      //       DATE_PART(
      //         'day',
      //         nullif(cast(coalesce(cleardtupdt, enddt) as text), '')::timestamp - nullif(cleardt, '')::timestamp
      //       ) diff,
      //       cleardt,
      //       cleardtupdt,
      //       cramt,
      //       count(
      //         distinct(
      //           concat(
      //             coalesce(scheme, ''),
      //             coalesce(plan, ''),
      //             coalesce(branch, ''),
      //             coalesce(trno, ''),
      //             coalesce(folio, '')
      //           )
      //         )
      //       ) as count,
      //       count(distinct hm_hdate) h_count
      //     from schema.final_batchclosed d
      //       left join schema.holiday_master_mirror b on d.fund = b.hm_fund
      //       and d.scheme = b.hm_scheme
      //       and d.plan = b.hm_plan
      //       and cast(hm_hdate as date) between cast(nullif(cleardt, '') as date)
      //       and cast(nullif(cleardtupdt, '') as date)
      //       and hm_hdate >= '2023-04-01'
      //       and b.hm_fund = '${fund}'
      //       and d.fund = '${fund}'
      //       and cast(hm_hdate as date) <= current_date
      //     where substring(trdt, 1, 10) = '${transaction_date}'
      //     group by 1,
      //       2,
      //       3,
      //       4,
      //       5
      //   ) a
      // group by 1,
      //   2,
      //   3 `;

      //   const query3 = `select distinct 'endorsement' as stage,
      //   asset_class,
      //   (
      //     case
      //       when (asset_class = 'LIQUID')
      //       and (
      //         enddt is not null
      //         and enddt != ''
      //         and enddt not like '1900%'
      //         and active_flag = 'Y'
      //       ) then (
      //         case
      //           when diff - h_count <= 0 then 'within TAT'
      //           when diff - h_count >= 1 then 'beyond TAT'
      //         end
      //       )
      //       when (asset_class = 'NON LIQUID')
      //       and (
      //         enddt is not null
      //         and enddt != ''
      //         and enddt not like '1900%'
      //         and active_flag = 'Y'
      //       ) then (
      //         case
      //           when diff - h_count <= 1 then 'within TAT'
      //           when diff - h_count >= 2 then 'beyond TAT'
      //         end
      //       ) else 'pending'
      //     end
      //   ) tat,
      //   sum(cast(cramt as decimal)) as amount,
      //   sum(count)
      // from (
      //     select distinct asset_class,
      //       DATE_PART(
      //         'day',
      //         nullif(cast(enddt as text), '')::timestamp - nullif(navdt, '')::timestamp
      //       ) diff,
      //       enddt,
      //       navdt,
      //       cramt,
      //       active_flag,
      //       count(
      //         distinct(
      //           concat(
      //             coalesce(scheme, ''),
      //             coalesce(plan, ''),
      //             coalesce(branch, ''),
      //             coalesce(trno, ''),
      //             coalesce(folio, '')
      //           )
      //         )
      //       ) as count,
      //       count(distinct hm_hdate) h_count
      //     from schema.final_batchclosed d
      //       left join schema.holiday_master_mirror b on d.fund = b.hm_fund
      //       and d.scheme = b.hm_scheme
      //       and d.plan = b.hm_plan
      //       and cast(hm_hdate as date) between cast(nullif(navdt, '') as date)
      //       and cast(nullif(enddt, '') as date)
      //       and hm_hdate >= '2023-04-01'
      //       and b.hm_fund = '${fund}'
      //       and d.fund = '${fund}'
      //       and cast(hm_hdate as date) <= current_date
      //     where substring(trdt, 1, 10) = '${transaction_date}'
      //     group by 1,
      //       2,
      //       3,
      //       4,
      //       5,
      //       6
      //   ) a
      // group by 1,
      //   2,
      //   3 `;
      const query1 = `select asset_class, trdt, 'CREDIT' stage,
    count(distinct concat(folio,scheme,plan,trno,branch))
    from schema.final_batchclosed
    where (
            cleardt is not null
            and cleardt != ''
            and cleardt not like '1900%'
          )
    and substring(trdt,1,10) = 'transactionDate'
    group by 1,2,3
    union
    select asset_class, trdt, 'CREDIT' stage,
    count(distinct concat(folio,scheme,plan,trno,branch))
    from schema.final_non_batchclosed
    where (
            cleardt is not null
            and cleardt != ''
            and cleardt not like '1900%'
          )
    and substring(trdt,1,10) = '${transaction_date}'
    group by 1,2,3
    union
    select asset_class, trdt, 'QC' stage,
    count(distinct concat(folio,scheme,plan,trno,branch))
    from schema.final_batchclosed
    where (
            (qcstatus is not null and qcstatus !='')
          )
    and substring(trdt,1,10) = 'transactionDate'
    group by 1,2,3
    union
    select asset_class, trdt, 'QC' stage,
    count(distinct concat(folio,scheme,plan,trno,branch))
    from schema.final_non_batchclosed
    where (
          (qcstatus is not null and qcstatus !='')
          )
    and substring(trdt,1,10) = '${transaction_date}'
    group by 1,2,3
    union
    select asset_class, trdt, 'Endorsement' stage,
    count(distinct concat(folio,scheme,plan,trno,branch))
    from schema.final_batchclosed
    where (
            enddt is not null
            and enddt != ''
            and enddt not like '1900%'
                  and active_flag = 'Y'
          )
    and substring(trdt,1,10) = '${transaction_date}'
    group by 1,2,3
    union
    select asset_class, trdt, 'Endorsement' stage,
    count(distinct concat(folio,scheme,plan,trno,branch))
    from schema.final_non_batchclosed
    where (
          enddt is not null
            and enddt != ''
            and enddt not like '1900%'
                  and active_flag = 'Y'
          )
    and substring(trdt,1,10) = '${transaction_date}'
    group by 1,2,3`;

      const query4 = `select distinct *
    from (
        (
          select asset_class,
            'pending_txn_count' as status,
            substring(trdt, 1, 10) as trdt,
            count(
              distinct(
                concat(
                  coalesce(scheme, ''),
                  coalesce(plan, ''),
                  coalesce(branch, ''),
                  coalesce(trno, ''),
                  coalesce(folio, '')
                )
              )
            ) as count,
            sum(cast(cramt as decimal)) as amount
          from schema.final_non_batchclosed
          where substring(trdt, 1, 10) <= '${transaction_date}'
            and fund = '${fund}'
          group by 1,
            2,
            3
          order by trdt desc
          limit 4
        )
        union
        (
          select asset_class,
            'completed_txn_count' as status,
            substring(trdt, 1, 10) as trdt,
            count(
              distinct(
                concat(
                  coalesce(scheme, ''),
                  coalesce(plan, ''),
                  coalesce(branch, ''),
                  coalesce(trno, ''),
                  coalesce(folio, '')
                )
              )
            ) as count,
            sum(cast(cramt as decimal)) as amount
          from schema.final_batchclosed
          where substring(trdt, 1, 10) <= '${transaction_date}'
            and fund = '${fund}'
          group by 1,
            2,
            3
          order by trdt desc
          limit 4
        )
      ) x
    order by trdt desc`;

      const query5 = `select asset_class,
      trdt,
    (diff - h_count) date_part,
      txn_count
    from(
        select asset_class,
          substring(trdt, 1, 10) as trdt,
    (
            DATE_PART(
              'day',
              nullif(cast(current_date as text), '')::timestamp - nullif(trdt, '')::timestamp
            )
          ) as diff,
          count(distinct hm_hdate) h_count,
          count(
            distinct(
              concat(
                coalesce(scheme, ''),
                coalesce(plan, ''),
                coalesce(branch, ''),
                coalesce(trno, ''),
                coalesce(folio, '')
              )
            )
          ) as txn_count
        from schema.final_non_batchclosed d
          left join schema.holiday_master_mirror b on cast(hm_hdate as date) between cast(trdt as date)
          and current_date
          and hm_hdate >= '2023-04-01'
          and cast(hm_hdate as date) <= current_date
        where substring(trdt, 1, 10) <= '${transaction_date}'
          and active_flag = 'P'
          and fund = '${fund}'
        group by 1,
          2,
          3
      ) x`;

      const fetch_results = async (array) => {
        const all_promises = array.map((ele, ind) => {
          let allresponse = [];
          return new Promise(async (resolve, reject) => {
            let response_obj = {};
            const res = await pool.query(ele.query);
            response_obj = {
              name: ele.name,
              result: res.rows,
            };

            allresponse.push(response_obj);

            resolve(response_obj);
          });
        });
        return all_promises;
      };
      let totalcount = 0;

      const calculate_result = async (array) => {
        const obj = {
          creditmark: {
            combine: 0,
            liquid: 0,
            nonLiquid: 0,
          },
          endorsed: {
            combine: 0,
            liquid: 0,
            nonLiquid: 0,
          },
          qualityCheck: {
            combine: 0,
            liquid: 0,
            nonLiquid: 0,
          },
          endorsed_liquid_percentage: 0,
          endorsed_noniquid_percentage: 0,
          endorsed_combine_percentage: 0,
        };

        const dataObject = {
          trhistory: {
            combined: 0,
            liquid: 0,
            nonliquid: 0,
          },
          tillDatePendingTATObject: {
            combine: 0,
            liquid: 0,
            nonliquid: 0,
          },
          specificDatePendingTATObject: {
            combine: 0,
            liquid: 0,
            nonliquid: 0,
          },
        };
        array.forEach((item) => {
          const rows = item.result;
          if (item.name == "tr history") {
            let object = {
              combined: {
                total_transaction_count: 0,
                total_transaction_amount: 0,
                pending_transaction_count: 0,
                previous_total_transaction_count: 0,
                within_t2_transaction: 0,
                beyond_t2_transaction: 0,
                total_completed_transaction_count: 0,
                transaction_status: "",
                percentage_difference: 0,
              },
              liquid: {
                total_transaction_count: 0,
                total_transaction_amount: 0,
                pending_transaction_count: 0,
                previous_total_transaction_count: 0,
                within_t2_transaction: 0,
                beyond_t2_transaction: 0,
                total_completed_transaction_count: 0,
                transaction_status: "",
                percentage_difference: 0,
              },
              nonliquid: {
                total_transaction_count: 0,
                total_transaction_amount: 0,
                pending_transaction_count: 0,
                previous_total_transaction_count: 0,
                within_t2_transaction: 0,
                beyond_t2_transaction: 0,
                total_completed_transaction_count: 0,
                transaction_status: "",
                percentage_difference: 0,
              },
            };
            let previous_date;

            for (let i = 0; i < rows.length; i++) {
              if (rows[i].trdt != transaction_date) {
                previous_date = rows[i].trdt;
                break;
              }
            }

            rows.forEach((ele, ind) => {
              if (ele.asset_class == "LIQUID") {
                if (ele.trdt == transaction_date) {
                  object.liquid.total_transaction_count += Number(ele.count);
                  object.liquid.total_transaction_amount += Number(ele.amount);
                  if (ele.status == "pending_txn_count")
                    object.liquid.pending_transaction_count += Number(
                      ele.count
                    );
                } else if (ele.trdt == previous_date) {
                  object.liquid.previous_total_transaction_count += Number(
                    ele.count
                  );
                  if (ele.status == "pending_txn_count") {
                    object.liquid.previous_pending_total_transaction_count +=
                      Number(ele.count);
                  }
                }
              } else {
                if (ele.trdt == transaction_date) {
                  object.nonliquid.total_transaction_count += Number(ele.count);
                  object.nonliquid.total_transaction_amount += Number(
                    ele.amount
                  );
                  if (ele.status == "pending_txn_count")
                    object.nonliquid.pending_transaction_count += Number(
                      ele.count
                    );
                } else if (ele.trdt == previous_date) {
                  object.nonliquid.previous_total_transaction_count += Number(
                    ele.count
                  );
                  if (ele.status == "pending_txn_count") {
                    object.nonliquid.previous_pending_total_transaction_count +=
                      Number(ele.count);
                  }
                }
              }
            });

            object.combined.previous_total_transaction_count =
              object.liquid.previous_total_transaction_count +
              object.nonliquid.previous_total_transaction_count;
            object.combined.pending_transaction_count =
              object.liquid.pending_transaction_count +
              object.nonliquid.pending_transaction_count;
            object.combined.total_transaction_amount =
              object.liquid.total_transaction_amount +
              object.nonliquid.total_transaction_amount;
            object.combined.total_transaction_count =
              object.liquid.total_transaction_count +
              object.nonliquid.total_transaction_count;
            object.liquid.total_completed_transaction_count =
              object.liquid.total_transaction_count -
              object.liquid.pending_transaction_count;
            object.nonliquid.total_completed_transaction_count =
              object.nonliquid.total_transaction_count -
              object.nonliquid.pending_transaction_count;
            object.combined.total_completed_transaction_count =
              object.liquid.total_completed_transaction_count +
              object.nonliquid.total_completed_transaction_count -
              (object.liquid.pending_transaction_count +
                object.nonliquid.pending_transaction_count);
            if (object.liquid.pending_transaction_count > 0)
              object.liquid.transaction_status = "Pending";
            else object.liquid.transaction_status = "Completed";
            if (object.nonliquid.pending_transaction_count > 0)
              object.nonliquid.transaction_status = "Pending";
            else object.nonliquid.transaction_status = "Completed";
            if (object.combined.pending_transaction_count > 0)
              object.combined.transaction_status = "Pending";
            else object.combined.transaction_status = "Completed";

            let liquid_percentage =
              ((object.liquid.total_transaction_count -
                object.liquid.previous_total_transaction_count) /
                object.liquid.previous_total_transaction_count) *
              100;
            let nonliquid_percentage =
              ((object.nonliquid.total_transaction_count -
                object.nonliquid.previous_total_transaction_count) /
                object.nonliquid.previous_total_transaction_count) *
              100;
            let combined_percentage =
              ((object.combined.total_transaction_count -
                object.combined.previous_total_transaction_count) /
                object.combined.previous_total_transaction_count) *
              100;
            object.liquid.percentage_difference = liquid_percentage;
            object.nonliquid.percentage_difference = nonliquid_percentage;
            object.combined.percentage_difference = combined_percentage;

            dataObject.trhistory = object;
          } else if (item.name == "Pending count with TAT") {
            const rows = item.result;

            let tillDateObject = {
              combine: {
                pending_count: 0,
                within: 0,
                beyond: 0,
                total_pending_count: 0,
              },
              liquid: {
                pending_count: 0,
                within: 0,
                beyond: 0,
                total_pending_count: 0,
              },
              nonliquid: {
                pending_count: 0,
                within: 0,
                beyond: 0,
                total_pending_count: 0,
              },
            };

            let specificDateObject = {
              combine: {
                pending_count: 0,
                within: 0,
                beyond: 0,
                total_pending_count: 0,
              },
              liquid: {
                pending_count: 0,
                within: 0,
                beyond: 0,
                total_pending_count: 0,
              },
              nonliquid: {
                pending_count: 0,
                within: 0,
                beyond: 0,
                total_pending_count: 0,
              },
            };

            rows.forEach((ele, ind) => {
              if (ele.trdt <= transaction_date) {
                const assetType =
                  ele.asset_class === "LIQUID" ? "liquid" : "nonliquid";
                const transactionType =
                  Number(ele.date_part) <= 2 ? "within" : "beyond";
                tillDateObject[assetType][transactionType] += Number(
                  ele.txn_count
                );
                tillDateObject[assetType]["total_pending_count"] += Number(
                  ele.txn_count
                );
              }
              if (ele.trdt == transaction_date) {
                const assetType =
                  ele.asset_class === "LIQUID" ? "liquid" : "nonliquid";
                const transactionType =
                  Number(ele.date_part) <= 2 ? "within" : "beyond";

                specificDateObject[assetType][transactionType] += Number(
                  ele.txn_count
                );
                specificDateObject[assetType]["total_pending_count"] += Number(
                  ele.txn_count
                );
              }
            });

            tillDateObject["combine"]["within"] =
              tillDateObject["liquid"]["within"] +
              tillDateObject["nonliquid"]["within"];
            specificDateObject["combine"]["within"] =
              specificDateObject["liquid"]["within"] +
              specificDateObject["nonliquid"]["within"];
            tillDateObject["combine"]["beyond"] =
              tillDateObject["liquid"]["beyond"] +
              tillDateObject["nonliquid"]["beyond"];
            specificDateObject["combine"]["beyond"] =
              specificDateObject["liquid"]["beyond"] +
              specificDateObject["nonliquid"]["beyond"];
            tillDateObject["combine"]["total_pending_count"] =
              tillDateObject["liquid"]["total_pending_count"] +
              tillDateObject["nonliquid"]["total_pending_count"];
            specificDateObject["combine"]["total_pending_count"] =
              specificDateObject["liquid"]["total_pending_count"] +
              specificDateObject["nonliquid"]["total_pending_count"];

            dataObject.tillDatePendingTATObject = tillDateObject;
            dataObject.specificDatePendingTATObject = specificDateObject;
          } else if (item.name == "radialchart") {
            const obj = {
              creditmark: {
                combine: 0,
                liquid: 0,
                nonLiquid: 0,
              },
              endorsed: {
                combine: 0,
                liquid: 0,
                nonLiquid: 0,
              },
              qualityCheck: {
                combine: 0,
                liquid: 0,
                nonLiquid: 0,
              },
              totalcount: 0,
              endorsed_liquid_percentage: 0,
              endorsed_noniquid_percentage: 0,
              endorsed_combine_percentage: 0,
            };

            rows.forEach((ele, ind) => {
              if (ele.stage == "CREDIT") {
                obj.creditmark.combine += Number(ele.count);
              } else if (ele.stage == "Endorsement") {
                obj.endorsed.combine += Number(ele.count);
              } else if (ele.stage == "QC") {
                obj.qualityCheck.combine += Number(ele.count);
              }

              if (ele.asset_class == "LIQUID") {
                if (ele.stage == "CREDIT") {
                  obj.creditmark.liquid += Number(ele.count);
                } else if (ele.stage == "Endorsement") {
                  obj.endorsed.liquid += Number(ele.count);
                } else if (ele.stage == "QC") {
                  obj.qualityCheck.liquid += Number(ele.count);
                }
              } else if (ele.asset_class == "NON LIQUID") {
                if (ele.stage == "CREDIT") {
                  obj.creditmark.nonLiquid += Number(ele.count);
                } else if (ele.stage == "Endorsement") {
                  obj.endorsed.nonLiquid += Number(ele.count);
                } else if (ele.stage == "QC") {
                  obj.qualityCheck.nonLiquid += Number(ele.count);
                }
              }
            });
            dataObject.radialchart = obj;
          }
          // if (item.name == "tr stage bifurcation_Endorsement") {
          //   let tr_Stage_Bifurcation_Endorsement = {
          //     liquid: {
          //       beyond_TAT: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //       pending: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //       within_TAT: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //     },
          //     nonLiquid: {
          //       beyond_TAT: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //       pending: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //       within_TAT: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //     },
          //   };

          //   rows.forEach((ele, ind) => {
          //     totalcount += Number(ele.sum);
          //     if (ele.asset_class == "NON LIQUID") {
          //       obj.endorsed.nonLiquid += Number(ele.sum);
          //       if (ele.tat == "beyond TAT") {
          //         tr_Stage_Bifurcation_Endorsement.nonLiquid.beyond_TAT.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_Endorsement.nonLiquid.beyond_TAT.sum =
          //           ele.sum;
          //       } else if (ele.tat == "pending") {
          //         tr_Stage_Bifurcation_Endorsement.nonLiquid.pending.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_Endorsement.nonLiquid.pending.sum =
          //           ele.sum;
          //       } else if (ele.tat == "within TAT") {
          //         tr_Stage_Bifurcation_Endorsement.nonLiquid.within_TAT.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_Endorsement.nonLiquid.within_TAT.sum =
          //           ele.sum;
          //       }
          //     } else {
          //       obj.endorsed.liquid += Number(ele.sum);

          //       if (ele.tat == "beyond TAT") {
          //         tr_Stage_Bifurcation_Endorsement.liquid.beyond_TAT.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_Endorsement.liquid.beyond_TAT.sum =
          //           ele.sum;
          //       } else if (ele.tat == "pending") {
          //         tr_Stage_Bifurcation_Endorsement.liquid.pending.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_Endorsement.liquid.pending.sum = ele.sum;
          //       } else if (ele.tat == "within TAT") {
          //         tr_Stage_Bifurcation_Endorsement.liquid.within_TAT.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_Endorsement.liquid.within_TAT.sum =
          //           ele.sum;
          //       }
          //     }
          //   });

          //   obj.endorsed.combine = obj.endorsed.liquid + obj.endorsed.nonLiquid;
          //   dataObject.tr_Stage_Bifurcation_Endorsement =
          //     tr_Stage_Bifurcation_Endorsement;
          // } else if (item.name == "tr stage bifurcation_CreditMarked") {
          //   let tr_Stage_Bifurcation_CreditMarked = {
          //     liquid: {
          //       beyond_TAT: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //       pending: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //       within_TAT: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //     },
          //     nonLiquid: {
          //       beyond_TAT: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //       pending: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //       within_TAT: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //     },
          //   };

          //   rows.forEach((ele, ind) => {
          //     totalcount += Number(ele.sum);
          //     if (ele.asset_class == "NON LIQUID") {
          //       obj.creditmark.nonLiquid += Number(ele.sum);
          //       if (ele.tat == "beyond TAT") {
          //         tr_Stage_Bifurcation_CreditMarked.nonLiquid.beyond_TAT.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_CreditMarked.nonLiquid.beyond_TAT.sum =
          //           ele.sum;
          //       } else if (ele.tat == "pending") {
          //         tr_Stage_Bifurcation_CreditMarked.nonLiquid.pending.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_CreditMarked.nonLiquid.pending.sum =
          //           ele.sum;
          //       } else if (ele.tat == "within TAT") {
          //         tr_Stage_Bifurcation_CreditMarked.nonLiquid.within_TAT.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_CreditMarked.nonLiquid.within_TAT.sum =
          //           ele.sum;
          //       }
          //     } else {
          //       obj.creditmark.liquid += Number(ele.sum);
          //       if (ele.tat == "beyond TAT") {
          //         tr_Stage_Bifurcation_CreditMarked.liquid.beyond_TAT.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_CreditMarked.liquid.beyond_TAT.sum =
          //           ele.sum;
          //       } else if (ele.tat == "pending") {
          //         tr_Stage_Bifurcation_CreditMarked.liquid.pending.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_CreditMarked.liquid.pending.sum =
          //           ele.sum;
          //       } else if (ele.tat == "within TAT") {
          //         tr_Stage_Bifurcation_CreditMarked.liquid.within_TAT.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_CreditMarked.liquid.within_TAT.sum =
          //           ele.sum;
          //       }
          //     }
          //   });
          //   obj.creditmark.combine =
          //     obj.creditmark.liquid + obj.creditmark.nonLiquid;
          //   dataObject.tr_Stage_Bifurcation_CreditMarked =
          //     tr_Stage_Bifurcation_CreditMarked;
         
          // } else if (item.name == "tr stage bifurcation_QualityCheck") {
          //   let tr_Stage_Bifurcation_QualityCheck = {
          //     liquid: {
          //       beyond_TAT: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //       pending: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //       within_TAT: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //     },
          //     nonLiquid: {
          //       beyond_TAT: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //       pending: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //       within_TAT: {
          //         amount: 0,
          //         sum: 0,
          //       },
          //     },
          //   };

          //   rows.forEach((ele, ind) => {
          //     totalcount += Number(ele.sum);
          //     if (ele.asset_class == "NON LIQUID") {
          //       obj.qualityCheck.nonLiquid += Number(ele.sum);
          //       if (ele.tat == "beyond TAT") {
          //         tr_Stage_Bifurcation_QualityCheck.nonLiquid.beyond_TAT.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_QualityCheck.nonLiquid.beyond_TAT.sum =
          //           ele.sum;
          //       } else if (ele.tat == "pending") {
          //         tr_Stage_Bifurcation_QualityCheck.nonLiquid.pending.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_QualityCheck.nonLiquid.pending.sum =
          //           ele.sum;
          //       } else if (ele.tat == "within TAT") {
          //         tr_Stage_Bifurcation_QualityCheck.nonLiquid.within_TAT.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_QualityCheck.nonLiquid.within_TAT.sum =
          //           ele.sum;
          //       }
          //     } else {
          //       obj.qualityCheck.liquid += Number(ele.sum);
          //       if (ele.tat == "beyond TAT") {
          //         tr_Stage_Bifurcation_QualityCheck.liquid.beyond_TAT.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_QualityCheck.liquid.beyond_TAT.sum =
          //           ele.sum;
          //       } else if (ele.tat == "pending") {
          //         tr_Stage_Bifurcation_QualityCheck.liquid.pending.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_QualityCheck.liquid.pending.sum =
          //           ele.sum;
          //       } else if (ele.tat == "within TAT") {
          //         tr_Stage_Bifurcation_QualityCheck.liquid.within_TAT.amount =
          //           ele.amount;
          //         tr_Stage_Bifurcation_QualityCheck.liquid.within_TAT.sum =
          //           ele.sum;
          //       }
          //     }
          //   });

          //   obj.qualityCheck.combine =
          //     obj.qualityCheck.liquid + obj.qualityCheck.nonLiquid;

          //   obj["totalcount"] = totalcount;

          //   dataObject.tr_Stage_Bifurcation_QualityCheck =
          //     tr_Stage_Bifurcation_QualityCheck;
          // }
        });

        return dataObject;
      };

      const main = async () => {
        const array = [
          // {
          //   name: "tr stage bifurcation_QualityCheck",
          //   query: query1.replace(/schema/g, `${fund_schema}`),
          // },
          // {
          //   name: "tr stage bifurcation_CreditMarked",
          //   query: query2.replace(/schema/g, `${fund_schema}`),
          // },
          // {
          //   name: "tr stage bifurcation_Endorsement",
          //   query: query3.replace(/schema/g, `${fund_schema}`),
          // },
          {
            name: "radialchart",
            query: query1.replace(/schema/g, `${fund_schema}`),
          },
          {
            name: "tr history",
            query: query4.replace(/schema/g, `${fund_schema}`),
          },
          {
            name: "Pending count with TAT",
            query: query5.replace(/schema/g, `${fund_schema}`),
          },
        ];

        const all_promises = await fetch_results(array);

        const all_result = await Promise.all(all_promises);
        dataObject = await calculate_result(all_result);

        // dataObject.radialchart.endorsed_combine_percentage = (
        //   (dataObject.radialchart.endorsed.combine /
        //     dataObject.trhistory.combined.total_transaction_count) *
        //   100
        // ).toFixed(2);
        // dataObject.radialchart.endorsed_liquid_percentage = (
        //   (dataObject.radialchart.endorsed.liquid /
        //     dataObject.trhistory.liquid.total_transaction_count) *
        //   100
        // ).toFixed(2);
        // dataObject.radialchart.endorsed_noniquid_percentage = (
        //   (dataObject.radialchart.endorsed.nonLiquid /
        //     dataObject.trhistory.nonliquid.total_transaction_count) *
        //   100
        // ).toFixed(2);

        // if (isNaN(dataObject.radialchart.endorsed_combine_percentage))
        //   dataObject.radialchart.endorsed_combine_percentage = 0;
        // if (isNaN(dataObject.radialchart.endorsed_liquid_percentage))
        //   dataObject.radialchart.endorsed_liquid_percentage = 0;
        // if (isNaN(dataObject.radialchart.endorsed_noniquid_percentage))
        //   dataObject.radialchart.endorsed_noniquid_percentage = 0;
        const cachedData = {
          tr_Stage_Bifurcation_Endorsement:
            dataObject.tr_Stage_Bifurcation_Endorsement,
          tr_Stage_Bifurcation_CreditMarked:
            dataObject.tr_Stage_Bifurcation_CreditMarked,
          tr_Stage_Bifurcation_QualityCheck:
            dataObject.tr_Stage_Bifurcation_QualityCheck,
          radialchart: dataObject.radialchart,
        };

        const cached_data = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "overview" },
          {
            $set: {
              date: transaction_date,
              "dataObject.tr_Stage_Bifurcation_Endorsement":
                cachedData.tr_Stage_Bifurcation_Endorsement,
              "dataObject.tr_Stage_Bifurcation_CreditMarked":
                cachedData.tr_Stage_Bifurcation_CreditMarked,
              "dataObject.tr_Stage_Bifurcation_QualityCheck":
                cachedData.tr_Stage_Bifurcation_QualityCheck,
              "dataObject.radialchart": cachedData.radialchart,
              "dataObject.trhistory": dataObject.trhistory,
              "dataObject.tillDatePendingTATObject":
                dataObject.tillDatePendingTATObject,
              "dataObject.specificDatePendingTATObject":
                dataObject.specificDatePendingTATObject,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );

        return cached_data.dataObject;
      };

      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log("Error while fetching top pending transactions", e);
    throw new Error(e);
  }
};
