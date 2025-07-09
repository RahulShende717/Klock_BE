import { getPoolByFund } from "../../lib/postgressConnect.js";
import OverViewMasterNew from "../../models/newoverview.model.js";
import CachedData from "../../models/cacheddata.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";

export const handleTRBifurcation = async (
  trdate,
  fundcode,
  snsstatus = false
) => {
  try {
    const transaction_date = trdate;
    const fund = fundcode;
    let fund_schema = getFundWiseSchema(fund);
    const pool = getPoolByFund(fund);
    if (pool == null) console.log("No Pool Connections For This AMC", fund);
    const keysToCheck = [
      // "transactionClassBifurcation",
      "avgTraxCompletedPerMin",
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

      const query1 = `select distinct asset_class,
      (
        case
          when (
            (asset_class) = 'LIQUID'
            and (diff - h_count) <= 0
          ) then 'with in TAT'
          when (
            (asset_class) = 'LIQUID'
            and (diff - h_count) >= 1
          ) then 'beyond TAT'
          when (
            (asset_class) = 'NON LIQUID'
            and (diff - h_count) <= 1
          ) then 'with in TAT'
          when (
            (asset_class) = 'NON LIQUID'
            and (diff - h_count) = 2
          ) then 'beyond TAT'
          when (
            (asset_class) = 'NON LIQUID'
            and (diff - h_count) > 2
          ) then 'beyond TAT' else 'other'
        end
      ),
      trtype,
      sum(count)
    from (
        select distinct asset_class,
          DATE_PART(
            'day',
            nullif(cast(batchclosedt as text), '')::timestamp - nullif(navdt, '')::timestamp
          ) diff,
          trtype,
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
          count(distinct hm_hdate) h_count
        from schema.final_batchclosed d
          left join schema.holiday_master_mirror b on d.fund = b.hm_fund
          and d.scheme = b.hm_scheme
          and d.plan = b.hm_plan
          and cast(hm_hdate as date) between cast(navdt as date)
          and cast(batchclosedt as date)
          and hm_hdate >= '2023-04-01'
          and cast(hm_hdate as date) <= current_date
        where substring(trdt, 1, 10) = '${transaction_date}'
          and fund = '${fund}'
          and hm_fund = '${fund}'
        group by 1,
          2,
          3
      ) a
    group by 1,
      2,
      3`;

      let query2 = `select distinct asset_class,
      substring(trdt, 1, 10) as trdt,
      (
        case
          when enddt is not null
          and enddt != ''
          and active_flag = 'Y'
          and enddt not like '1900%' then 'Yes' else 'No'
        end
      ) as endorsed,
      (
        case
          when cleardt is not null
          and cleardt != ''
          and cleardt not like '1900%' then 'Yes' else 'No'
        end
      ) as credit_marked,
      (
        case
          when cleardtupdt is not null
          and cleardtupdt != ''
          and cleardt not like '1900%' then 'Yes' else 'No'
        end
      ) as credit_updated,
      (
        case
          when qcenddt is not null
          and qcenddt != '' then 'Yes' else 'No'
        end
      ) as qcmarked,
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
    from (
        (
          select distinct asset_class,
            fund,
            scheme,
            plan,
            branch,
            trno,
            folio,
            trdt,
            enddt,
            active_flag,
            cleardt,
            cleardtupdt,
            qcenddt,
            cramt
          from schema.final_non_batchclosed
          where substring(trdt, 1, 10) = '${transaction_date}'
        )
        union
        (
          select distinct asset_class,
            fund,
            scheme,
            plan,
            branch,
            trno,
            folio,
            trdt,
            enddt,
            active_flag,
            cleardt,
            cleardtupdt,
            qcenddt,
            cramt
          from dbo.final_batchclosed
          where substring(trdt, 1, 10) = '${transaction_date}'
        )
      ) a
    group by 1,
      2,
      3,
      4,
      5,
      6 `;

      const query3 = `select asset_class,
      cast(
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
        ) / 480.0 as decimal
      ) as avg_count
    from schema.final_batchclosed
    where substring(batchclosedt, 1, 10) = '${transaction_date}'
      and fund = '${fund}'
    group by 1`;

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

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;

          if (item.name == "tr bifurcation") {
            let rows = item.result;
            let transactionClassBifurcation = {
              liquid: {
                ADD: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                NEW: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },

                IPO: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
              nonLiquid: {
                ADD: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                NEW: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                IPO: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
            };

            rows.forEach((items) => {
              if (items.asset_class == "LIQUID") {
                if (items.trtype == "NEW") {
                  transactionClassBifurcation.liquid.NEW.total += Number(
                    items.sum
                  );
                  if (items.case == "with in TAT") {
                    transactionClassBifurcation.liquid.NEW.withinTAT = Number(
                      items.sum
                    );
                  }
                  if (items.case == "beyond TAT") {
                    transactionClassBifurcation.liquid.NEW.beyondTAT = Number(
                      items.sum
                    );
                  }
                } else if (items.trtype == "ADD") {
                  transactionClassBifurcation.liquid.ADD.total += Number(
                    items.sum
                  );
                  if (items.case == "with in TAT") {
                    transactionClassBifurcation.liquid.ADD.withinTAT = Number(
                      items.sum
                    );
                  }
                  if (items.case == "beyond TAT") {
                    transactionClassBifurcation.liquid.ADD.beyondTAT = Number(
                      items.sum
                    );
                  }
                } else if (items.trtype == "SIN") {
                  transactionClassBifurcation.liquid.SIN.total += Number(
                    items.sum
                  );
                  if (items.case == "with in TAT") {
                    transactionClassBifurcation.liquid.SIN.withinTAT = Number(
                      items.sum
                    );
                  }
                  if (items.case == "beyond TAT") {
                    transactionClassBifurcation.liquid.SIN.beyondTAT = Number(
                      items.sum
                    );
                  }
                } else if (items.trtype == "IPO") {
                  transactionClassBifurcation.liquid.IPO.total += Number(
                    items.sum
                  );
                  if (items.case == "with in TAT") {
                    transactionClassBifurcation.liquid.IPO.withinTAT = Number(
                      items.sum
                    );
                  }
                  if (items.case == "beyond TAT") {
                    transactionClassBifurcation.liquid.IPO.beyondTAT = Number(
                      items.sum
                    );
                  }
                }
              } else if (items.asset_class == "NON LIQUID") {
                if (items.trtype == "NEW") {
                  transactionClassBifurcation.liquid.NEW.total += Number(
                    items.sum
                  );
                  if (items.case == "with in TAT") {
                    transactionClassBifurcation.nonLiquid.NEW.withinTAT =
                      Number(items.sum);
                  }
                  if (items.case == "beyond TAT") {
                    transactionClassBifurcation.nonLiquid.NEW.beyondTAT =
                      Number(items.sum);
                  }
                } else if (items.trtype == "ADD") {
                  transactionClassBifurcation.nonLiquid.ADD.total += Number(
                    items.sum
                  );

                  if (items.case == "with in TAT") {
                    transactionClassBifurcation.nonLiquid.ADD.withinTAT =
                      Number(items.sum);
                  }
                  if (items.case == "beyond TAT") {
                    transactionClassBifurcation.nonLiquid.ADD.beyondTAT =
                      Number(items.sum);
                  }
                } else if (items.trtype == "SIN") {
                  transactionClassBifurcation.nonLiquid.SIN.total += Number(
                    items.sum
                  );
                  if (items.case == "with in TAT") {
                    transactionClassBifurcation.nonLiquid.SIN.withinTAT =
                      Number(items.sum);
                  }
                  if (items.case == "beyond TAT") {
                    transactionClassBifurcation.nonLiquid.SIN.beyondTAT =
                      Number(items.sum);
                  }
                } else if (items.trtype == "IPO") {
                  transactionClassBifurcation.nonLiquid.IPO.total += Number(
                    items.sum
                  );

                  if (items.case == "with in TAT") {
                    transactionClassBifurcation.nonLiquid.IPO.withinTAT =
                      Number(items.sum);
                  }
                  if (items.case == "beyond TAT") {
                    transactionClassBifurcation.nonLiquid.IPO.beyondTAT =
                      Number(items.sum);
                  }
                }
              }
            });

            dataObject.transactionClassBifurcation =
              transactionClassBifurcation;
          } else if (item.name == "Average Transaction(per minute)") {
            let obj = {
              LIQUID: 0,
              NONLIQUID: 0,
              Combine: 0,
            };

            rows.forEach((items) => {
              if (items.asset_class == "LIQUID") {
                obj.LIQUID = parseInt(Number(items.avg_count));
              } else if (items.asset_class == "NON LIQUID") {
                obj.NONLIQUID = parseInt(Number(items.avg_count));
              }
            });

            obj.Combine = Number(obj.LIQUID) + Number(obj.NONLIQUID);

            dataObject.avgTraxCompletedPerMin = obj;
          } else if (item.name == "Radial Chart") {
            // dataObject["radialchart"] = rows;
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
              obj.totalcount += Number(ele.count);
              if (ele.asset_class == "LIQUID") {
                if (ele["endorsed"] == "Yes")
                  obj["endorsed"]["liquid"] += Number(ele.count);
                if (ele["credit_marked"] == "Yes")
                  obj["creditmark"]["liquid"] += Number(ele.count);
                if (ele["qcmarked"] == "Yes")
                  obj["qualityCheck"]["liquid"] += Number(ele.count);
              } else {
                if (ele["endorsed"] == "Yes")
                  obj["endorsed"]["nonLiquid"] += Number(ele.count);
                if (ele["credit_marked"] == "Yes")
                  obj["creditmark"]["nonLiquid"] += Number(ele.count);
                if (ele["qcmarked"] == "Yes")
                  obj["qualityCheck"]["nonLiquid"] += Number(ele.count);
              }
            });

            obj.creditmark.combine =
              obj.creditmark.liquid + obj.creditmark.nonLiquid;
            obj.endorsed.combine = obj.endorsed.liquid + obj.endorsed.nonLiquid;
            obj.qualityCheck.combine =
              obj.qualityCheck.liquid + obj.qualityCheck.nonLiquid;

            // obj.endorsed_combine_percentage =
            //   (obj.endorsed.combine /
            //     dataObject.trhistory.combine.total_transaction_count) *
            //   100;
            // obj.endorsed_liquid_percentage =
            //   (obj.endorsed.liquid /
            //     dataObject.trhistory.liquid.total_transaction_count) *
            //   100;
            // obj.endorsed_noniquid_percentage =
            //   (obj.endorsed.nonLiquid /
            //     dataObject.trhistory.nonliquid.total_transaction_count) *
            //   100;

            // if (isNaN(obj.endorsed_combine_percentage))
            //   obj.endorsed_combine_percentage = 0;
            // if (isNaN(obj.endorsed_liquid_percentage))
            //   obj.endorsed_liquid_percentage = 0;
            // if (isNaN(obj.endorsed_noniquid_percentage))
            //   obj.endorsed_noniquid_percentage = 0;

            dataObject.radialchart = obj;
          }
        });
        return dataObject;
      };

      const main = async () => {
        const array = [
          // {
          //   name: "tr bifurcation",
          //   query: query1.replace(/schema/g, `${fund_schema}`),
          // },
          // {
          //   name: "Radial Chart",
          //   query: query2,
          // },
          {
            name: "Average Transaction(per minute)",
            query: query3.replace(/schema/g, `${fund_schema}`),
          },
        ];

        const all_promises = await fetch_results(array);

        const all_result = await Promise.all(all_promises);

        dataObject = await calculate_result(all_result);
        const cachedData = {
          // transactionClassBifurcation: {
          //   // combine: dataObject.transactionClassBifurcation.combine,
          //   liquid: dataObject.transactionClassBifurcation.liquid,
          //   nonLiquid: dataObject.transactionClassBifurcation.nonLiquid,
          // },
          // radialchart: {
          //   creditmark: dataObject.radialchart.creditmark,
          //   endorsed: dataObject.radialchart.endorsed,
          //   qualityCheck: dataObject.radialchart.qualityCheck,
          //   totalcount: dataObject.radialchart.totalcount,
          //   endorsed_liquid_percentage:
          //     dataObject.radialchart.endorsed_liquid_percentage,
          //   endorsed_noniquid_percentage:
          //     dataObject.radialchart.endorsed_noniquid_percentage,
          //   endorsed_combine_percentage:
          //     dataObject.radialchart.endorsed_combine_percentage,
          // },
          avgTraxCompletedPerMin: {
            LIQUID: dataObject.avgTraxCompletedPerMin.LIQUID,
            NONLIQUID: dataObject.avgTraxCompletedPerMin.NONLIQUID,
            Combine: dataObject.avgTraxCompletedPerMin.Combine,
          },
        };

        const cached_data = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "overview" },
          {
            $set: {
              date: transaction_date,
              "dataObject.transactionClassBifurcation":
                cachedData.transactionClassBifurcation,
              "dataObject.radialchart": cachedData.radialchart,
              "dataObject.avgTraxCompletedPerMin":
                cachedData.avgTraxCompletedPerMin,
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
