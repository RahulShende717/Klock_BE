import CachedData from "../../models/cacheddata.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import SwpQueries from "../../models/swpqueries.model.js";
import { getPoolObj } from "../../utils/pool.js";

export const swp_summary_provider = async (
  trdate,
  fundcode,
  snsstatus = false
) => {
  try {
    let dataObject = {};
    let transaction_date = trdate;
    let fund = fundcode;
    const pool = getPoolObj(fund);
    let schema = getFundWiseSchema(fund);
    const keysToCheck = [
      "disbursement",
      "qualitycheck_summary",
      "radialchart",
      "master_information",
    ];

    const isDataPresent = await CachedData.findOne({
      $and: [
        { fund: fund, date: transaction_date, type: "swp" },
        {
          $and: keysToCheck.map((key) => ({
            [`dataObject.${key}`]: { $exists: true },
          })),
        },
      ],
    });

    if (!snsstatus && isDataPresent != null) {

      return isDataPresent.dataObject;
    } else {
      const radialchartObj = {
        disbursement: {
          combine: 0,
          liquid: 0,
          nonLiquid: 0,
        },
        fundingsummary: {
          combine: 0,
          liquid: 0,
          nonLiquid: 0,
        },
        qualitycheck: {
          combine: 0,
          liquid: 0,
          nonLiquid: 0,
        },
      };
      const swp_summary_calculation = (rows, summarytype) => {
        let obj = {
          completed: {
            liquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
            nonLiquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
            combine: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
          },
          rejected: {
            liquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
            nonLiquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
            combine: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
          },
          pending: {
            liquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
            nonLiquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
            combine: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
          },
        };

        rows.forEach((ele, ind) => {
          if (ele.asset_class == "NON LIQUID") {
            if (ele.status == "completed") {
              radialchartObj[summarytype].nonLiquid += Number(ele.count);
              if (ele.tat == "within TAT") {
                obj.completed.nonLiquid.within_TAT.amount += Number(ele.amount);
                obj.completed.nonLiquid.within_TAT.sum += Number(ele.count);
              } else {
                obj.completed.nonLiquid.beyond_TAT.amount += Number(ele.amount);
                obj.completed.nonLiquid.beyond_TAT.sum += Number(ele.count);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                obj.pending.nonLiquid.within_TAT.amount += Number(ele.amount);
                obj.pending.nonLiquid.within_TAT.sum += Number(ele.count);
              } else {
                obj.pending.nonLiquid.beyond_TAT.amount += Number(ele.amount);
                obj.pending.nonLiquid.beyond_TAT.sum += Number(ele.count);
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                obj.rejected.nonLiquid.within_TAT.amount += Number(ele.amount);
                obj.rejected.nonLiquid.within_TAT.sum += Number(ele.count);
              } else {
                obj.rejected.nonLiquid.beyond_TAT.amount += Number(ele.amount);
                obj.rejected.nonLiquid.beyond_TAT.sum += Number(ele.count);
              }
            }
          } else {
            if (ele.status == "completed") {
              radialchartObj[summarytype].liquid += Number(ele.count);
              if (ele.tat == "within TAT") {
                obj.completed.liquid.within_TAT.amount += Number(ele.amount);
                obj.completed.liquid.within_TAT.sum += Number(ele.count);
              } else {
                obj.completed.liquid.beyond_TAT.amount += Number(ele.amount);
                obj.completed.liquid.beyond_TAT.sum += Number(ele.count);

              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                obj.pending.liquid.within_TAT.amount += Number(ele.amount);
                obj.pending.liquid.within_TAT.sum += Number(ele.count);
              } else {
                obj.pending.liquid.beyond_TAT.amount += Number(ele.amount);
                obj.pending.liquid.beyond_TAT.sum += Number(ele.count);
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                obj.rejected.liquid.within_TAT.amount += Number(ele.amount);
                obj.rejected.liquid.within_TAT.sum += Number(ele.count);
              } else {
                obj.rejected.liquid.beyond_TAT.amount += Number(ele.amount);
                obj.rejected.liquid.beyond_TAT.sum += Number(ele.count);
              }
            }
          }
        });

        obj.completed.combine.beyond_TAT.amount =
          obj.completed.liquid.beyond_TAT.amount +
          obj.completed.nonLiquid.beyond_TAT.amount;
        obj.completed.combine.beyond_TAT.sum =
          obj.completed.liquid.beyond_TAT.sum +
          obj.completed.nonLiquid.beyond_TAT.sum;
        obj.completed.combine.within_TAT.sum =
          obj.completed.liquid.within_TAT.sum +
          obj.completed.nonLiquid.within_TAT.sum;
        obj.completed.combine.within_TAT.amount =
          obj.completed.liquid.within_TAT.amount +
          obj.completed.nonLiquid.within_TAT.amount;

        obj.pending.combine.beyond_TAT.amount =
          obj.pending.liquid.beyond_TAT.amount +
          obj.pending.nonLiquid.beyond_TAT.amount;
        obj.pending.combine.within_TAT.amount =
          obj.pending.liquid.within_TAT.amount +
          obj.pending.nonLiquid.within_TAT.amount;
        obj.pending.combine.beyond_TAT.sum =
          obj.pending.liquid.beyond_TAT.sum +
          obj.pending.nonLiquid.beyond_TAT.sum;
        obj.pending.combine.within_TAT.sum =
          obj.pending.liquid.within_TAT.sum +
          obj.pending.nonLiquid.within_TAT.sum;

        obj.rejected.combine.beyond_TAT.amount =
          obj.rejected.liquid.beyond_TAT.amount +
          obj.rejected.nonLiquid.beyond_TAT.amount;
        obj.rejected.combine.beyond_TAT.sum =
          obj.rejected.liquid.beyond_TAT.sum +
          obj.rejected.nonLiquid.beyond_TAT.sum;
        obj.rejected.combine.within_TAT.sum =
          obj.rejected.liquid.within_TAT.sum +
          obj.rejected.nonLiquid.within_TAT.sum;
        obj.rejected.combine.within_TAT.amount =
          obj.rejected.liquid.within_TAT.amount +
          obj.rejected.nonLiquid.within_TAT.amount;

        radialchartObj.disbursement.combine =
          radialchartObj.disbursement.liquid +
          radialchartObj.disbursement.nonLiquid;
        radialchartObj.fundingsummary.combine =
          radialchartObj.fundingsummary.liquid +
          radialchartObj.fundingsummary.nonLiquid;
        radialchartObj.qualitycheck.combine =
          radialchartObj.qualitycheck.liquid +
          radialchartObj.qualitycheck.nonLiquid;

        dataObject.radialchart = radialchartObj;

        dataObject[summarytype] = obj;
      };

      dataObject.radialchart = radialchartObj;

      const master_information_calculation = (rows) => {
        let obj = {
          liquid: [],
          nonLiquid: [],
          combine: [],
        };

        rows.forEach((ele, ind) => {
          if (ele.frequency != "others") {
            obj.combine.push(ele);
          }
          if (ele.asset_class == "LIQUID" && ele.frequency != "others") {
            obj.liquid.push(ele);
          }
          if (ele.asset_class == "NON LIQUID" && ele.frequency != "others") {
            obj.nonLiquid.push(ele);
          }
        });

        dataObject.master_information = obj;
      };
      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "qc_summary":
              swp_summary_calculation(rows, "qualitycheck");
              break;
            case "disbursement_summay":
              swp_summary_calculation(rows, "disbursement");
              break;
            case "master_information":
              master_information_calculation(rows);
              break;
          }
        });
      };
      const fetch_results = async (queries) => {
        return Promise.all(
          queries.map(async (queryObj) => {
            const result = await pool.query(queryObj.query);
            return {
              name: queryObj.name,
              result: result.rows,
            };
          })
        );
      };
      const main = async () => {
        let formattedQueries = [];
        let purchesQueries = await SwpQueries.findOne(
          { endpoint: "SwpSummary" },
          { queriesArray: 1 }
        );
        let queriesArray = purchesQueries.queriesArray;
        queriesArray.map((ele) => {
          //   let formattedQuery = ele.query.replace(/schema/g, `${fund_schema}`);
          let formattedQuery = ele.query.replace(
            /transactionDate/g,
            `${transaction_date}`
          );
          formattedQuery = formattedQuery.replace(/schema/g, `${schema}`);
          formattedQuery = formattedQuery.replace(/'fund'/g, `'${fund}'`);

          formattedQueries.push({ name: ele.name, query: formattedQuery });
        });

        const all_results = await fetch_results(formattedQueries);
        await calculate_result(all_results);
        const cached_data = await CachedData.findOneAndUpdate(
          { date: transaction_date, type: "swp", fund: fund },
          {
            $set: {
              date: transaction_date,
              "dataObject.disbursement": dataObject.disbursement,
              "dataObject.qualitycheck_summary": dataObject.qualitycheck,
              "dataObject.radialchart": dataObject.radialchart,
              "dataObject.master_information": dataObject.master_information,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );
        return cached_data.dataObject;

        // res.status(200).send(cached_data.dataObject);
      };
      const result = await main();
      return result;
    }
  } catch (e) {
    console.log("Errro in swp summary provider", e);
    throw new Error(e);
  }
};
