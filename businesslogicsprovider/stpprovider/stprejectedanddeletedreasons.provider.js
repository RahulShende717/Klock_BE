import CachedData from "../../models/cacheddata.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import STPQueries from "../../models/stpqueries.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolObj } from "../../utils/pool.js";

export const rejectedDeletedReasons = async (
  trdate,
  fundcode,
  snsstatus = false
) => {
  try {
    let dataObject = {};
    let transaction_date = trdate;
    let fund = fundcode;
    let pool = getPoolObj(fund);
    let schema = getFundWiseSchema(fund);
    const keysToCheck = [
      "deleted_transactions",
      "rejected_transactions",
      "funding_summary_transaction_date",
      "funding_summary_funding_date",
      "alerts",
    ];

    const isDataPresent = await CachedData.findOne({
      $and: [
        { fund: fund, date: transaction_date, type: "stp" },
        {
          $and: keysToCheck.map((key) => ({
            [`dataObject.${key}`]: { $exists: true },
          })),
        },
      ],
    });
    if (!snsstatus && isDataPresent != null) {

      return {
        deletedTransactions: isDataPresent.dataObject.deleted_transactions,
        rejectedTransactions: isDataPresent.dataObject.rejected_transactions,
      };
    } else {
      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          if (item.name == "deleted_transactions") {
            let obj = {
              inflow: {
                combine: [],
                liquid: [],
                nonliquid: [],
              },
              outflow: {
                combine: [],
                liquid: [],
                nonliquid: [],
              },
            };

            const deletedInflow = rows.filter((item) => item.flow == "Inflow");
            const deletedOutflow = rows.filter(
              (item) => item.flow == "Outflow"
            );

            deletedInflow.forEach((ele, ind) => {
              obj.inflow.combine.push(ele);
              if (ele.asset_class == "LIQUID") {
                obj.inflow.liquid.push(ele);
              } else if (ele.asset_class == "NON LIQUID") {
                obj.inflow.nonliquid.push(ele);
              }
            });
            deletedOutflow.forEach((ele, ind) => {
              obj.outflow.combine.push(ele);
              if (ele.asset_class == "LIQUID") {
                obj.outflow.liquid.push(ele);
              } else if (ele.asset_class == "NON LIQUID") {
                obj.outflow.nonliquid.push(ele);
              }
            });

            dataObject["deleted_transactions"] = obj;
          } else if (item.name == "rejected_transactions") {
            let obj = {
              inflow: {
                combine: [],
                liquid: [],
                nonliquid: [],
              },
              outflow: {
                combine: [],
                liquid: [],
                nonliquid: [],
              },
            };
            const rejectedInflow = rows.filter((item) => item.flow == "Inflow");
            const rejectedOutflow = rows.filter(
              (item) => item.flow == "Outflow"
            );

            rejectedInflow.forEach((ele, ind) => {
              obj.inflow.combine.push(ele);
              if (ele.asset_class == "LIQUID") {
                obj.inflow.liquid.push(ele);
              } else if (ele.asset_class == "NON LIQUID") {
                obj.inflow.nonliquid.push(ele);
              }
            });
            rejectedOutflow.forEach((ele, ind) => {
              obj.outflow.combine.push(ele);
              if (ele.asset_class == "LIQUID") {
                obj.outflow.liquid.push(ele);
              } else if (ele.asset_class == "NON LIQUID") {
                obj.outflow.nonliquid.push(ele);
              }
            });

            dataObject["rejected_transactions"] = obj;
          } else if (item.name === "funding_summary_stp_fundingdt") {
            let obj = {
              combine: {
                liquid: [],
                debt: [],
                equity: [],
              },
              liquid: {
                liquid: [],
                debt: [],
                equity: [],
              },
              nonliquid: {
                liquid: [],
                debt: [],
                equity: [],
              },
            };
            rows.forEach((item) => {
              if (item.fscheme === "LIQUID") obj.combine.liquid.push(item);
              if (item.fscheme === "DEBT") obj.combine.debt.push(item);
              if (item.fscheme === "EQUITY") obj.combine.equity.push(item);

              if (item.asset_class === "LIQUID") {
                if (item.fscheme === "LIQUID") obj.liquid.liquid.push(item);
                if (item.fscheme === "DEBT") obj.liquid.debt.push(item);
                if (item.fscheme === "EQUITY") obj.liquid.equity.push(item);
              } else if (item.asset_class === "NON LIQUID") {
                if (item.fscheme === "LIQUID") obj.nonliquid.liquid.push(item);
                if (item.fscheme === "DEBT") obj.nonliquid.debt.push(item);
                if (item.fscheme === "EQUITY") obj.nonliquid.equity.push(item);
              }
            });
            dataObject["funding_summary_funding_date"] = obj;
          } else if (item.name === "funding_summary_stp_trdt") {
            let obj = {
              combine: {
                liquid: [],
                debt: [],
                equity: [],
              },
              liquid: {
                liquid: [],
                debt: [],
                equity: [],
              },
              nonliquid: {
                liquid: [],
                debt: [],
                equity: [],
              },
            };
            rows.forEach((item) => {
              if (item.fscheme === "LIQUID") obj.combine.liquid.push(item);
              if (item.fscheme === "DEBT") obj.combine.debt.push(item);
              if (item.fscheme === "EQUITY") obj.combine.equity.push(item);

              if (item.asset_class === "LIQUID") {
                if (item.fscheme === "LIQUID") obj.liquid.liquid.push(item);
                if (item.fscheme === "DEBT") obj.liquid.debt.push(item);
                if (item.fscheme === "EQUITY") obj.liquid.equity.push(item);
              } else if (item.asset_class === "NON LIQUID") {
                if (item.fscheme === "LIQUID") obj.nonliquid.liquid.push(item);
                if (item.fscheme === "DEBT") obj.nonliquid.debt.push(item);
                if (item.fscheme === "EQUITY") obj.nonliquid.equity.push(item);
              }
            });

            dataObject["funding_summary_transaction_date"] = obj;
          } else if (item.name === "alerts") {
            let obj = {
              liquid: [],
              nonliquid: [],
              combine: [],
            };

            rows.forEach((ele, ind) => {
              obj.combine.push(ele);
              if (ele.asset_class == "LIQUID") {
                obj.liquid.push(ele);
              } else {
                obj.nonliquid.push(ele);
              }
            });

            dataObject.alerts = obj;
          }
        });
        return dataObject;
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
        let purchesQueries = await STPQueries.findOne(
          { endpoint: "RejectedDeletedReasons" },
          { queriesArray: 1 }
        );
        let queriesArray = purchesQueries.queriesArray;
        queriesArray.map((ele) => {
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
          { fund: fund, date: transaction_date, type: "stp" },
          {
            $set: {
              date: transaction_date,
              "dataObject.rejected_transactions":
                dataObject.rejected_transactions,
              "dataObject.deleted_transactions":
                dataObject.deleted_transactions,
              "dataObject.funding_summary_transaction_date":
                dataObject.funding_summary_transaction_date,
              "dataObject.funding_summary_funding_date":
                dataObject.funding_summary_funding_date,
              "dataObject.alerts": dataObject.alerts,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );

        let response_obj = {
          deletedTransactions: cached_data.dataObject.deleted_transactions,
          rejectedTransactions: cached_data.dataObject.rejected_transactions,
          funding_summary_transaction_date:
            cached_data.dataObject.funding_summary_transaction_date,
          funding_summary_funding_date:
            cached_data.dataObject.funding_summary_funding_date,
          alerts: cached_data.dataObject.alerts,
        };
        return response_obj;
      };
      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log("Error in rejecteddeletedandreasons provider ", e);
  }
};
