import PurchaseQueries from "../../models/purchasequeries.model.js";
import CachedData from "../../models/cacheddata.model.js";
import { getPoolObj } from "../../utils/pool.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";

export const refundTransactionsData = async (
  trdate,
  fundcode,
  snsstatus = false
) => {
  try {
    let dataObject = {};
    let transaction_date = trdate;
    let fund = fundcode;
    let schema = getFundWiseSchema(fund);
    const pool = getPoolObj(fund);
    if (pool == null) console.log("No Pool Connections For This AMC", fund);

    const keysToCheck = ["refund"];

    const isDataPresent = await CachedData.findOne({
      $and: [
        { fund: fund, date: transaction_date, type: "purchase" },
        {
          $and: keysToCheck.map((key) => ({
            [`dataObject.${key}`]: { $exists: true },
          })),
        },
      ],
    });
    if (!snsstatus && isDataPresent != null) {
      return {
        refund: isDataPresent.dataObject.refund,
      };
    } else {
      const refund_calculation = async (rows) => {
        const refund = {
          combine: {
            refund_done: {
              count: 0,
              amount: 0,
            },
            refund_pending: {
              count: 0,
              amount: 0,
            },
            refund_total: {
              count: 0,
              amount: 0,
            },
          },
          liquid: {
            refund_done: {
              count: 0,
              amount: 0,
            },
            refund_pending: {
              count: 0,
              amount: 0,
            },
            refund_total: {
              count: 0,
              amount: 0,
            },
          },
          nonLiquid: {
            refund_done: {
              count: 0,
              amount: 0,
            },
            refund_pending: {
              count: 0,
              amount: 0,
            },
            refund_total: {
              count: 0,
              amount: 0,
            },
          },
        };
        rows.map((row) => {
          if (row.completion_status === "REFUND DONE") {
            if (row.asset_class === "LIQUID") {
              refund["liquid"].refund_done.count += Number(row.total_count);
              refund["liquid"].refund_done.amount += Number(row.total_amount);
            } else {
              refund["nonLiquid"].refund_done.count += Number(row.total_count);
              refund["nonLiquid"].refund_done.amount += Number(
                row.total_amount
              );
            }
          } else if (row.completion_status === "REFUND PENDING") {
            if (row.asset_class === "LIQUID") {
              refund["liquid"].refund_pending.count += Number(row.total_count);
              refund["liquid"].refund_pending.amount += Number(
                row.total_amount
              );
            } else {
              refund["nonLiquid"].refund_pending.count += Number(
                row.total_count
              );
              refund["nonLiquid"].refund_pending.amount += Number(
                row.total_amount
              );
            }
          }
        });
        refund["combine"].refund_pending.count =
          refund["nonLiquid"].refund_pending.count +
          refund["liquid"].refund_pending.count;
        refund["combine"].refund_pending.amount =
          refund["nonLiquid"].refund_pending.amount +
          refund["liquid"].refund_pending.amount;

        refund["combine"].refund_done.count =
          refund["nonLiquid"].refund_done.count +
          refund["liquid"].refund_done.count;
        refund["combine"].refund_done.amount =
          refund["nonLiquid"].refund_done.amount +
          refund["liquid"].refund_done.amount;

        refund["combine"].refund_total.count =
          refund["combine"].refund_done.count +
          refund["combine"].refund_pending.count;
        refund["liquid"].refund_total.count =
          refund["liquid"].refund_done.count +
          refund["liquid"].refund_pending.count;
        refund["nonLiquid"].refund_total.count =
          refund["nonLiquid"].refund_done.count +
          refund["nonLiquid"].refund_pending.count;

        refund["combine"].refund_total.amount =
          refund["combine"].refund_done.amount +
          refund["combine"].refund_pending.amount;
        refund["liquid"].refund_total.amount =
          refund["liquid"].refund_done.amount +
          refund["liquid"].refund_pending.amount;
        refund["nonLiquid"].refund_total.amount =
          refund["nonLiquid"].refund_done.amount +
          refund["nonLiquid"].refund_pending.amount;

        dataObject["refund"] = refund;
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "Refund":
              refund_calculation(rows);
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
        let purchesQueries = await PurchaseQueries.findOne(
          { endpoint: "PurchaseBifurcation" },
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
          { fund: fund, date: transaction_date, type: "purchase" },
          {
            $set: {
              date: transaction_date,
              "dataObject.refund": dataObject.refund,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );

        return {
          refund: cached_data.dataObject.refund,
        };
      };

      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log("Error in refund provider purchase", e);
    throw new Error(e);
  }
};
