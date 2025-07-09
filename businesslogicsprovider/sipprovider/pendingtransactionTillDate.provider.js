import CachedData from "../../models/cacheddata.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import SIPQueries from "../../models/sipqueries.model.js";
import { getPoolObj } from "../../utils/pool.js";

export const pendingtransactionsDataTillDate = async (
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
    const keysToCheck = [
      "topPendingTransactionTillDateCombined",
      "topPendingTransactionTillDateLiquid",
      "topPendingTransactionTillDateNonLiquid",
    ];

    const isDataPresent = await CachedData.findOne({
      $and: [
        { fund: fund, date: transaction_date, type: "sip" },
        {
          $or: keysToCheck.map((key) => ({
            [`dataObject.${key}`]: { $exists: true },
          })),
        },
      ],
    });
    if (!snsstatus && isDataPresent != null) {
      return {
        topPendingTransactionNonLiquid:
          isDataPresent.dataObject.pending_transactions_nonliquid_tillDate,
        topPendingTransactionLiquid:
          isDataPresent.dataObject.pending_transactions_liquid_tillDate,
        topPendingTransactionCombined:
          isDataPresent.dataObject.pending_transactions_combine_tillDate,
      };
    } else {
      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          if (item.name == "pending_transactions_liquid_tillDate") {
            dataObject["topPendingTransactionTillDateLiquid"] = rows;
          } else if (item.name == "pending_transactions_combine_tillDate") {
            dataObject["topPendingTransactionTillDateCombined"] = rows;
          } else if (item.name == "pending_transactions_nonliquid_tillDate") {
            dataObject["topPendingTransactionTillDateNonLiquid"] = rows;
          }
        });
        return dataObject;
      };

      const fetch_results = async (queries) => {
        return Promise.all(
          queries.map(async (queryObj) => {
            // const result = await pool.query(queryObj.query);
            const startTime = Date.now()
              const result = await pool.query(queryObj.query);
              const endTime = Date.now()
            return {
              name: queryObj.name,
              result: result.rows,
            };
          })
        );
      };

      const main = async () => {
        let formattedQueries = [];
        let purchesQueries = await SIPQueries.findOne(
          { endpoint: "pending_transactions_tillDate" },
          { queriesArray: 1 }
        );
        let queriesArray = purchesQueries.queriesArray;
        queriesArray.map((ele) => {
          let formattedQuery = ele.query.replace(
            /'transactionDate'/g,
            `'${transaction_date}'`
          );
          formattedQuery = formattedQuery.replace(/schema/g, `${schema}`);
          formattedQuery = formattedQuery.replace(/'fund'/g, `'${fund}'`);
          formattedQueries.push({ name: ele.name, query: formattedQuery });
        });
        const all_results = await fetch_results(formattedQueries);
        await calculate_result(all_results);
        const cached_data = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "sip" },
          {
            $set: {
              date: transaction_date,
              "dataObject.topPendingTransactionTillDateNonLiquid":
                dataObject.topPendingTransactionTillDateNonLiquid,
              "dataObject.topPendingTransactionTillDateLiquid":
                dataObject.topPendingTransactionTillDateLiquid,
              "dataObject.topPendingTransactionTillDateCombined":
                dataObject.topPendingTransactionTillDateCombined,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );

        let response_obj = {
          topPendingTransactionTillDateNonLiquid:
            cached_data.dataObject.topPendingTransactionTillDateNonLiquid,
          topPendingTransactionTillDateLiquid:
            cached_data.dataObject.topPendingTransactionTillDateLiquid,
          topPendingTransactionTillDateCombined:
            cached_data.dataObject.topPendingTransactionTillDateCombined,
        };
        return response_obj;
      };
      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log("Error pending transaction provider till date sip", e);
    throw new Error(e);
  }
};
