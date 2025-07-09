import STPQueries from "../../models/stpqueries.model.js";
import CachedData from "../../models/cacheddata.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolObj } from "../../utils/pool.js";

export const pending_trxn_provider_stp = async (
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
      "topPendingTransactionCombined",
      "topPendingTransactionLiquid",
      "topPendingTransactionNonLiquid",
      "pendingReasonsCombine",
      "pendingReasonsNonLiquid",
      "pendingReasonsLiquid",
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
        topPendingTransactionNonLiquid:
          isDataPresent.dataObject.topPendingTransactionNonLiquid,
        topPendingTransactionLiquid:
          isDataPresent.dataObject.topPendingTransactionLiquid,
        topPendingTransactionCombined:
          isDataPresent.dataObject.topPendingTransactionCombined,
        pendingReasonsLiquid: isDataPresent.dataObject.pendingReasonsLiquid,
        pendingReasonsCombine: isDataPresent.dataObject.pendingReasonsCombine,
        pendingReasonsNonLiquid:
          isDataPresent.dataObject.pendingReasonsNonLiquid,
      };
    } else {
      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          const inflow = rows.filter((item) => item.flow == "Inflow");
          const outflow = rows.filter((item) => item.flow == "Outflow");
          if (item.name == "topPendingLiquid") {
            dataObject["topPendingTransactionLiquid"] = {
              inflow: inflow,
              outflow: outflow,
            };
          } else if (item.name == "topPendingCombine") {
            dataObject["topPendingTransactionCombined"] = {
              inflow: inflow,
              outflow: outflow,
            };
          } else if (item.name == "topPendingNonLiquid") {
            dataObject["topPendingTransactionNonLiquid"] = {
              inflow: inflow,
              outflow: outflow,
            };
          } else if (item.name == "topPendingReasonsCombine") {
            dataObject["pendingReasonsCombine"] = {
              inflow: inflow,
              outflow: outflow,
            };
          } else if (item.name == "topPendingReasonsLiquid") {
            dataObject["pendingReasonsLiquid"] = {
              inflow: inflow,
              outflow: outflow,
            };
          } else if (item.name == "topPendingReasonsNonLiquid") {
            dataObject["pendingReasonsNonLiquid"] = {
              inflow: inflow,
              outflow: outflow,
            };
          }
        });
        return dataObject;
      };

      const fetch_results = async (queries) => {
        return Promise.all(
          queries.map(async (queryObj, index) => {
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
          { endpoint: "PendingTransactions" },
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
              "dataObject.topPendingTransactionNonLiquid":
                dataObject.topPendingTransactionNonLiquid,
              "dataObject.topPendingTransactionLiquid":
                dataObject.topPendingTransactionLiquid,
              "dataObject.topPendingTransactionCombined":
                dataObject.topPendingTransactionCombined,
              "dataObject.pendingReasonsLiquid":
                dataObject.pendingReasonsLiquid,
              "dataObject.pendingReasonsCombine":
                dataObject.pendingReasonsCombine,
              "dataObject.pendingReasonsNonLiquid":
                dataObject.pendingReasonsNonLiquid,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );

        let response_obj = {
          topPendingTransactionNonLiquid:
            cached_data.dataObject.topPendingTransactionNonLiquid,
          topPendingTransactionLiquid:
            cached_data.dataObject.topPendingTransactionLiquid,
          topPendingTransactionCombined:
            cached_data.dataObject.topPendingTransactionCombined,
          pendingReasonsLiquid: cached_data.dataObject.pendingReasonsLiquid,
          pendingReasonsCombine: cached_data.dataObject.pendingReasonsCombine,
          pendingReasonsNonLiquid:
            cached_data.dataObject.pendingReasonsNonLiquid,
        };
        return response_obj;
      };
      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log("Error in pending transaction controller stp", e);
    throw new Error(e);
  }
};
