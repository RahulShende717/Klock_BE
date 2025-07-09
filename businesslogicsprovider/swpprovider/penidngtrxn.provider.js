import CachedData from "../../models/cacheddata.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import SwpQueries from "../../models/swpqueries.model.js";
import { getPoolObj } from "../../utils/pool.js";

export const pending_txn_swp = async (trdate, fundcode, snsstatus = false) => {
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
      "deletedTransactions",
      "rejectedTransactions",
      "swpalerts",
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
        deletedTransactions: isDataPresent.dataObject.deleted_transactions,
        rejectedTransactions: isDataPresent.dataObject.rejected_transactions,
        swpalerts: isDataPresent.dataObject.swpalerts,
      };
    } else {
      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          if (item.name == "pending_trxn_liquid") {
            dataObject["topPendingTransactionLiquid"] = rows;
          } else if (item.name == "pending_trxn_combine") {
            dataObject["topPendingTransactionCombined"] = rows;
          } else if (item.name == "pending_trxn_nonliquid") {
            dataObject["topPendingTransactionNonLiquid"] = rows;
          } else if (item.name == "pending_reasons_combine") {
            dataObject["pendingReasonsCombine"] = rows;
          } else if (item.name == "pending_reasons_liquid") {
            dataObject["pendingReasonsLiquid"] = rows;
          } else if (item.name == "pending_reasons_nonliquid") {
            dataObject["pendingReasonsNonLiquid"] = rows;
          } else if (item.name == "deleted_transactions") {
            let obj = {
              combine: [],
              liquid: [],
              nonliquid: [],
            };

            rows.forEach((ele, ind) => {
              obj.combine.push(ele);
              if (ele.asset_class == "LIQUID") {
                obj.liquid.push(ele);
              } else if (ele.asset_class == "NON LIQUID") {
                obj.nonliquid.push(ele);
              }
            });

            dataObject["deleted_transactions"] = obj;
          } else if (item.name == "rejected_transactions") {
            let obj = {
              combine: [],
              liquid: [],
              nonliquid: [],
            };

            rows.forEach((ele, ind) => {
              obj.combine.push(ele);
              if (ele.asset_class == "LIQUID") {
                obj.liquid.push(ele);
              } else if (ele.asset_class == "NON LIQUID") {
                obj.nonliquid.push(ele);
              }
            });

            dataObject["rejected_transactions"] = obj;
          } else if (item.name == "swp_alerts") {
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

            dataObject.swpalerts = obj;
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
        let purchesQueries = await SwpQueries.findOne(
          { endpoint: "SwitchPendigTrxnsReasons" },
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
          { fund: fund, date: transaction_date, type: "swp" },
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
              "dataObject.rejectedTransactions":
                dataObject.rejected_transactions,
              "dataObject.deletedTransactions": dataObject.deleted_transactions,
              "dataObject.swpalerts": dataObject.swpalerts,
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
    console.log("Error in pending trasnaction swp", e);
    throw new Error(e);
  }
};
