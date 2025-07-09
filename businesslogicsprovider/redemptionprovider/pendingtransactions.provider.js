import PurchaseQueries from "../../models/purchasequeries.model.js";
import CachedData from "../../models/cacheddata.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import RedemptionQueries from "../../models/redemptionqueries.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { get_common_pendingtransactions } from "../../utils/getfilterpendingtransactions.js";
import { cache_transaction_journey_provider } from "../transactionlifecyle.provider.js";
import { getPoolObj } from "../../utils/pool.js";

export const pendingtrxnandreasons = async (
  trDate,
  fundcode,
  snsstatus = false
) => {
  try {
    let dataObject = {};
    let transaction_date = trDate;
    let fund = fundcode;
    const pool = getPoolObj(fund);
    if (pool == null) console.log("No Pool Connections For This AMC", fund);
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
        { fund: fund, date: transaction_date, type: "redemption" },
        {
          $and: keysToCheck.map((key) => ({
            [`dataObject.${key}`]: { $exists: true },
          })),
        },
      ],
    });
    if (!snsstatus && isDataPresent != null) {
      return {
        success: true,
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
          if (item.name == "pending_transactions_liquid") {
            dataObject["topPendingTransactionLiquid"] = rows;
          } else if (item.name == "pending_transactions_combine") {
            dataObject["topPendingTransactionCombined"] = rows;
          } else if (item.name == "pending_transactions_nonliquid") {
            dataObject["topPendingTransactionNonLiquid"] = rows;
          } else if (item.name == "pending_reason_combine") {
            dataObject["pendingReasonsCombine"] = rows;
          } else if (item.name == "pending_reason_liquid") {
            dataObject["pendingReasonsLiquid"] = rows;
          } else if (item.name == "pending_reason_nonliquid") {
            dataObject["pendingReasonsNonLiquid"] = rows;
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
        let purchesQueries = await RedemptionQueries.findOne(
          { endpoint: "PendingTransactionsAndReasons" },
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

        const common_ihnos_list = await get_common_pendingtransactions(
          dataObject.topPendingTransactionCombined,
          dataObject.topPendingTransactionNonLiquid,
          dataObject.topPendingTransactionLiquid
        );

        //pre-compute results for all ihno
        cache_transaction_journey_provider(
          // dataObject.topPendingTransactionLiquid,
          common_ihnos_list,
          fund,
          transaction_date,
          "redemption"
        );
        const cached_data = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "redemption" },
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

        return {
          success: true,
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
      };
      const result = await main();
      return result;
    }
  } catch (e) {
    console.log("Error in pending transaction controller redemption", e);
    throw new Error(e);
  }
};
