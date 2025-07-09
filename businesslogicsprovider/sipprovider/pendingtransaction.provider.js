import CachedData from "../../models/cacheddata.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import SIPQueries from "../../models/sipqueries.model.js";
import { getPoolObj } from "../../utils/pool.js";
import { get_common_pendingtransactions } from "../../utils/getfilterpendingtransactions.js";
import { cache_transaction_journey_provider } from "../transactionlifecyle.provider.js";

export const sip_pendingtransactionsData = async (
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
      "topPendingTransactionCombined",
      "topPendingTransactionLiquid",
      "topPendingTransactionNonLiquid",
      "rejected_transactions",
      "pending_reasons_nonLiquid",
      "pending_reasons_liquid",
      "pending_reasons_combine",
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
          isDataPresent.dataObject.topPendingTransactionNonLiquid,
        topPendingTransactionLiquid:
          isDataPresent.dataObject.topPendingTransactionLiquid,
        topPendingTransactionCombined:
          isDataPresent.dataObject.topPendingTransactionCombined,
        pendingReasonsLiquid: isDataPresent.dataObject.pending_reasons_liquid,
        pendingReasonsCombine: isDataPresent.dataObject.pending_reasons_combine,
        pendingReasonsNonLiquid:
          isDataPresent.dataObject.pending_reasons_nonLiquid,
        deletedTransactions: isDataPresent.dataObject.deleted_transactions,
        rejectedTransactions: isDataPresent.dataObject.rejected_transactions,
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
          } else if (item.name == "pending_reasons_combine") {
            dataObject["pending_reasons_combine"] = rows;
          } else if (item.name == "pending_reasons_liquid") {
            dataObject["pending_reasons_liquid"] = rows;
          } else if (item.name == "pending_reasons_nonLiquid") {
            dataObject["pending_reasons_nonLiquid"] = rows;
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
          }
        });
        return dataObject;
      };

      const fetch_results = async (queries) => {
        return Promise.all(
          queries.map(async (queryObj) => {
            const startTime = Date.now();
            const result = await pool.query(queryObj.query);
            const endTime = Date.now();
            // const result = await pool.query(queryObj.query);
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
          { endpoint: "PendingTransactions" },
          { queriesArray: 1 }
        );
        let queriesArray = purchesQueries.queriesArray;
        queriesArray.map((ele) => {
          let formattedQuery;
          formattedQuery = ele.query.replace(
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
          "sip_transaction"
        );

        const cached_data = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "sip" },
          {
            $set: {
              date: transaction_date,
              "dataObject.topPendingTransactionNonLiquid":
                dataObject.topPendingTransactionNonLiquid,
              "dataObject.topPendingTransactionLiquid":
                dataObject.topPendingTransactionLiquid,
              "dataObject.topPendingTransactionCombined":
                dataObject.topPendingTransactionCombined,
              "dataObject.rejected_transactions":
                dataObject.rejected_transactions,
              "dataObject.pending_reasons_nonLiquid":
                dataObject.pending_reasons_nonLiquid,
              "dataObject.pending_reasons_liquid":
                dataObject.pending_reasons_liquid,
              "dataObject.pending_reasons_combine":
                dataObject.pending_reasons_combine,
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
          pendingReasonsLiquid: cached_data.dataObject.pending_reasons_liquid,
          pendingReasonsCombine: cached_data.dataObject.pending_reasons_combine,
          pendingReasonsNonLiquid:
            cached_data.dataObject.pending_reasons_nonLiquid,
          deletedTransactions: cached_data.dataObject.deleted_transactions,
          rejectedTransactions: cached_data.dataObject.rejected_transactions,
        };
        return response_obj;
      };
      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log("Error in pending transaction  provider sip", e);
    throw new Error(e);
  }
};
