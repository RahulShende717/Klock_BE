import SwitchQueries from "../../models/switch.model.js";
import CachedData from "../../models/cacheddata.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getPoolObj } from "../../utils/pool.js";
import { get_common_pendingtransactions } from "../../utils/getfilterpendingtransactions.js";
import { cache_transaction_journey_provider } from "../transactionlifecyle.provider.js";

export const pendingtrxn_rejectedreason_provider = async (
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
      "rejected_transactions",
      "deleted_transactions",
    ];

    const isDataPresent = await CachedData.findOne({
      $and: [
        { fund: fund, date: transaction_date, type: "switch" },
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
        rejectedTransactions: isDataPresent.dataObject.rejected_transactions,
        deletedTransactions: isDataPresent.dataObject.deleted_transactions,
      };
    } else {
      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          const inflow = rows.filter((item) => item.flow == "Inflow");
          const outflow = rows.filter((item) => item.flow == "Outflow");

          if (item.name == "pending_trxn_liquid") {
            dataObject["topPendingTransactionLiquid"] = {
              inflow: inflow,
              outflow: outflow,
            };
          } else if (item.name == "pending_trxn_combine") {
            dataObject["topPendingTransactionCombined"] = {
              inflow: inflow,
              outflow: outflow,
            };
          } else if (item.name == "pending_trxn_nonliquid") {
            dataObject["topPendingTransactionNonLiquid"] = {
              inflow: inflow,
              outflow: outflow,
            };
          } else if (item.name == "pending_reasons_combine") {
            dataObject["pendingReasonsCombine"] = {
              inflow: inflow,
              outflow: outflow,
            };
          } else if (item.name == "pending_reasons_liquid") {
            dataObject["pendingReasonsLiquid"] = {
              inflow: inflow,
              outflow: outflow,
            };
          } else if (item.name == "pending_reasons_nonliquid") {
            dataObject["pendingReasonsNonLiquid"] = {
              inflow: inflow,
              outflow: outflow,
            };
          } else if (item.name == "rejected_reasons") {
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

            rows.forEach((ele, ind) => {
              if (ele.flow == "Inflow") {
                obj.inflow.combine.push(ele);
              } else {
                obj.outflow.combine.push(ele);
              }
              if (ele.asset_class == "LIQUID") {
                if (ele.flow == "Inflow") {
                  obj.inflow.liquid.push(ele);
                } else {
                  obj.outflow.liquid.push(ele);
                }
              } else {
                if (ele.flow == "Inflow") {
                  obj.inflow.nonliquid.push(ele);
                } else {
                  obj.outflow.nonliquid.push(ele);
                }
              }
            });

            dataObject["rejected_transactions"] = obj;
          } else if (item.name == "pre-process_rejected_reasons") {
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

            rows.forEach((ele, ind) => {
              if (ele.flow == "Inflow") {
                obj.inflow.combine.push(ele);
              } else {
                obj.outflow.combine.push(ele);
              }
              if (ele.asset_class == "LIQUID") {
                if (ele.flow == "Inflow") {
                  obj.inflow.liquid.push(ele);
                } else {
                  obj.outflow.liquid.push(ele);
                }
              } else {
                if (ele.flow == "Inflow") {
                  obj.inflow.nonliquid.push(ele);
                } else {
                  obj.outflow.nonliquid.push(ele);
                }
              }
            });

            dataObject["deleted_transactions"] = obj;
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
        let purchesQueries = await SwitchQueries.findOne(
          { endpoint: "PendingandRejectedReasons" },
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

        const allcombined = [
          ...dataObject.topPendingTransactionCombined.inflow,
          ...dataObject.topPendingTransactionCombined.outflow,
        ];

        const allliquid = [
          ...dataObject.topPendingTransactionLiquid.inflow,
          ...dataObject.topPendingTransactionLiquid.outflow,
        ];

        const allnonliquid = [
          ...dataObject.topPendingTransactionNonLiquid.inflow,
          ...dataObject.topPendingTransactionNonLiquid.outflow,
        ];
        const common_ihnos_list = await get_common_pendingtransactions(
          allcombined,
          allnonliquid,
          allliquid
        );

        //pre-compute results for all ihno
        cache_transaction_journey_provider(
          // dataObject.topPendingTransactionLiquid,
          common_ihnos_list,
          fund,
          transaction_date,
          "switch"
        );

        const cached_data = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "switch" },
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
              "dataObject.rejected_transactions":
                dataObject.rejected_transactions,
              "dataObject.deleted_transactions":
                dataObject.deleted_transactions,
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
          rejectedTransactions: cached_data.dataObject.rejected_transactions,
          deletedTransactions: cached_data.dataObject.deleted_transactions,
        };
        return response_obj;
      };
      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log(
      "Error in pending and rejected transaction controller switch",
      e
    );
    throw new Error(e);
  }
};
