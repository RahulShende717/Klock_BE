import RedemptionQueries from "../../models/redemptionqueries.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
// import CachedData from "../../models/newoverview.model.js";
import CachedData from "../../models/cacheddata.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolObj } from "../../utils/pool.js";
import moment from "moment";

export const transaction_history_provider = async (
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
    const from_date = moment(transaction_date)
      .subtract(5, "days")
      .format("YYYY-MM-DD");
    const keysToCheck = [
      "trhistory",
      "tillDatePendingTATObject",
      "specificDatePendingTATObject",
      "rejectOrdeleteTrnasObject",
      // "most_redemmedscheme_count",
      // "most_redemmed_scheme_amount",
      "average_transaction_perminutes",
      "top_performing_scheme_calculation",
      "top_performing_scheme_trxn",
      "changeOfTotalTransaction",
      //   "top_performing_scheme_calculation",
      //   "top_performing_scheme_trxn",
    ];
    const isDataPresent = await CachedData.findOne({
      $and: [
        { date: transaction_date, type: "redemption", fund: fund },
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
      const total_transaction_count_Calculation = (rows) => {
        let object = {
          combined: {
            total_transaction_count: 0,
            total_transaction_amount: 0,
            pending_transaction_count: 0,
            within_t2_transaction: 0,
            beyond_t2_transaction: 0,
            total_completed_transaction_count: 0,
            transaction_status: "",
          },
          liquid: {
            total_transaction_count: 0,
            total_transaction_amount: 0,
            pending_transaction_count: 0,
            within_t2_transaction: 0,
            beyond_t2_transaction: 0,
            total_completed_transaction_count: 0,
            transaction_status: "",
          },
          nonliquid: {
            total_transaction_count: 0,
            total_transaction_amount: 0,
            pending_transaction_count: 0,
            within_t2_transaction: 0,
            beyond_t2_transaction: 0,
            total_completed_transaction_count: 0,
            transaction_status: "",
          },
        };

        let rejectOrdeleteTrnasObject = {
          combine: {
            rejected_transaction_Before_endorsement: {
              count: 0,
              amount: 0,
            },
            rejected_transaction_After_endorsement: {
              count: 0,
              amount: 0,
            },
          },
          liquid: {
            rejected_transaction_Before_endorsement: {
              count: 0,
              amount: 0,
            },
            rejected_transaction_After_endorsement: {
              count: 0,
              amount: 0,
            },
          },
          non_liquid: {
            rejected_transaction_Before_endorsement: {
              count: 0,
              amount: 0,
            },
            rejected_transaction_After_endorsement: {
              count: 0,
              amount: 0,
            },
          },
        };

        rows.forEach((ele, ind) => {
          if (ele.asset_class == "LIQUID") {
            if (ele.trdt == transaction_date) {
              object.liquid.total_transaction_count += Number(ele.count);
              object.liquid.total_transaction_amount += Number(ele.amount);
              if (ele.status == "pending_txn_count")
                object.liquid.pending_transaction_count += Number(ele.count);
              if (ele.status == "deleted_txn_count") {
                rejectOrdeleteTrnasObject.liquid.rejected_transaction_Before_endorsement.count +=
                  Number(ele.count);
                rejectOrdeleteTrnasObject.liquid.rejected_transaction_Before_endorsement.amount +=
                  Number(ele.amount);
              }
              if (ele.status == "rejected_txn_count") {
                rejectOrdeleteTrnasObject.liquid.rejected_transaction_After_endorsement.count +=
                  Number(ele.count);
                rejectOrdeleteTrnasObject.liquid.rejected_transaction_After_endorsement.amount +=
                  Number(ele.amount);
              }
            }
          } else {
            if (ele.trdt == transaction_date) {
              object.nonliquid.total_transaction_count += Number(ele.count);
              object.nonliquid.total_transaction_amount += Number(ele.amount);

              if (ele.status == "pending_txn_count")
                object.nonliquid.pending_transaction_count += Number(ele.count);
              if (ele.status == "deleted_txn_count") {
                rejectOrdeleteTrnasObject.non_liquid.rejected_transaction_Before_endorsement.count +=
                  Number(ele.count);
                rejectOrdeleteTrnasObject.non_liquid.rejected_transaction_Before_endorsement.amount +=
                  Number(ele.amount);
              }
              if (ele.status == "rejected_txn_count") {
                rejectOrdeleteTrnasObject.non_liquid.rejected_transaction_After_endorsement.count +=
                  Number(ele.count);
                rejectOrdeleteTrnasObject.non_liquid.rejected_transaction_After_endorsement.amount +=
                  Number(ele.amount);
              }
            }
          }
        });

        object.combined.pending_transaction_count =
          object.liquid.pending_transaction_count +
          object.nonliquid.pending_transaction_count;
        object.combined.total_transaction_amount =
          object.liquid.total_transaction_amount +
          object.nonliquid.total_transaction_amount;
        object.combined.total_transaction_count =
          object.liquid.total_transaction_count +
          object.nonliquid.total_transaction_count;
        object.liquid.total_completed_transaction_count =
          object.liquid.total_transaction_count -
          object.liquid.pending_transaction_count;
        object.nonliquid.total_completed_transaction_count =
          object.nonliquid.total_transaction_count -
          object.nonliquid.pending_transaction_count;
        object.combined.total_completed_transaction_count =
          object.liquid.total_completed_transaction_count +
          object.nonliquid.total_completed_transaction_count -
          (object.liquid.pending_transaction_count +
            object.nonliquid.pending_transaction_count);
        if (object.liquid.pending_transaction_count > 0)
          object.liquid.transaction_status = "Pending";
        else object.liquid.transaction_status = "Completed";
        if (object.nonliquid.pending_transaction_count > 0)
          object.nonliquid.transaction_status = "Pending";
        else object.nonliquid.transaction_status = "Completed";
        if (object.combined.pending_transaction_count > 0)
          object.combined.transaction_status = "Pending";
        else object.combined.transaction_status = "Completed";

        rejectOrdeleteTrnasObject.combine.rejected_transaction_After_endorsement.count =
          rejectOrdeleteTrnasObject.liquid
            .rejected_transaction_After_endorsement.count +
          rejectOrdeleteTrnasObject.non_liquid
            .rejected_transaction_After_endorsement.count;
        rejectOrdeleteTrnasObject.combine.rejected_transaction_After_endorsement.amount =
          rejectOrdeleteTrnasObject.liquid
            .rejected_transaction_After_endorsement.amount +
          rejectOrdeleteTrnasObject.non_liquid
            .rejected_transaction_After_endorsement.amount;

        rejectOrdeleteTrnasObject.combine.rejected_transaction_Before_endorsement.count =
          rejectOrdeleteTrnasObject.liquid
            .rejected_transaction_Before_endorsement.count +
          rejectOrdeleteTrnasObject.non_liquid
            .rejected_transaction_Before_endorsement.count;
        rejectOrdeleteTrnasObject.combine.rejected_transaction_Before_endorsement.amount =
          rejectOrdeleteTrnasObject.liquid
            .rejected_transaction_Before_endorsement.amount +
          rejectOrdeleteTrnasObject.non_liquid
            .rejected_transaction_Before_endorsement.amount;

        dataObject.trhistory = object;
        dataObject.rejectOrdeleteTrnasObject = rejectOrdeleteTrnasObject;
      };
      const TAT_Pending_Count_Calculation = (rows) => {
        let tillDateObject = {
          combine: {
            pending_count: 0,
            within: 0,
            beyond: 0,
            total_pending_count: 0,
          },
          liquid: {
            pending_count: 0,
            within: 0,
            beyond: 0,
            total_pending_count: 0,
          },
          nonliquid: {
            pending_count: 0,
            within: 0,
            beyond: 0,
            total_pending_count: 0,
          },
        };

        let specificDateObject = {
          combine: {
            pending_count: 0,
            within: 0,
            beyond: 0,
            total_pending_count: 0,
          },
          liquid: {
            pending_count: 0,
            within: 0,
            beyond: 0,
            total_pending_count: 0,
          },
          nonliquid: {
            pending_count: 0,
            within: 0,
            beyond: 0,
            total_pending_count: 0,
          },
        };

        rows.forEach((ele, ind) => {
          if (ele.trdt <= transaction_date) {
            const assetType =
              ele.asset_class === "LIQUID" ? "liquid" : "nonliquid";
            const transactionType =
              Number(ele.date_part) <= 2 ? "within" : "beyond";
            tillDateObject[assetType][transactionType] += Number(ele.txn_count);
            tillDateObject[assetType]["total_pending_count"] += Number(
              ele.txn_count
            );
          }
          if (ele.trdt == transaction_date) {
            const assetType =
              ele.asset_class === "LIQUID" ? "liquid" : "nonliquid";
            const transactionType =
              Number(ele.date_part) <= 2 ? "within" : "beyond";

            specificDateObject[assetType][transactionType] += Number(
              ele.txn_count
            );
            specificDateObject[assetType]["total_pending_count"] += Number(
              ele.txn_count
            );
          }
        });

        tillDateObject["combine"]["within"] =
          tillDateObject["liquid"]["within"] +
          tillDateObject["nonliquid"]["within"];
        specificDateObject["combine"]["within"] =
          specificDateObject["liquid"]["within"] +
          specificDateObject["nonliquid"]["within"];
        tillDateObject["combine"]["beyond"] =
          tillDateObject["liquid"]["beyond"] +
          tillDateObject["nonliquid"]["beyond"];
        specificDateObject["combine"]["beyond"] =
          specificDateObject["liquid"]["beyond"] +
          specificDateObject["nonliquid"]["beyond"];
        tillDateObject["combine"]["total_pending_count"] =
          tillDateObject["liquid"]["total_pending_count"] +
          tillDateObject["nonliquid"]["total_pending_count"];
        specificDateObject["combine"]["total_pending_count"] =
          specificDateObject["liquid"]["total_pending_count"] +
          specificDateObject["nonliquid"]["total_pending_count"];

        dataObject.tillDatePendingTATObject = tillDateObject;
        dataObject.specificDatePendingTATObject = specificDateObject;
      };

      const most_redemmed_scheme_count = (rows) => {
        dataObject.top_performing_scheme_trxn = rows;
      };

      const most_redemmed_scheme_amount = (rows) => {
        dataObject.top_performing_scheme_calculation = rows;
      };

      const change_of_total_transaction_count = async (rows) => {
        let object = {
          combined: {
            total_transaction_count: 0,
            total_transaction_amount: 0,
            pending_transaction_count: 0,
            previous_total_transaction_count: 0,
            total_completed_transaction_count: 0,
            transaction_status: "",
            percentage_difference: 0,
          },
          liquid: {
            total_transaction_count: 0,
            total_transaction_amount: 0,
            pending_transaction_count: 0,
            previous_total_transaction_count: 0,
            total_completed_transaction_count: 0,
            transaction_status: "",
            percentage_difference: 0,
          },
          nonliquid: {
            total_transaction_count: 0,
            total_transaction_amount: 0,
            pending_transaction_count: 0,
            previous_total_transaction_count: 0,
            total_completed_transaction_count: 0,
            transaction_status: "",
            percentage_difference: 0,
          },
        };

        let previous_date;
        for (let i = 0; i < rows.length; i++) {
          if (rows[i].trdt != transaction_date) {
            previous_date = rows[i].trdt;
            break;
          }
        }

        rows.forEach((ele, ind) => {
          if (ele.asset_class == "LIQUID") {
            if (ele.trdt == transaction_date) {
              object.liquid.total_transaction_count += Number(ele.count);
              object.liquid.total_transaction_amount += Number(ele.amount);
            } else if (ele.trdt == previous_date) {
              object.liquid.previous_total_transaction_count += Number(
                ele.count
              );
            }
          } else {
            if (ele.trdt == transaction_date) {
              object.nonliquid.total_transaction_count += Number(ele.count);
              object.nonliquid.total_transaction_amount += Number(ele.amount);
            } else if (ele.trdt == previous_date) {
              object.nonliquid.previous_total_transaction_count += Number(
                ele.count
              );
            }
          }
        });

        object.combined.previous_total_transaction_count =
          object.liquid.previous_total_transaction_count +
          object.nonliquid.previous_total_transaction_count;
        object.combined.pending_transaction_count =
          object.liquid.pending_transaction_count +
          object.nonliquid.pending_transaction_count;
        object.combined.total_transaction_amount =
          object.liquid.total_transaction_amount +
          object.nonliquid.total_transaction_amount;
        object.combined.total_transaction_count =
          object.liquid.total_transaction_count +
          object.nonliquid.total_transaction_count;
        object.liquid.total_completed_transaction_count =
          object.liquid.total_transaction_count -
          object.liquid.pending_transaction_count;
        object.nonliquid.total_completed_transaction_count =
          object.nonliquid.total_transaction_count -
          object.nonliquid.pending_transaction_count;
        object.combined.total_completed_transaction_count =
          object.liquid.total_completed_transaction_count +
          object.nonliquid.total_completed_transaction_count -
          (object.liquid.pending_transaction_count +
            object.nonliquid.pending_transaction_count);

        let liquid_percentage =
          ((object.liquid.total_transaction_count -
            object.liquid.previous_total_transaction_count) /
            object.liquid.previous_total_transaction_count) *
          100;
        let nonliquid_percentage =
          ((object.nonliquid.total_transaction_count -
            object.nonliquid.previous_total_transaction_count) /
            object.nonliquid.previous_total_transaction_count) *
          100;
        let combined_percentage =
          ((object.combined.total_transaction_count -
            object.combined.previous_total_transaction_count) /
            object.combined.previous_total_transaction_count) *
          100;
        object.liquid.percentage_difference = liquid_percentage;
        object.nonliquid.percentage_difference = nonliquid_percentage;
        object.combined.percentage_difference = combined_percentage;

        dataObject.changeOfTotalTransaction = object;
      };

      const average_transaction_perminute = (rows) => {
        dataObject.average_transaction_perminutes = rows;
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "change_of_total_transaction_count":
              change_of_total_transaction_count(rows);
              break;
            case "transaction_history":
              total_transaction_count_Calculation(rows);
              break;
            case "pending_transaction_tat_status":
              TAT_Pending_Count_Calculation(rows);
              break;
            case "most_redemmed_scheme_amount":
              most_redemmed_scheme_amount(rows);
              break;
            case "most_redemmed_scheme_count":
              most_redemmed_scheme_count(rows);
              break;
            case "average_transaction_minute":
              average_transaction_perminute(rows);
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
        let purchesQueries = await RedemptionQueries.findOne(
          { endpoint: "RedemptionTransactiosData" },
          { queriesArray: 1 }
        );
        let queriesArray = purchesQueries.queriesArray;
        queriesArray.map((ele) => {
          let formattedQuery;
          if (ele.name == "change_of_total_transaction_count") {
            formattedQuery = ele.query.replace(/from_date/g, `${from_date}`);
            formattedQuery = formattedQuery.replace(
              /transactionDate/g,
              `${transaction_date}`
            );
          } else {
            formattedQuery = ele.query.replace(
              /transactionDate/g,
              `${transaction_date}`
            );
          }

          formattedQuery = formattedQuery.replace(/schema/g, `${schema}`);
          formattedQuery = formattedQuery.replace(/'fund'/g, `'${fund}'`);
          formattedQuery = formattedQuery.replace(/schema/g, `${schema}`);
          formattedQueries.push({ name: ele.name, query: formattedQuery });
        });
        const all_results = await fetch_results(formattedQueries);
        await calculate_result(all_results);
        const cached_data = await CachedData.findOneAndUpdate(
          { date: transaction_date, type: "redemption", fund: fund },
          {
            $set: {
              date: transaction_date,
              "dataObject.trhistory": dataObject.trhistory,
              "dataObject.tillDatePendingTATObject":
                dataObject.tillDatePendingTATObject,
              "dataObject.specificDatePendingTATObject":
                dataObject.specificDatePendingTATObject,
              "dataObject.rejectOrdeleteTrnasObject":
                dataObject.rejectOrdeleteTrnasObject,
              "dataObject.top_performing_scheme_trxn":
                dataObject.top_performing_scheme_trxn,
              "dataObject.top_performing_scheme_calculation":
                dataObject.top_performing_scheme_calculation,
              "dataObject.average_transaction_perminutes":
                dataObject.average_transaction_perminutes,
              "dataObject.changeOfTotalTransaction":
                dataObject.changeOfTotalTransaction,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );
        return cached_data.dataObject;
      };
      const result = await main();
      return result;
    }
  } catch (e) {
    console.log("Error in transaction provider redemption", e);
    throw new Error(e);
  }
};
