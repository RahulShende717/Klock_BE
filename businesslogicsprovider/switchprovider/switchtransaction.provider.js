import CachedData from "../../models/cacheddata.model.js";
import SwitchQueries from "../../models/switch.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getPoolObj } from "../../utils/pool.js";
import moment from "moment";

const switch_transction_provider = async (
  trdate,
  fundcode,
  snsstatus = false
) => {
  try {
    let dataObject = {};
    let transaction_date = trdate;
    let fund = fundcode;
    let schema = getFundWiseSchema(fund);
    const from_date = moment(transaction_date)
      .subtract(5, "days")
      .format("YYYY-MM-DD");
    const pool = getPoolObj(fund);
    const keysToCheck = [
      "average_transaction_perminutes",
      "trhistory",
      "tillDatePendingTATObject",
      "specificDatePendingTATObject",
      "rejectTrnasObject",
      "deleteTrnasObject",
      "top_performing_scheme_calculation",
      "top_performing_scheme_trxn",
      "changeOfTotalTransaction"
    ];
    const isDataPresent = await CachedData.findOne({
      $and: [
        { date: transaction_date, type: "switch", fund: fund },
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
      const average_transaction_Calculation = async (rows) => {
        let avg_obj = {
          inflow: rows.filter((item) => item.flow == "Inflow"),
          outflow: rows.filter((item) => item.flow == "Outflow"),
        };
        dataObject.average_transaction_perminutes = avg_obj;
      };
      const total_transaction_count_Calculation = (rows) => {
        let object = {
          inflow: {
            combined: {
              total_transaction_count: 0,
              total_transaction_amount: 0,
              pending_transaction_count: 0,
              previous_total_transaction_count: 0,
              within_t2_transaction: 0,
              beyond_t2_transaction: 0,
              total_completed_transaction_count: 0,
              transaction_status: "",
              percentage_difference: 0,
            },
            liquid: {
              total_transaction_count: 0,
              total_transaction_amount: 0,
              pending_transaction_count: 0,
              previous_total_transaction_count: 0,
              within_t2_transaction: 0,
              beyond_t2_transaction: 0,
              total_completed_transaction_count: 0,
              transaction_status: "",
              percentage_difference: 0,
            },
            nonliquid: {
              total_transaction_count: 0,
              total_transaction_amount: 0,
              pending_transaction_count: 0,
              previous_total_transaction_count: 0,
              within_t2_transaction: 0,
              beyond_t2_transaction: 0,
              total_completed_transaction_count: 0,
              transaction_status: "",
              percentage_difference: 0,
            },
          },
          outflow: {
            combined: {
              total_transaction_count: 0,
              total_transaction_amount: 0,
              pending_transaction_count: 0,
              previous_total_transaction_count: 0,
              within_t2_transaction: 0,
              beyond_t2_transaction: 0,
              total_completed_transaction_count: 0,
              transaction_status: "",
              percentage_difference: 0,
            },
            liquid: {
              total_transaction_count: 0,
              total_transaction_amount: 0,
              pending_transaction_count: 0,
              previous_total_transaction_count: 0,
              within_t2_transaction: 0,
              beyond_t2_transaction: 0,
              total_completed_transaction_count: 0,
              transaction_status: "",
              percentage_difference: 0,
            },
            nonliquid: {
              total_transaction_count: 0,
              total_transaction_amount: 0,
              pending_transaction_count: 0,
              previous_total_transaction_count: 0,
              within_t2_transaction: 0,
              beyond_t2_transaction: 0,
              total_completed_transaction_count: 0,
              transaction_status: "",
              percentage_difference: 0,
            },
          },
        };

        let rejectTrnasObject = {
          inflow: {
            combine: {
              amount: 0,
              count: 0,
            },
            liquid: {
              amount: 0,
              count: 0,
            },
            non_liquid: {
              amount: 0,
              count: 0,
            },
          },
          outflow: {
            combine: {
              amount: 0,
              count: 0,
            },
            liquid: {
              amount: 0,
              count: 0,
            },
            non_liquid: {
              amount: 0,
              count: 0,
            },
          },
        };
        let deleteTrnasObject = {
          inflow: {
            combine: {
              amount: 0,
              count: 0,
            },
            liquid: {
              amount: 0,
              count: 0,
            },
            non_liquid: {
              amount: 0,
              count: 0,
            },
          },
          outflow: {
            combine: {
              amount: 0,
              count: 0,
            },
            liquid: {
              amount: 0,
              count: 0,
            },
            non_liquid: {
              amount: 0,
              count: 0,
            },
          },
        };
        const rowsInflow = rows.filter((item) => item.flow == "Inflow");
        const rowsOutflow = rows.filter((item) => item.flow == "Outflow");
        let previous_date_inflow, previous_date_outflow;
        for (let i = 0; i < rowsInflow.length; i++) {
          if (rowsInflow[i].trdt != transaction_date) {
            previous_date_inflow = rowsInflow[i].trdt;
            break;
          }
        }
        for (let i = 0; i < rowsOutflow.length; i++) {
          if (rowsOutflow[i].trdt != transaction_date) {
            previous_date_outflow = rowsOutflow[i].trdt;
            break;
          }
        }

        rowsInflow.forEach((ele, ind) => {
          if (ele.asset_class == "LIQUID") {
            if (ele.trdt == transaction_date) {
              object.inflow.liquid.total_transaction_count += Number(ele.count);
              object.inflow.liquid.total_transaction_amount += Number(ele.sum);
              if (ele.status == "pending_txn_count")
                object.inflow.liquid.pending_transaction_count += Number(
                  ele.count
                );
              if (ele.status == "deleted_txn_count") {
                deleteTrnasObject.inflow.liquid.count += Number(ele.count);
                deleteTrnasObject.inflow.liquid.amount += Number(ele.sum);
              }
              if (ele.status == "rejected_txn_count") {
                rejectTrnasObject.inflow.liquid.count += Number(ele.count);
                rejectTrnasObject.inflow.liquid.amount += Number(ele.sum);
              }
            } else if (ele.trdt == previous_date_inflow) {
              object.inflow.liquid.previous_total_transaction_count += Number(
                ele.count
              );
            }
          } else {
            if (ele.trdt == transaction_date) {
              object.inflow.nonliquid.total_transaction_count += Number(
                ele.count
              );
              object.inflow.nonliquid.total_transaction_amount += Number(
                ele.sum
              );

              if (ele.status == "pending_txn_count")
                object.inflow.nonliquid.pending_transaction_count += Number(
                  ele.count
                );
              if (ele.status == "deleted_txn_count") {
                deleteTrnasObject.inflow.non_liquid.count += Number(ele.count);
                deleteTrnasObject.inflow.non_liquid.amount += Number(ele.sum);
              }
              if (ele.status == "rejected_txn_count") {
                rejectTrnasObject.inflow.non_liquid.count += Number(ele.count);
                rejectTrnasObject.inflow.non_liquid.amount += Number(ele.sum);
              }
            } else if (ele.trdt == previous_date_inflow) {
              object.inflow.nonliquid.previous_total_transaction_count +=
                Number(ele.count);
            }
          }
        });
        rowsOutflow.forEach((ele, ind) => {
          if (ele.asset_class == "LIQUID") {
            if (ele.trdt == transaction_date) {
              object.outflow.liquid.total_transaction_count += Number(
                ele.count
              );
              object.outflow.liquid.total_transaction_amount += Number(ele.sum);
              if (ele.status == "pending_txn_count")
                object.outflow.liquid.pending_transaction_count += Number(
                  ele.count
                );
              if (ele.status == "deleted_txn_count") {
                deleteTrnasObject.outflow.liquid.count += Number(ele.count);
                deleteTrnasObject.outflow.liquid.amount += Number(ele.sum);
              }
              if (ele.status == "rejected_txn_count") {
                rejectTrnasObject.outflow.liquid.count += Number(ele.count);
                rejectTrnasObject.outflow.liquid.amount += Number(ele.sum);
              }
            } else if (ele.trdt == previous_date_outflow) {
              object.outflow.liquid.previous_total_transaction_count += Number(
                ele.count
              );
            }
          } else {
            if (ele.trdt == transaction_date) {
              object.outflow.nonliquid.total_transaction_count += Number(
                ele.count
              );
              object.outflow.nonliquid.total_transaction_amount += Number(
                ele.sum
              );

              if (ele.status == "pending_txn_count")
                object.outflow.nonliquid.pending_transaction_count += Number(
                  ele.count
                );
              if (ele.status == "deleted_txn_count") {
                deleteTrnasObject.outflow.non_liquid.count += Number(ele.count);
                deleteTrnasObject.outflow.non_liquid.amount += Number(ele.sum);
              }
              if (ele.status == "rejected_txn_count") {
                rejectTrnasObject.outflow.non_liquid.count += Number(ele.count);
                rejectTrnasObject.outflow.non_liquid.amount += Number(ele.sum);
              }
            } else if (ele.trdt == previous_date_outflow) {
              object.outflow.nonliquid.previous_total_transaction_count +=
                Number(ele.count);
            }
          }
        });

        object.inflow.combined.previous_total_transaction_count =
          object.inflow.liquid.previous_total_transaction_count +
          object.inflow.nonliquid.previous_total_transaction_count;
        object.inflow.combined.pending_transaction_count =
          object.inflow.liquid.pending_transaction_count +
          object.inflow.nonliquid.pending_transaction_count;
        object.inflow.combined.total_transaction_amount =
          object.inflow.liquid.total_transaction_amount +
          object.inflow.nonliquid.total_transaction_amount;
        object.inflow.combined.total_transaction_count =
          object.inflow.liquid.total_transaction_count +
          object.inflow.nonliquid.total_transaction_count;
        object.inflow.liquid.total_completed_transaction_count =
          object.inflow.liquid.total_transaction_count -
          object.inflow.liquid.pending_transaction_count;
        object.inflow.nonliquid.total_completed_transaction_count =
          object.inflow.nonliquid.total_transaction_count -
          object.inflow.nonliquid.pending_transaction_count;
        object.inflow.combined.total_completed_transaction_count =
          object.inflow.liquid.total_completed_transaction_count +
          object.inflow.nonliquid.total_completed_transaction_count -
          (object.inflow.liquid.pending_transaction_count +
            object.inflow.nonliquid.pending_transaction_count);
        if (object.inflow.liquid.pending_transaction_count > 0)
          object.inflow.liquid.transaction_status = "Pending";
        else object.inflow.liquid.transaction_status = "Completed";
        if (object.inflow.nonliquid.pending_transaction_count > 0)
          object.inflow.nonliquid.transaction_status = "Pending";
        else object.inflow.nonliquid.transaction_status = "Completed";
        if (object.inflow.combined.pending_transaction_count > 0)
          object.inflow.combined.transaction_status = "Pending";
        else object.inflow.combined.transaction_status = "Completed";

        object.outflow.combined.previous_total_transaction_count =
          object.outflow.liquid.previous_total_transaction_count +
          object.outflow.nonliquid.previous_total_transaction_count;
        object.outflow.combined.pending_transaction_count =
          object.outflow.liquid.pending_transaction_count +
          object.outflow.nonliquid.pending_transaction_count;
        object.outflow.combined.total_transaction_amount =
          object.outflow.liquid.total_transaction_amount +
          object.outflow.nonliquid.total_transaction_amount;
        object.outflow.combined.total_transaction_count =
          object.outflow.liquid.total_transaction_count +
          object.outflow.nonliquid.total_transaction_count;
        object.outflow.liquid.total_completed_transaction_count =
          object.outflow.liquid.total_transaction_count -
          object.outflow.liquid.pending_transaction_count;
        object.outflow.nonliquid.total_completed_transaction_count =
          object.outflow.nonliquid.total_transaction_count -
          object.outflow.nonliquid.pending_transaction_count;
        object.outflow.combined.total_completed_transaction_count =
          object.outflow.liquid.total_completed_transaction_count +
          object.outflow.nonliquid.total_completed_transaction_count -
          (object.outflow.liquid.pending_transaction_count +
            object.outflow.nonliquid.pending_transaction_count);
        if (object.outflow.liquid.pending_transaction_count > 0)
          object.outflow.liquid.transaction_status = "Pending";
        else object.outflow.liquid.transaction_status = "Completed";
        if (object.outflow.nonliquid.pending_transaction_count > 0)
          object.outflow.nonliquid.transaction_status = "Pending";
        else object.outflow.nonliquid.transaction_status = "Completed";
        if (object.outflow.combined.pending_transaction_count > 0)
          object.outflow.combined.transaction_status = "Pending";
        else object.outflow.combined.transaction_status = "Completed";

        rejectTrnasObject.inflow.combine.count =
          rejectTrnasObject.inflow.liquid.count +
          rejectTrnasObject.inflow.non_liquid.count;
        rejectTrnasObject.inflow.combine.amount =
          rejectTrnasObject.inflow.liquid.amount +
          rejectTrnasObject.inflow.non_liquid.amount;

        deleteTrnasObject.inflow.combine.count =
          deleteTrnasObject.inflow.liquid.count +
          deleteTrnasObject.inflow.non_liquid.count;
        deleteTrnasObject.inflow.combine.amount =
          deleteTrnasObject.inflow.liquid.amount +
          deleteTrnasObject.inflow.non_liquid.amount;

        rejectTrnasObject.outflow.combine.count =
          rejectTrnasObject.outflow.liquid.count +
          rejectTrnasObject.outflow.non_liquid.count;
        rejectTrnasObject.outflow.combine.amount =
          rejectTrnasObject.outflow.liquid.amount +
          rejectTrnasObject.outflow.non_liquid.amount;

        deleteTrnasObject.outflow.combine.count =
          deleteTrnasObject.outflow.liquid.count +
          deleteTrnasObject.outflow.non_liquid.count;
        deleteTrnasObject.outflow.combine.amount =
          deleteTrnasObject.outflow.liquid.amount +
          deleteTrnasObject.outflow.non_liquid.amount;

        

        dataObject.trhistory = object;
        dataObject.rejectTrnasObject = rejectTrnasObject;
        dataObject.deleteTrnasObject = deleteTrnasObject;
      };

      const TAT_Pending_Count_Calculation = (rows) => {
        let tillDateObject = {
          inflow: {
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
          },
          outflow: {
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
          },
        };

        let specificDateObject = {
          inflow: {
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
          },
          outflow: {
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
          },
        };

        const inflowData = rows.filter((item) => item.flow == "Inflow");
        const outflowData = rows.filter((item) => item.flow == "Outflow");

        rows.forEach((ele, ind) => {
          if (ele.trdt <= transaction_date) {
            const assetType =
              ele.asset_class === "LIQUID" ? "liquid" : "nonliquid";
            const transactionType =
              Number(ele.date_part) <= 2 ? "within" : "beyond";
            const flow = ele.flow === "Inflow" ? "inflow" : "outflow";
            tillDateObject[flow][assetType][transactionType] += Number(
              ele.txn_count
            );
            tillDateObject[flow][assetType]["total_pending_count"] += Number(
              ele.txn_count
            );
          }
          if (ele.trdt == transaction_date) {
            const assetType =
              ele.asset_class === "LIQUID" ? "liquid" : "nonliquid";
            const transactionType =
              Number(ele.date_part) <= 2 ? "within" : "beyond";
            const flow = ele.flow === "Inflow" ? "inflow" : "outflow";

            specificDateObject[flow][assetType][transactionType] += Number(
              ele.txn_count
            );
            specificDateObject[flow][assetType]["total_pending_count"] +=
              Number(ele.txn_count);
          }
        });

        tillDateObject["inflow"]["combine"]["within"] =
          tillDateObject["inflow"]["liquid"]["within"] +
          tillDateObject["inflow"]["nonliquid"]["within"];
        tillDateObject["outflow"]["combine"]["within"] =
          tillDateObject["outflow"]["liquid"]["within"] +
          tillDateObject["outflow"]["nonliquid"]["within"];
        specificDateObject["inflow"]["combine"]["within"] =
          specificDateObject["inflow"]["liquid"]["within"] +
          specificDateObject["inflow"]["nonliquid"]["within"];
        specificDateObject["outflow"]["combine"]["within"] =
          specificDateObject["outflow"]["liquid"]["within"] +
          specificDateObject["outflow"]["nonliquid"]["within"];
        tillDateObject["inflow"]["combine"]["beyond"] =
          tillDateObject["inflow"]["liquid"]["beyond"] +
          tillDateObject["inflow"]["nonliquid"]["beyond"];
        tillDateObject["outflow"]["combine"]["beyond"] =
          tillDateObject["outflow"]["liquid"]["beyond"] +
          tillDateObject["outflow"]["nonliquid"]["beyond"];
        specificDateObject["inflow"]["combine"]["beyond"] =
          specificDateObject["inflow"]["liquid"]["beyond"] +
          specificDateObject["inflow"]["nonliquid"]["beyond"];
        specificDateObject["outflow"]["combine"]["beyond"] =
          specificDateObject["outflow"]["liquid"]["beyond"] +
          specificDateObject["outflow"]["nonliquid"]["beyond"];
        tillDateObject["inflow"]["combine"]["total_pending_count"] =
          tillDateObject["inflow"]["liquid"]["total_pending_count"] +
          tillDateObject["inflow"]["nonliquid"]["total_pending_count"];
        tillDateObject["outflow"]["combine"]["total_pending_count"] =
          tillDateObject["outflow"]["liquid"]["total_pending_count"] +
          tillDateObject["outflow"]["nonliquid"]["total_pending_count"];
        specificDateObject["inflow"]["combine"]["total_pending_count"] =
          specificDateObject["inflow"]["liquid"]["total_pending_count"] +
          specificDateObject["inflow"]["nonliquid"]["total_pending_count"];
        specificDateObject["outflow"]["combine"]["total_pending_count"] =
          specificDateObject["outflow"]["liquid"]["total_pending_count"] +
          specificDateObject["outflow"]["nonliquid"]["total_pending_count"];

        dataObject.tillDatePendingTATObject = tillDateObject;
        dataObject.specificDatePendingTATObject = specificDateObject;
      };

      // count
      const Top_Performing_Scheme_Calculation = async (rows) => {
        const inflow = rows.filter((item) => item.flow == "Inflow");
        const outflow = rows.filter((item) => item.flow == "Outflow");
        dataObject.top_performing_scheme_trxn = {
          inflow: inflow,
          outflow: outflow,
        };
      };

      const change_of_total_transaction_count = async (rows) => {
        let object = {
          inflow: {
            combined: {
              total_transaction_count: 0,
              total_transaction_amount: 0,
              pending_transaction_count: 0,
              previous_total_transaction_count: 0,
              within_t2_transaction: 0,
              beyond_t2_transaction: 0,
              total_completed_transaction_count: 0,
              transaction_status: "",
              percentage_difference: 0,
            },
            liquid: {
              total_transaction_count: 0,
              total_transaction_amount: 0,
              pending_transaction_count: 0,
              previous_total_transaction_count: 0,
              within_t2_transaction: 0,
              beyond_t2_transaction: 0,
              total_completed_transaction_count: 0,
              transaction_status: "",
              percentage_difference: 0,
            },
            nonliquid: {
              total_transaction_count: 0,
              total_transaction_amount: 0,
              pending_transaction_count: 0,
              previous_total_transaction_count: 0,
              within_t2_transaction: 0,
              beyond_t2_transaction: 0,
              total_completed_transaction_count: 0,
              transaction_status: "",
              percentage_difference: 0,
            },
          },
          outflow: {
            combined: {
              total_transaction_count: 0,
              total_transaction_amount: 0,
              pending_transaction_count: 0,
              previous_total_transaction_count: 0,
              within_t2_transaction: 0,
              beyond_t2_transaction: 0,
              total_completed_transaction_count: 0,
              transaction_status: "",
              percentage_difference: 0,
            },
            liquid: {
              total_transaction_count: 0,
              total_transaction_amount: 0,
              pending_transaction_count: 0,
              previous_total_transaction_count: 0,
              within_t2_transaction: 0,
              beyond_t2_transaction: 0,
              total_completed_transaction_count: 0,
              transaction_status: "",
              percentage_difference: 0,
            },
            nonliquid: {
              total_transaction_count: 0,
              total_transaction_amount: 0,
              pending_transaction_count: 0,
              previous_total_transaction_count: 0,
              within_t2_transaction: 0,
              beyond_t2_transaction: 0,
              total_completed_transaction_count: 0,
              transaction_status: "",
              percentage_difference: 0,
            },
          },
        };

        let rejectTrnasObject = {
          inflow: {
            combine: {
              amount: 0,
              count: 0,
            },
            liquid: {
              amount: 0,
              count: 0,
            },
            non_liquid: {
              amount: 0,
              count: 0,
            },
          },
          outflow: {
            combine: {
              amount: 0,
              count: 0,
            },
            liquid: {
              amount: 0,
              count: 0,
            },
            non_liquid: {
              amount: 0,
              count: 0,
            },
          },
        };
        let deleteTrnasObject = {
          inflow: {
            combine: {
              amount: 0,
              count: 0,
            },
            liquid: {
              amount: 0,
              count: 0,
            },
            non_liquid: {
              amount: 0,
              count: 0,
            },
          },
          outflow: {
            combine: {
              amount: 0,
              count: 0,
            },
            liquid: {
              amount: 0,
              count: 0,
            },
            non_liquid: {
              amount: 0,
              count: 0,
            },
          },
        };
        const rowsInflow = rows.filter((item) => item.flow == "Inflow");
        const rowsOutflow = rows.filter((item) => item.flow == "Outflow");
        let previous_date_inflow, previous_date_outflow;
        for (let i = 0; i < rowsInflow.length; i++) {
          if (rowsInflow[i].trdt != transaction_date) {
            previous_date_inflow = rowsInflow[i].trdt;
            break;
          }
        }
        for (let i = 0; i < rowsOutflow.length; i++) {
          if (rowsOutflow[i].trdt != transaction_date) {
            previous_date_outflow = rowsOutflow[i].trdt;
            break;
          }
        }

        rowsInflow.forEach((ele, ind) => {
          if (ele.asset_class == "LIQUID") {
            if (ele.trdt == transaction_date) {
              object.inflow.liquid.total_transaction_count += Number(ele.count);
              object.inflow.liquid.total_transaction_amount += Number(ele.sum);
              if (ele.status == "pending_txn_count")
                object.inflow.liquid.pending_transaction_count += Number(
                  ele.count
                );
              if (ele.status == "deleted_txn_count") {
                deleteTrnasObject.inflow.liquid.count += Number(ele.count);
                deleteTrnasObject.inflow.liquid.amount += Number(ele.sum);
              }
              if (ele.status == "rejected_txn_count") {
                rejectTrnasObject.inflow.liquid.count += Number(ele.count);
                rejectTrnasObject.inflow.liquid.amount += Number(ele.sum);
              }
            } else if (ele.trdt == previous_date_inflow) {
              object.inflow.liquid.previous_total_transaction_count += Number(
                ele.count
              );
            }
          } else {
            if (ele.trdt == transaction_date) {
              object.inflow.nonliquid.total_transaction_count += Number(
                ele.count
              );
              object.inflow.nonliquid.total_transaction_amount += Number(
                ele.sum
              );

              if (ele.status == "pending_txn_count")
                object.inflow.nonliquid.pending_transaction_count += Number(
                  ele.count
                );
              if (ele.status == "deleted_txn_count") {
                deleteTrnasObject.inflow.non_liquid.count += Number(ele.count);
                deleteTrnasObject.inflow.non_liquid.amount += Number(ele.sum);
              }
              if (ele.status == "rejected_txn_count") {
                rejectTrnasObject.inflow.non_liquid.count += Number(ele.count);
                rejectTrnasObject.inflow.non_liquid.amount += Number(ele.sum);
              }
            } else if (ele.trdt == previous_date_inflow) {
              object.inflow.nonliquid.previous_total_transaction_count +=
                Number(ele.count);
            }
          }
        });
        rowsOutflow.forEach((ele, ind) => {
          if (ele.asset_class == "LIQUID") {
            if (ele.trdt == transaction_date) {
              object.outflow.liquid.total_transaction_count += Number(
                ele.count
              );
              object.outflow.liquid.total_transaction_amount += Number(ele.sum);
              if (ele.status == "pending_txn_count")
                object.outflow.liquid.pending_transaction_count += Number(
                  ele.count
                );
              if (ele.status == "deleted_txn_count") {
                deleteTrnasObject.outflow.liquid.count += Number(ele.count);
                deleteTrnasObject.outflow.liquid.amount += Number(ele.sum);
              }
              if (ele.status == "rejected_txn_count") {
                rejectTrnasObject.outflow.liquid.count += Number(ele.count);
                rejectTrnasObject.outflow.liquid.amount += Number(ele.sum);
              }
            } else if (ele.trdt == previous_date_outflow) {
              object.outflow.liquid.previous_total_transaction_count += Number(
                ele.count
              );
            }
          } else {
            if (ele.trdt == transaction_date) {
              object.outflow.nonliquid.total_transaction_count += Number(
                ele.count
              );
              object.outflow.nonliquid.total_transaction_amount += Number(
                ele.sum
              );

              if (ele.status == "pending_txn_count")
                object.outflow.nonliquid.pending_transaction_count += Number(
                  ele.count
                );
              if (ele.status == "deleted_txn_count") {
                deleteTrnasObject.outflow.non_liquid.count += Number(ele.count);
                deleteTrnasObject.outflow.non_liquid.amount += Number(ele.sum);
              }
              if (ele.status == "rejected_txn_count") {
                rejectTrnasObject.outflow.non_liquid.count += Number(ele.count);
                rejectTrnasObject.outflow.non_liquid.amount += Number(ele.sum);
              }
            } else if (ele.trdt == previous_date_outflow) {
              object.outflow.nonliquid.previous_total_transaction_count +=
                Number(ele.count);
            }
          }
        });

        object.inflow.combined.previous_total_transaction_count =
          object.inflow.liquid.previous_total_transaction_count +
          object.inflow.nonliquid.previous_total_transaction_count;
        object.inflow.combined.pending_transaction_count =
          object.inflow.liquid.pending_transaction_count +
          object.inflow.nonliquid.pending_transaction_count;
        object.inflow.combined.total_transaction_amount =
          object.inflow.liquid.total_transaction_amount +
          object.inflow.nonliquid.total_transaction_amount;
        object.inflow.combined.total_transaction_count =
          object.inflow.liquid.total_transaction_count +
          object.inflow.nonliquid.total_transaction_count;
        object.inflow.liquid.total_completed_transaction_count =
          object.inflow.liquid.total_transaction_count -
          object.inflow.liquid.pending_transaction_count;
        object.inflow.nonliquid.total_completed_transaction_count =
          object.inflow.nonliquid.total_transaction_count -
          object.inflow.nonliquid.pending_transaction_count;
        object.inflow.combined.total_completed_transaction_count =
          object.inflow.liquid.total_completed_transaction_count +
          object.inflow.nonliquid.total_completed_transaction_count -
          (object.inflow.liquid.pending_transaction_count +
            object.inflow.nonliquid.pending_transaction_count);
        if (object.inflow.liquid.pending_transaction_count > 0)
          object.inflow.liquid.transaction_status = "Pending";
        else object.inflow.liquid.transaction_status = "Completed";
        if (object.inflow.nonliquid.pending_transaction_count > 0)
          object.inflow.nonliquid.transaction_status = "Pending";
        else object.inflow.nonliquid.transaction_status = "Completed";
        if (object.inflow.combined.pending_transaction_count > 0)
          object.inflow.combined.transaction_status = "Pending";
        else object.inflow.combined.transaction_status = "Completed";

        object.outflow.combined.previous_total_transaction_count =
          object.outflow.liquid.previous_total_transaction_count +
          object.outflow.nonliquid.previous_total_transaction_count;
        object.outflow.combined.pending_transaction_count =
          object.outflow.liquid.pending_transaction_count +
          object.outflow.nonliquid.pending_transaction_count;
        object.outflow.combined.total_transaction_amount =
          object.outflow.liquid.total_transaction_amount +
          object.outflow.nonliquid.total_transaction_amount;
        object.outflow.combined.total_transaction_count =
          object.outflow.liquid.total_transaction_count +
          object.outflow.nonliquid.total_transaction_count;
        object.outflow.liquid.total_completed_transaction_count =
          object.outflow.liquid.total_transaction_count -
          object.outflow.liquid.pending_transaction_count;
        object.outflow.nonliquid.total_completed_transaction_count =
          object.outflow.nonliquid.total_transaction_count -
          object.outflow.nonliquid.pending_transaction_count;
        object.outflow.combined.total_completed_transaction_count =
          object.outflow.liquid.total_completed_transaction_count +
          object.outflow.nonliquid.total_completed_transaction_count -
          (object.outflow.liquid.pending_transaction_count +
            object.outflow.nonliquid.pending_transaction_count);
        if (object.outflow.liquid.pending_transaction_count > 0)
          object.outflow.liquid.transaction_status = "Pending";
        else object.outflow.liquid.transaction_status = "Completed";
        if (object.outflow.nonliquid.pending_transaction_count > 0)
          object.outflow.nonliquid.transaction_status = "Pending";
        else object.outflow.nonliquid.transaction_status = "Completed";
        if (object.outflow.combined.pending_transaction_count > 0)
          object.outflow.combined.transaction_status = "Pending";
        else object.outflow.combined.transaction_status = "Completed";

        

        let liquid_percentage_inflow =
          ((object.inflow.liquid.total_transaction_count -
            object.inflow.liquid.previous_total_transaction_count) /
            object.inflow.liquid.previous_total_transaction_count) *
          100;
        let nonliquid_percentage_inflow =
          ((object.inflow.nonliquid.total_transaction_count -
            object.inflow.nonliquid.previous_total_transaction_count) /
            object.inflow.nonliquid.previous_total_transaction_count) *
          100;
        let combined_percentage_inflow =
          ((object.inflow.combined.total_transaction_count -
            object.inflow.combined.previous_total_transaction_count) /
            object.inflow.combined.previous_total_transaction_count) *
          100;
        object.inflow.liquid.percentage_difference = liquid_percentage_inflow;
        object.inflow.nonliquid.percentage_difference =
          nonliquid_percentage_inflow;
        object.inflow.combined.percentage_difference =
          combined_percentage_inflow;

        let liquid_percentage_outflow =
          ((object.outflow.liquid.total_transaction_count -
            object.outflow.liquid.previous_total_transaction_count) /
            object.outflow.liquid.previous_total_transaction_count) *
          100;
        let nonliquid_percentage_outflow =
          ((object.outflow.nonliquid.total_transaction_count -
            object.outflow.nonliquid.previous_total_transaction_count) /
            object.outflow.nonliquid.previous_total_transaction_count) *
          100;
        let combined_percentage_outflow =
          ((object.outflow.combined.total_transaction_count -
            object.outflow.combined.previous_total_transaction_count) /
            object.outflow.combined.previous_total_transaction_count) *
          100;
        object.outflow.liquid.percentage_difference = liquid_percentage_outflow;
        object.outflow.nonliquid.percentage_difference =
          nonliquid_percentage_outflow;
        object.outflow.combined.percentage_difference =
          combined_percentage_outflow;

          dataObject.changeOfTotalTransaction = object;
      }

      const Top_permoming_scheme_trxn_calculations = async (rows) => {
        const inflow = rows.filter((item) => item.flow == "Inflow");
        const outflow = rows.filter((item) => item.flow == "Outflow");
        dataObject.top_performing_scheme_calculation = {
          inflow: inflow,
          outflow: outflow,
        };
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "change_of_total_transaction_count":
              change_of_total_transaction_count(rows);
              break;
            case "average_transaction":
              average_transaction_Calculation(rows);
              break;
            case "total_transaction_count":
              total_transaction_count_Calculation(rows);
              break;
            case "TAT_Pending_Count":
              TAT_Pending_Count_Calculation(rows);
              break;
            case "Top_Performing_Scheme":
              Top_Performing_Scheme_Calculation(rows);
              break;
            case "top_performing_scheme_trxn":
              Top_permoming_scheme_trxn_calculations(rows);
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
        let purchesQueries = await SwitchQueries.findOne(
          { endpoint: "SwitchTransaction" },
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

          formattedQueries.push({ name: ele.name, query: formattedQuery });
        });
        const all_results = await fetch_results(formattedQueries);
        await calculate_result(all_results);
        const cached_data = await CachedData.findOneAndUpdate(
          { date: transaction_date, type: "switch", fund: fund },
          {
            $set: {
              date: transaction_date,
              "dataObject.average_transaction_perminutes":
                dataObject.average_transaction_perminutes,
              "dataObject.trhistory": dataObject.trhistory,
              "dataObject.tillDatePendingTATObject":
                dataObject.tillDatePendingTATObject,
              "dataObject.specificDatePendingTATObject":
                dataObject.specificDatePendingTATObject,
              "dataObject.rejectTrnasObject": dataObject.rejectTrnasObject,
              "dataObject.deleteTrnasObject": dataObject.deleteTrnasObject,
              "dataObject.top_performing_scheme_calculation":
                dataObject.top_performing_scheme_calculation,
              "dataObject.top_performing_scheme_trxn":
                dataObject.top_performing_scheme_trxn,
                "dataObject.changeOfTotalTransaction":
                dataObject.changeOfTotalTransaction
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
    console.log("Error in switch transaction provider", e);
    throw new Error(e);
  }
};

export { switch_transction_provider };
