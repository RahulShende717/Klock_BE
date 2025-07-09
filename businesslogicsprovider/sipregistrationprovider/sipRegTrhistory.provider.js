import CachedData from "../../models/cacheddata.model.js";
import SipRegistrationQueries from "../../models/sipregistration.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolObj } from "../../utils/pool.js";
import moment from "moment";

const sipRegistrationTrHistory = async (
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
    const from_date = moment(transaction_date)
      .subtract(5, "days")
      .format("YYYY-MM-DD");
    const keysToCheck = [
      "reported_resigstration",
      "top_performing_scheme_trxn",
      "top_performing_scheme_calculation",
      "sip_canclation",
      "total_application_recieved",
      "specificDatePendingTATObject",
      "average_transaction_perminutes",
      "changeOfTotalTransaction",
    ];
    const isDataPresent = await CachedData.findOne({
      $and: [
        { fund: fund, date: transaction_date, type: "sip_registration" },
        {
          $or: keysToCheck.map((key) => ({
            [`dataObject.${key}`]: { $exists: true },
          })),
        },
      ],
    });

    if (!snsstatus && isDataPresent != null) {
      return {
        reported_resigstration: isDataPresent.dataObject.reported_resigstration,
        top_performing_scheme_trxn:
          isDataPresent.dataObject.top_performing_scheme_trxn,
        top_performing_scheme_calculation:
          isDataPresent.dataObject.top_performing_scheme_calculation,
        sip_canclation: isDataPresent.dataObject.sip_canclation,
        total_application_recieved:
          isDataPresent.dataObject.total_application_recieved,
        specificDatePendingTATObject:
          isDataPresent.dataObject.specificDatePendingTATObject,
        changeOfTotalTransaction:
          isDataPresent.dataObject.changeOfTotalTransaction,
      };
    } else {
      const REPORTED_REGISTRATIONS_Calculation = async (rows) => {
        let object = {
          combined: {
            total_transaction_count: 0,
            total_transaction_amount: 0,
            average_ticket_size: 0,

            transaction_status: "",
            pending_transaction_count: 0,
          },
          liquid: {
            total_transaction_count: 0,
            total_transaction_amount: 0,
            average_ticket_size: 0,

            transaction_status: "",
            pending_transaction_count: 0,
          },
          nonliquid: {
            total_transaction_count: 0,
            total_transaction_amount: 0,
            average_ticket_size: 0,

            transaction_status: "",
            pending_transaction_count: 0,
          },
        };

        rows.forEach((ele, ind) => {
          if (ele.sip_regdt === transaction_date) {
            object.combined.total_transaction_count += Number(ele.count);
            object.combined.total_transaction_amount += Number(ele.amount);
            if (ele.status === "pending_sipreg_count")
              object.combined.pending_transaction_count += Number(ele.count);

            if (ele.asset_class == "LIQUID") {
              object.liquid.total_transaction_count += Number(ele.count);
              object.liquid.total_transaction_amount += Number(ele.amount);
              if (ele.status === "pending_sipreg_count")
                object.liquid.pending_transaction_count += Number(ele.count);
            } else {
              object.nonliquid.total_transaction_count += Number(ele.count);
              object.nonliquid.total_transaction_amount += Number(ele.amount);
              if (ele.status === "pending_sipreg_count")
                object.nonliquid.pending_transaction_count += Number(ele.count);
            }
          }
        });

        object.combined.average_ticket_size = (
          object.combined.total_transaction_amount /
          object.combined.total_transaction_count
        ).toFixed(2);
        object.liquid.average_ticket_size = (
          object.liquid.total_transaction_amount /
          object.liquid.total_transaction_count
        ).toFixed(2);
        object.nonliquid.average_ticket_size = (
          object.nonliquid.total_transaction_amount /
          object.nonliquid.total_transaction_count
        ).toFixed(2);
        if (object.liquid.pending_transaction_count > 0)
          object.liquid.transaction_status = "Pending";
        else object.liquid.transaction_status = "Completed";
        if (object.nonliquid.pending_transaction_count > 0)
          object.nonliquid.transaction_status = "Pending";
        else object.nonliquid.transaction_status = "Completed";
        if (object.combined.pending_transaction_count > 0)
          object.combined.transaction_status = "Pending";
        else object.combined.transaction_status = "Completed";
        dataObject.reported_resigstration = object;
      };

      const change_of_total_registration_count = async (rows) => {
        let object = {
          combined: {
            total_transaction_count: 0,
            previous_total_transaction_count: 0,
            percentage_difference: 0,
          },
          liquid: {
            total_transaction_count: 0,

            previous_total_transaction_count: 0,
            percentage_difference: 0,
          },
          nonliquid: {
            total_transaction_count: 0,

            previous_total_transaction_count: 0,
            percentage_difference: 0,
          },
        };

        let previous_date;
        for (let i = rows.length - 1; i >= 0; i--) {
          if (rows[i].sip_regdt != transaction_date) {
            previous_date = rows[i].sip_regdt;
            break;
          }
        }

        rows.forEach((ele, ind) => {
          if (ele.sip_regdt === transaction_date) {
            object.combined.total_transaction_count += Number(ele.count);

            if (ele.asset_class == "LIQUID") {
              object.liquid.total_transaction_count += Number(ele.count);
            } else {
              object.nonliquid.total_transaction_count += Number(ele.count);
            }
          } else if (ele.sip_regdt === previous_date) {
            object.combined.previous_total_transaction_count += Number(
              ele.count
            );

            if (ele.asset_class == "LIQUID") {
              object.liquid.previous_total_transaction_count += Number(
                ele.count
              );
            } else {
              object.nonliquid.previous_total_transaction_count += Number(
                ele.count
              );
            }
          }
        });

        object.combined.percentage_difference =
          ((object.combined.total_transaction_count -
            object.combined.previous_total_transaction_count) /
            object.combined.previous_total_transaction_count) *
          100;
        object.liquid.percentage_difference =
          ((object.liquid.total_transaction_count -
            object.liquid.previous_total_transaction_count) /
            object.liquid.previous_total_transaction_count) *
          100;
        object.nonliquid.percentage_difference =
          ((object.nonliquid.total_transaction_count -
            object.nonliquid.previous_total_transaction_count) /
            object.nonliquid.previous_total_transaction_count) *
          100;

        dataObject.changeOfTotalTransaction = object;
      };

      const TOP_PERFORMING_SCHEME_COUNTWISE_Calc = async (rows) => {
        dataObject.top_performing_scheme_trxn = rows;
      };

      const TOP_PERFORM_SCHEME_AMOUNTWISE_Calculation = (rows) => {
        dataObject.top_performing_scheme_calculation = rows;
      };
      const SIP_CANCELLATION_Calculation = (rows) => {
        let obj = {
          combined: {
            SIP_Cancellation_request: 0,
            SIP_Cancellation_pending: 0,
          },
          liquid: {
            SIP_Cancellation_request: 0,
            SIP_Cancellation_pending: 0,
          },
          nonliquid: {
            SIP_Cancellation_request: 0,
            SIP_Cancellation_pending: 0,
          },
        };

        rows.forEach((ele, ind) => {
          obj.combined.SIP_Cancellation_request += Number(ele.cnt);
          if (ele.status == "PENDING") {
            obj.combined.SIP_Cancellation_pending += Number(ele.cnt);
          }
          if (ele.asset_class == "LIQUID") {
            obj.liquid.SIP_Cancellation_request += Number(ele.cnt);
            if (ele.status == "PENDING") {
              obj.liquid.SIP_Cancellation_pending += Number(ele.cnt);
            }
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.nonliquid.SIP_Cancellation_request += Number(ele.cnt);
            if (ele.status == "PENDING") {
              obj.nonliquid.SIP_Cancellation_pending += Number(ele.cnt);
            }
          }
        });
        dataObject.sip_canclation = obj;
      };

      const Total_application_recieved_calculation = (rows) => {
        let obj = {
          liquid: {
            digital: 0,
            physical: 0,
          },
          nonliquid: {
            digital: 0,
            physical: 0,
          },
          combine: {
            digital: 0,
            physical: 0,
          },
        };

        rows.forEach((ele, ind) => {
          if (ele.source == "DIGITAL") {
            obj.combine.digital += Number(ele.txncount);
          } else {
            obj.combine.physical += Number(ele.txncount);
          }
          if (ele.asset_class == "LIQUID") {
            if (ele.source == "DIGITAL") {
              obj.liquid.digital += Number(ele.txncount);
            } else {
              obj.liquid.physical += Number(ele.txncount);
            }
          } else if (ele.asset_class == "NON LIQUID") {
            if (ele.source == "DIGITAL") {
              obj.nonliquid.digital += Number(ele.txncount);
            } else {
              obj.nonliquid.physical += Number(ele.txncount);
            }
          }
        });
        let liquid_digital_percentage =
          (obj.liquid.digital / obj.combine.digital) * 100;
        let liquid_physical_percentage =
          (obj.liquid.physical / obj.combine.physical) * 100;

        let nonliquid_digital_percentage =
          (obj.nonliquid.digital / obj.combine.digital) * 100;
        let nonliquid_physical_percentage =
          (obj.nonliquid.physical / obj.combine.physical) * 100;

        let combine_digital_percentage =
          (obj.combine.digital / obj.combine.digital) * 100;
        let combine_physical_percentage =
          (obj.combine.physical / obj.combine.physical) * 100;

        obj.liquid.percentage = liquid_digital_percentage;
        obj.nonliquid.percentage = nonliquid_digital_percentage;
        obj.liquid.percentage = liquid_physical_percentage;
        obj.nonliquid.percentage = nonliquid_physical_percentage;
        obj.combine.percentage = combine_digital_percentage;
        obj.combine.percentage = combine_physical_percentage;

        dataObject.total_application_recieved = obj;
      };

      const pending_trxn_calculation = (rows) => {
        let specificDateObject = {
          combine: {
            pending_count: 0,
            within: 0,
            beyond: 0,
            total_pending_count: 0,
            transaction_status: "",
          },
          liquid: {
            pending_count: 0,
            within: 0,
            beyond: 0,
            total_pending_count: 0,
            transaction_status: "",
          },
          nonliquid: {
            pending_count: 0,
            within: 0,
            beyond: 0,
            total_pending_count: 0,
            transaction_status: "",
          },
        };

        rows.forEach((ele, ind) => {
          const assetType =
            ele.asset_class === "LIQUID" ? "liquid" : "nonliquid";
          const transactionType =
            Number(ele.date_part) <= 2 ? "within" : "beyond";

          specificDateObject[assetType][transactionType] += Number(ele.txncnt);
          specificDateObject[assetType]["total_pending_count"] += Number(
            ele.txncnt
          );

          specificDateObject["combine"]["within"] =
            specificDateObject["liquid"]["within"] +
            specificDateObject["nonliquid"]["within"];
          specificDateObject["combine"]["beyond"] =
            specificDateObject["liquid"]["beyond"] +
            specificDateObject["nonliquid"]["beyond"];
          specificDateObject["combine"]["total_pending_count"] =
            specificDateObject["liquid"]["total_pending_count"] +
            specificDateObject["nonliquid"]["total_pending_count"];
        });

        dataObject.specificDatePendingTATObject = specificDateObject;

        dataObject.specificDatePendingTATObject.combine.transaction_status =
          dataObject.specificDatePendingTATObject.combine.total_pending_count >
          0
            ? "pending"
            : "completed";
        dataObject.specificDatePendingTATObject.liquid.transaction_status =
          dataObject.specificDatePendingTATObject.liquid.total_pending_count > 0
            ? "pending"
            : "completed";
        dataObject.specificDatePendingTATObject.nonliquid.transaction_status =
          dataObject.specificDatePendingTATObject.nonliquid
            .total_pending_count > 0
            ? "pending"
            : "completed";
        //   dataObject.reported_resigstration.combined.total_transaction_count -

        // dataObject.specificDatePendingTATObject.liquid.transaction_status =
        //   dataObject.reported_resigstration.liquid.total_transaction_count -
        //   dataObject.specificDatePendingTATObject.liquid.total_pending_count;
        // dataObject.specificDatePendingTATObject.nonliquid.transaction_status =
        //   dataObject.reported_resigstration.nonliquid.total_transaction_count -
        //   dataObject.specificDatePendingTATObject.nonliquid.total_pending_count;
      };

      const average_transaction_minute = (rows) => {
        dataObject.average_transaction_perminutes = rows;
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "REPORTED_REGISTRATIONS":
              REPORTED_REGISTRATIONS_Calculation(rows);
              break;
            case "change_of_total_transaction_count":
              change_of_total_registration_count(rows);
              break;
            case "TOP_PERFORMING_SCHEME_COUNTWISE":
              TOP_PERFORMING_SCHEME_COUNTWISE_Calc(rows);
              break;
            case "TOP_PERFORM_SCHEME_AMOUNTWISE":
              TOP_PERFORM_SCHEME_AMOUNTWISE_Calculation(rows);
              break;
            case "SIP_CANCELLATION":
              SIP_CANCELLATION_Calculation(rows);
              break;
            case "total_application_received":
              Total_application_recieved_calculation(rows);
            case "pending_trxn":
              pending_trxn_calculation(rows);
            case "average_transaction":
              average_transaction_minute(rows);
          }
        });
      };

      const fetch_results = async (queries) => {
        return Promise.all(
          queries.map(async (queryObj) => {
            // const result = await pool.query(queryObj.query);
            const startTime = Date.now();
            const result = await pool.query(queryObj.query);
            const endTime = Date.now();
            return {
              name: queryObj.name,
              result: result.rows,
            };
          })
        );
      };

      const main = async () => {
        let formattedQueries = [];
        let quries = await SipRegistrationQueries.findOne(
          { endpoint: "RegstrationTrHistory" },
          { queriesArray: 1 }
        );
        let queriesArray = quries.queriesArray;
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
          { fund: fund, date: transaction_date, type: "sip_registration" },
          {
            $set: {
              date: transaction_date,
              "dataObject.reported_resigstration":
                dataObject.reported_resigstration,
              "dataObject.top_performing_scheme_trxn":
                dataObject.top_performing_scheme_trxn,
              "dataObject.top_performing_scheme_calculation":
                dataObject.top_performing_scheme_calculation,
              "dataObject.sip_canclation": dataObject.sip_canclation,
              "dataObject.total_application_recieved":
                dataObject.total_application_recieved,
              "dataObject.specificDatePendingTATObject":
                dataObject.specificDatePendingTATObject,
              "dataObject.changeOfTotalTransaction":
                dataObject.changeOfTotalTransaction,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );
        return dataObject;
      };

      const data = await main();
      return data;
    }
  } catch (e) {
    console.log("Error in sipsummary provider ", e);
    throw new Error(e);
  }
};

export { sipRegistrationTrHistory };
