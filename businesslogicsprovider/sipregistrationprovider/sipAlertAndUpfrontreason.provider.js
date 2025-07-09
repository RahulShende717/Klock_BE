import CachedData from "../../models/cacheddata.model.js";
import SIPQueries from "../../models/sipqueries.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getPoolObj } from "../../utils/pool.js";
import SipRegistrationQueries from "../../models/sipregistration.model.js";

export const sipAlertAndUpfrontreason = async (
  trDate,
  fundcode,
  snsstatus = false
) => {
  try {
    let dataObject = {};
    let errCards = [];
    let transaction_date = trDate;
    let fund = fundcode;
    let schema = getFundWiseSchema(fund);
    const pool = getPoolObj(fund);
    const keysToCheck = [
      "sip_cancel",
      "sip_pause",
      "registration_completed",
      "registration_incomplete",
      // "debit_failure_Reasons",
      "UpfrontRejectionReasons",
    ];
    const isDataPresent = await CachedData.findOne({
      $and: [
        { fund: fund, date: transaction_date, type: "sip_registration" },
        {
          $and: keysToCheck.map((key) => ({
            [`dataObject.${key}`]: { $exists: true },
          })),
        },
      ],
    });

    if (!snsstatus && isDataPresent != null) {

      return {
        sip_cancel: isDataPresent.dataObject.sip_cancel,
        sip_pause: isDataPresent.dataObject.sip_pause,
        registration_completed: isDataPresent.dataObject.registration_completed,
        registration_incomplete:
          isDataPresent.dataObject.registration_incomplete,
        UpfrontRejectionReasons:
          isDataPresent.dataObject.UpfrontRejectionReasons,
      };
    } else {
      const registration_incomplete_calculation = (rows) => {
        let obj = {
          combine: {
            count: 0,
          },
          liquid: {
            count: 0,
          },
          nonliquid: {
            count: 0,
          },
        };

        rows.forEach((ele, ind) => {
          obj.combine.count += Number(ele.cnt);
          if (ele.asset_class == "LIQUID") {
            obj.liquid.count += Number(ele.cnt);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.nonliquid.count += Number(ele.cnt);
          }
        });
        dataObject.registration_incomplete = obj;
      };

      const registration_completed_calculation = (rows) => {
        let obj = {
          combine: {
            count: 0,
          },
          liquid: {
            count: 0,
          },
          nonliquid: {
            count: 0,
          },
        };

        rows.forEach((ele, ind) => {
          obj.combine.count += Number(ele.count);
          if (ele.asset_class == "LIQUID") {
            obj.liquid.count += Number(ele.count);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.nonliquid.count += Number(ele.count);
          }
        });

        dataObject.registration_completed = obj;
      };

      const sip_pause_calculation = (rows) => {
        let obj = {
          combine: {
            count: 0,
          },
          liquid: {
            count: 0,
          },
          nonliquid: {
            count: 0,
          },
        };

        rows.forEach((ele, ind) => {
          obj.combine.count += Number(ele.sum);
          if (ele.asset_class == "LIQUID") {
            obj.liquid.count += Number(ele.sum);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.nonliquid.count += Number(ele.sum);
          }
        });

        dataObject.sip_pause = obj;
      };

      const sip_cancel_calculation = (rows) => {
        let obj = {
          combine: {
            count: 0,
          },
          liquid: {
            count: 0,
          },
          nonliquid: {
            count: 0,
          },
        };

        rows.forEach((ele, ind) => {
          obj.combine.count += Number(ele.sum);
          if (ele.asset_class == "LIQUID") {
            obj.liquid.count += Number(ele.sum);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.nonliquid.count += Number(ele.sum);
          }
        });

        dataObject.sip_cancel = obj;
      };

      const DebitFaiure_calculation = (rows) => {
        let obj = {
          combine: [],
          liquid: [],
          nonliquid: [],
        };
        rows.forEach((ele, ind) => {
          obj.combine.push(ele);
          if (ele.asset_class == "LIQUID") {
            obj.liquid.push(ele);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.nonliquid.push(ele);
          }
        });
        dataObject.debit_failure_Reasons = obj;
      };
      const UpfrontRejectionReasonsTransactionsCalc = (rows) => {
        let obj = {
          combine: [],
          liquid: [],
          nonliquid: [],
        };

        rows.forEach((ele, ind) => {
          obj.combine.push(ele);
          if (ele.asset_class == "LIQUID") {
            obj.liquid.push(ele);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.nonliquid.push(ele);
          }
        });
        dataObject.UpfrontRejectionReasonsTransaction = obj;
      };
      const UpfrontRejectionReasonsRegistrationCalc = (rows) => {
        let obj = {
          combine: [],
          liquid: [],
          nonliquid: [],
        };

        rows.forEach((ele, ind) => {
          obj.combine.push(ele);
          if (ele.asset_class == "LIQUID") {
            obj.liquid.push(ele);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.nonliquid.push(ele);
          }
        });
        dataObject.UpfrontRejectionReasons = obj;
      };

      const calculate_result = async (array) => {
        try {
          array.forEach((item) => {
            const rows = item.result;
            switch (item.name) {
              case "UPFRONT_REJECTION_REASONS":
                UpfrontRejectionReasonsRegistrationCalc(rows);
                break;

              case "registration_completed":
                registration_completed_calculation(rows);

              case "registration_incomplete":
                registration_incomplete_calculation(rows);
                break;

              case "sip_pause":
                sip_pause_calculation(rows);
                break;

              case "sip_cancel":
                sip_cancel_calculation(rows);
                break;
              // case 'nigo_summary_report_liquid': handle_nigo_summary_data(rows, "liquid"); break;
              // case 'nigo_summary_report_nonliquid': handle_nigo_summary_data(rows, "nonliquid"); break;
            }
          });
        } catch (e) {
          console.log("Error in query results: ", e);
        }
      };

      const fetch_results = async (queries) => {
        try {
          return Promise.all(
            queries.map(async (queryObj) => {
              const startTime = Date.now();
              const result = await pool.query(queryObj.query);
              const endTime = Date.now();
              return {
                name: queryObj.name,
                result: result.rows,
              };
            })
          );
        } catch (error) {
          console.log("Query Error name:", error, queryObj.name);
          errCards.push({ name: queryObj.name });
          return;
        }
      };

      const main = async () => {
        let formattedQueries = [];
        let redemptionQueries = await SipRegistrationQueries.findOne(
          { endpoint: "regAlertAndUpfrontReason" },
          { queriesArray: 1 }
        );
        let queriesArray = redemptionQueries.queriesArray;
        queriesArray.map((ele) => {
          let formattedQuery = ele.query.replace(
            /'transactionDate'/g,
            `'${transaction_date}'`
          );
          formattedQuery = formattedQuery.replace(/schema/g, `${schema}`);
          formattedQuery = formattedQuery.replace(/'fund'/g, `'${fund}'`);

          formattedQueries.push({ name: ele.name, query: formattedQuery });
        });

        // let dummy=[]
        //   dummy.push(formattedQueries[1])
        const all_results = await fetch_results(formattedQueries);

        await calculate_result(all_results);

        const cached_data = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "sip_registration" },
          {
            $set: {
              date: transaction_date,
              "dataObject.registration_incomplete":
                dataObject.registration_incomplete,
              "dataObject.registration_completed":
                dataObject.registration_completed,
              // "dataObject.debit_failure_Reasons":
              //   dataObject.debit_failure_Reasons,
              "dataObject.UpfrontRejectionReasons":
                dataObject.UpfrontRejectionReasons,
              "dataObject.sip_pause": dataObject.sip_pause,
              "dataObject.sip_cancel": dataObject.sip_cancel,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );
        return dataObject;
      };
      const result = await main();
      return result;
    }
  } catch (e) {
    console.log(
      "Error in alerts and debit failure upfront provider in sip ",
      e
    );
    throw new Error(e);
  }
};
