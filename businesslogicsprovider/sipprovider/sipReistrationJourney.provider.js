import CachedData from "../../models/cacheddata.model.js";
import SIPQueries from "../../models/sipqueries.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getPoolObj } from "../../utils/pool.js";

export const sipRegstrationJourney = async (
  trDate,
  fundcode,
  snsstatus = false
) => {
  try {
    let dataObject = {};
    let transaction_date = trDate;
    let fund = fundcode;
    let schema = getFundWiseSchema(fund);
    const pool = getPoolObj(fund);
    const keysToCheck = [
      "reported_registration",
      "pending_registration",
      "successfully_registered",
      "rejected_registration",
      "sent_to_aggregator",
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
      return isDataPresent.dataObject;
    } else {
      const Reported_registration_calculation = (rows) => {
        let obj = {
          reported_registration_combine: {
            amount: 0,
            count: 0,
          },
          reported_registration_liquid: {
            amount: 0,
            count: 0,
          },
          reported_registration_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        rows.forEach((ele, ind) => {
          obj.reported_registration_combine.amount += Number(ele.amt);
          obj.reported_registration_combine.count += Number(ele.cnt);
          if (ele.asset_class == "LIQUID") {
            obj.reported_registration_liquid.amount += Number(ele.amt);
            obj.reported_registration_liquid.count += Number(ele.cnt);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.reported_registration_nonliquid.amount += Number(ele.amt);
            obj.reported_registration_nonliquid.count += Number(ele.cnt);
          }
        });
        dataObject.reported_registration = obj;
      };
      const successfully_registered_calculation = (rows) => {
        let obj = {
          successfully_registered_combine: {
            amount: 0,
            count: 0,
          },
          successfully_registered_liquid: {
            amount: 0,
            count: 0,
          },
          successfully_registered_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        rows.forEach((ele, ind) => {
          obj.successfully_registered_combine.amount += Number(ele.amt);
          obj.successfully_registered_combine.count += Number(ele.cnt);
          if (ele.asset_class == "LIQUID") {
            obj.successfully_registered_liquid.amount += Number(ele.amt);
            obj.successfully_registered_liquid.count += Number(ele.cnt);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.successfully_registered_nonliquid.amount += Number(ele.amt);
            obj.successfully_registered_nonliquid.count += Number(ele.cnt);
          }
        });
        dataObject.successfully_registered = obj;
      };
      const pending_registration_calculation = (rows) => {
        let obj = {
          pending_registration_combine: {
            amount: 0,
            count: 0,
          },
          pending_registration_liquid: {
            amount: 0,
            count: 0,
          },
          pending_registration_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        rows.forEach((ele, ind) => {
          obj.pending_registration_combine.amount += Number(ele.amt);
          obj.pending_registration_combine.count += Number(ele.cnt);
          if (ele.asset_class == "LIQUID") {
            obj.pending_registration_liquid.amount += Number(ele.amt);
            obj.pending_registration_liquid.count += Number(ele.cnt);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.pending_registration_nonliquid.amount += Number(ele.amt);
            obj.pending_registration_nonliquid.count += Number(ele.cnt);
          }
        });
        dataObject.pending_registration = obj;
      };
      const rejected_registration_calculation = (rows) => {
        let obj = {
          rejected_registration_combine: {
            amount: 0,
            count: 0,
          },
          rejected_registration_liquid: {
            amount: 0,
            count: 0,
          },
          rejected_registration_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        rows.forEach((ele, ind) => {
          obj.rejected_registration_combine.amount += Number(ele.amt);
          obj.rejected_registration_combine.count += Number(ele.cnt);
          if (ele.asset_class == "LIQUID") {
            obj.rejected_registration_liquid.amount += Number(ele.amt);
            obj.rejected_registration_liquid.count += Number(ele.cnt);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.rejected_registration_nonliquid.amount += Number(ele.amt);
            obj.rejected_registration_nonliquid.count += Number(ele.cnt);
          }
        });
        dataObject.rejected_registration = obj;
      };
      const sent_to_aggregator_calculation = (rows, fund) => {
        let obj = {
          sent_to_aggregator_combine: {
            amount: 0,
            count: 0,
          },
          sent_to_aggregator_liquid: {
            amount: 0,
            count: 0,
          },
          sent_to_aggregator_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        // let licObj = {
        //   sent_to_aggregator_combine: {
        //     amount: 0,
        //     count: 0,
        //   },
        //   sent_to_aggregator_liquid: {
        //     amount: 0,
        //     count: 0,
        //   },
        //   sent_to_aggregator_nonliquid: {
        //     amount: 0,
        //     count: 0,
        //   },
        // };

        // if (fund == "102") {
        //   rows.forEach((ele, ind) => {
        //     licObj.sent_to_aggregator_combine.amount += Number(ele.amount);
        //     licObj.sent_to_aggregator_combine.count += Number(ele.trxns);
        //     if (ele.asset_class == "LIQUID") {
        //       licObj.sent_to_aggregator_liquid.amount += Number(ele.amount);
        //       licObj.sent_to_aggregator_liquid.count += Number(ele.trxns);
        //     }
        //     if (ele.asset_class == "NON LIQUID") {
        //       licObj.sent_to_aggregator_nonliquid.amount += Number(ele.amount);
        //       licObj.sent_to_aggregator_nonliquid.count += Number(ele.trxns);
        //     }
        //   });
        // } else {
          rows.forEach((ele, ind) => {
            obj.sent_to_aggregator_combine.amount += Number(ele.amount);
            obj.sent_to_aggregator_combine.count += Number(ele.trxns);
            if (ele.asset_class == "LIQUID") {
              obj.sent_to_aggregator_liquid.amount += Number(ele.amount);
              obj.sent_to_aggregator_liquid.count += Number(ele.trxns);
            }
            if (ele.asset_class == "NON LIQUID") {
              obj.sent_to_aggregator_nonliquid.amount += Number(ele.amount);
              obj.sent_to_aggregator_nonliquid.count += Number(ele.trxns);
            }
          });
        // }

        // if (fund == "102") {
        //   dataObject.sent_to_aggregator = licObj;
        // } else {
          dataObject.sent_to_aggregator = obj;
        // }
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "Reported_registration":
              Reported_registration_calculation(rows);
              break;
            case "successfully_registered":
              successfully_registered_calculation(rows);
              break;
            case "pending_registration":
              pending_registration_calculation(rows);
              break;
            case "rejected_registration":
              rejected_registration_calculation(rows);
              break;
            case "sent_to_aggregator":
              sent_to_aggregator_calculation(rows);
              break;

            case "sent_to_aggregator_lic":
              sent_to_aggregator_calculation(rows, fund);
              break;
            // case 'nigo_summary_report_liquid': handle_nigo_summary_data(rows, "liquid"); break;
            // case 'nigo_summary_report_nonliquid': handle_nigo_summary_data(rows, "nonliquid"); break;
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
        let redemptionQueries = await SIPQueries.findOne(
          { endpoint: "sipRegistrationJourney" },
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
        let lic_query = {
          name: "sent_to_aggregator_lic",
          query: `Select asset_class,count(distinct trxns) trxns,sum(amount) as amount
from (Select distinct asset_class,concat(srt_fund,srt_acno,srt_ihno) as trxns ,cast(srt_instlamt as decimal) as amount
	from lic.sipregistrationcompleted where substring(srt_regdate,1,10)='${transaction_date}' and srt_unitflag='Y'
	and exists (Select distinct * from lic.newmandateregistrationdetails_mirror nm where nm.parentihno=srt_ihno
and nm.folionumber=srt_acno)
	)x
group by 1
 `,
        };

        if (fund == "102") formattedQueries.push(lic_query);
        const all_results = await fetch_results(formattedQueries);

        await calculate_result(all_results);

        const cached_data = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "sip" },
          {
            $set: {
              date: transaction_date,
              "dataObject.reported_registration":
                dataObject.reported_registration,
              "dataObject.successfully_registered":
                dataObject.successfully_registered,
              "dataObject.pending_registration":
                dataObject.pending_registration,
              "dataObject.rejected_registration":
                dataObject.rejected_registration,
              "dataObject.sent_to_aggregator": dataObject.sent_to_aggregator,
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
    console.log("Error in sip registrationJourney provider ", e);
    throw new Error(e);
  }
};
