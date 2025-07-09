import SwitchQueries from "../../models/switch.model.js";
import CachedData from "../../models/cacheddata.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getPoolObj } from "../../utils/pool.js";

export const switchalerts_verificationstatus = async (
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
    const keysToCheck = ["alerts"];

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

      return isDataPresent.dataObject;
    } else {
      const switch_alerts = (rows) => {
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



        dataObject["alerts"] = obj;
      };
      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "switch_alerts":
              switch_alerts(rows);
              break;
            // case "endorsement_summary":
            //   endorsement_summary_calculation(rows);
            //   break;
          }
        });
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
          { endpoint: "AlertsVerificationStatus" },
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
          { fund: fund, date: transaction_date, type: "switch" },
          {
            $set: {
              date: transaction_date,
              "dataObject.alerts": dataObject.alerts,
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
    console.log("Error in switch alerts and verification status", e);
    throw new Error(e);
  }
};
