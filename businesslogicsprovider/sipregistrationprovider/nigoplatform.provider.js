import CachedData from "../../models/cacheddata.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import SipRegistrationQueries from "../../models/sipregistration.model.js";
import { getPoolObj } from "../../utils/pool.js";

export const nigo_platform_provider = async (
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
      "nigo_summary_combine",
      "nigo_summary_liquid",
      "nigo_summary_nonliquid",
      "platform_wise_combine",
      "platform_wise_liquid",
      "platform_wise_nonliquid",
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
        nigo_summary_combine: isDataPresent.dataObject.nigo_summary_combine,
        nigo_summary_liquid: isDataPresent.dataObject.nigo_summary_liquid,
        nigo_summary_nonliquid: isDataPresent.dataObject.nigo_summary_nonliquid,
        platform_wise_combine: isDataPresent.dataObject.platform_wise_combine,
        platform_wise_liquid: isDataPresent.dataObject.platform_wise_liquid,
        platform_wise_nonliquid:
          isDataPresent.dataObject.platform_wise_nonliquid,
      };
    } else {
      const platform_wise_calculation = (rows) => {
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

        let sortedByAmountCombine = obj.combine
          .slice()
          .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
        let sortedByCountCombine = obj.combine
          .slice()
          .sort((a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn));

        let sortedByAmountLiquid = obj.liquid
          .slice()
          .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
        let sortedByCountLiquid = obj.liquid
          .slice()
          .sort((a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn));

        let sortedByAmountNonLiquid = obj.nonliquid
          .slice()
          .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
        let sortedByCountNonLiquid = obj.nonliquid
          .slice()
          .sort((a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn));

        let platform_wise_combine = {
          sortedByAmount: sortedByAmountCombine,
          sortedByCount: sortedByCountCombine,
        };

        let platform_wise_liquid = {
          sortedByAmount: sortedByAmountLiquid,
          sortedByCount: sortedByCountLiquid,
        };

        let platform_wise_nonliquid = {
          sortedByAmount: sortedByAmountNonLiquid,
          sortedByCount: sortedByCountNonLiquid,
        };

        dataObject.platform_wise_combine = platform_wise_combine;
        dataObject.platform_wise_liquid = platform_wise_liquid;
        dataObject.platform_wise_nonliquid = platform_wise_nonliquid;
      };

      const nigo_summary_calculation = (rows) => {
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
        dataObject.nigo_summary_combine = obj.combine;
        dataObject.nigo_summary_liquid = obj.liquid;
        dataObject.nigo_summary_nonliquid = obj.nonliquid;
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "nigo_summary":
              nigo_summary_calculation(rows, "combine");
              break;
            case "Platform_wise_registration":
              platform_wise_calculation(rows, "nonliquid");
              break;
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
        let purchesQueries = await SipRegistrationQueries.findOne(
          { endpoint: "nigoplatform_registration" },
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
          { fund: fund, date: transaction_date, type: "sip_registration" },
          {
            $set: {
              date: transaction_date,
              "dataObject.platform_wise_combine":
                dataObject.platform_wise_combine,
              "dataObject.platform_wise_liquid": dataObject.platform_wise_liquid,
              "dataObject.platform_wise_nonliquid":
                dataObject.platform_wise_nonliquid,
              "dataObject.nigo_summary_combine":
                dataObject.nigo_summary_combine,
              "dataObject.nigo_summary_liquid": dataObject.nigo_summary_liquid,
              "dataObject.nigo_summary_nonliquid":
                dataObject.nigo_summary_nonliquid,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );

        return dataObject;
      };

      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log("Error sip bifurcation", e);
    throw new Error(e);
  }
};
