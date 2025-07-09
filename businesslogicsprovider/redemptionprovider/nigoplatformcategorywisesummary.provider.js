import OverViewMasterNew from "../../models/newoverview.model.js";
import CachedData from "../../models/cacheddata.model.js";
import PurchaseQueries from "../../models/purchasequeries.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import RedemptionQueries from "../../models/redemptionqueries.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolObj } from "../../utils/pool.js";

export const nigoPlatformCategoryWise = async (
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
      "platform_wise_liquid",
      "platform_wise_nonliquid",
      "platform_wise_combine",
      "nigo_summary_combine",
      "nigo_summary_liquid",
      "nigo_summary_nonliquid",
      "scheme_wise_transaction"
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
      return isDataPresent.dataObject;
    } else {
      const handle_nigo_summary_data = (rows) => {
        let obj = {
          nigo_summary_combine: [],
          nigo_summary_liquid: [],
          nigo_summary_nonliquid: [],
        };

        rows.forEach((ele, ind) => {
          obj.nigo_summary_combine.push(ele);
          if (ele.asset_class == "LIQUID") {
            obj.nigo_summary_liquid.push(ele);
          } else if (ele.asset_class == "NON LIQUID") {
            obj.nigo_summary_nonliquid.push(ele);
          }
        });

        dataObject.nigo_summary_combine = obj.nigo_summary_combine;
        dataObject.nigo_summary_liquid = obj.nigo_summary_liquid;
        dataObject.nigo_summary_nonliquid = obj.nigo_summary_nonliquid;
      };
      const platform_wise_transactions = (rows, asset_class) => {
        let sortedByAmount = rows
          .slice()
          .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
        let sortedByCount = rows
          .slice()
          .sort((a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn));

        let name = `platform_wise_${asset_class}`;

        dataObject[name] = {
          sortedByAmount: sortedByAmount,
          sortedByCount: sortedByCount,
        };

      };

      const handle_scheme_wise_transaction = (rows) => {
        const obj = {
          combine: [],
          liquid: [],
          nonLiquid: [],
        };
        
        // Helper function to aggregate data
        const aggregateData = (assetClass, row) => {
          const targetArray = assetClass === "NON LIQUID" ? obj.nonLiquid : obj.liquid;
          const existingScheme = targetArray.find(s => s.scheme_name === row.scheme_name);
        
          if (existingScheme) {
            // Update existing scheme
            existingScheme.reported.count += Number(row.count);
            existingScheme.reported.amount += Number(row.amount)
            
            if (row.status === "PENDING") {
              existingScheme.pending = {
                count: (existingScheme.pending?.count || 0) + Number(row.count),
                amount: (existingScheme.pending?.amount || 0) + Number(row.amount),
              };
            }
          } else {
            // Create new scheme entry
            const newScheme = {
              scheme_name: row.scheme_name,
              reported: {
                count: Number(row.count),
                amount: Number(row.amount),
              },
              pending: {
                count: 0,
                amount: 0
              }
            };
        
            if (row.status === "PENDING") {
              newScheme.pending.count += Number(row.count)
              newScheme.pending.amount += Number(row.amount)

            }
        
            targetArray.push(newScheme);
          }
        };
        
        // Process each row
        rows.forEach(row => {
          aggregateData(row.asset_class, row);
        });
        
        // Output the result
        obj.combine = [...obj.liquid, ...obj.nonLiquid];
        dataObject.scheme_wise_transaction = obj
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "platform_wise_nonliquid":
              platform_wise_transactions(rows, "nonliquid");
              break;
            case "platform_wise_liquid":
              platform_wise_transactions(rows, "liquid");
              break;
            case "platform_wise_combine":
              platform_wise_transactions(rows, "combine");
              break;
            case "nigo_summary":
              handle_nigo_summary_data(rows);
              break;
            case "scheme_wise_transaction":
              handle_scheme_wise_transaction(rows);
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
        let redemptionQueries = await RedemptionQueries.findOne(
          { endpoint: "NigoPlatformCategorywiseSummary" },
          { queriesArray: 1 }
        );
        let queriesArray = redemptionQueries.queriesArray;
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
          { fund: fund, date: transaction_date, type: "redemption" },
          {
            $set: {
              date: transaction_date,
              "dataObject.platform_wise_liquid":
                dataObject.platform_wise_liquid,
              "dataObject.platform_wise_nonliquid":
                dataObject.platform_wise_nonliquid,
              "dataObject.platform_wise_combine":
                dataObject.platform_wise_combine,
              "dataObject.nigo_summary_combine":
                dataObject.nigo_summary_combine,
              "dataObject.nigo_summary_liquid": dataObject.nigo_summary_liquid,
              "dataObject.nigo_summary_nonliquid":
                dataObject.nigo_summary_nonliquid,
              "dataObject.scheme_wise_transaction":
                dataObject.scheme_wise_transaction,
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
    console.log(
      "Error while fecthing nigoplatform wise summary  redemption",
      e
    );
    throw new Error(e);
  }
};
