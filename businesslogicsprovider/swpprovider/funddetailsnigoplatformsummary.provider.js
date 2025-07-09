import CachedData from "../../models/cacheddata.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import SwpQueries from "../../models/swpqueries.model.js";
import { getPoolObj } from "../../utils/pool.js";

export const funddetails_nigo_platform_summary_provider = async (
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
      "fund_details_bank_wise",
      "nigo_summary_combine",
      "nigo_summary_liquid",
      "nigo_summary_nonliquid",
      "platform_wise_combine",
      "platform_wise_liquid",
      "platform_wise_nonliquid",
      "fund_details",
      "scheme_wise_transaction"
    ];

    const isDataPresent = await CachedData.findOne({
      $and: [
        { fund: fund, date: transaction_date, type: "swp" },
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

      const funddetails_bankwise_calucation = (rows) => {
        let obj = {
          all: {
            combine: [],
            liquid: [],
            nonLiquid: [],
          },
          electronic: {
            combine: [],
            liquid: [],
            nonLiquid: [],
          },
          physical: {
            combine: [],
            liquid: [],
            nonLiquid: [],
          },
        };

        rows.forEach((ele, ind) => {
          obj.all.combine.push(ele);
          if (ele.type === "ELECTRONIC") {
            obj.electronic.combine.push(ele);
            if (ele.asset_class === "LIQUID") {
              obj.all.liquid.push(ele);
              obj.electronic.liquid.push(ele);
            } else if (ele.asset_class === "NON LIQUID") {
              obj.all.nonLiquid.push(ele);
              obj.electronic.nonLiquid.push(ele);
            }
          } else if (ele.type === "PHYSICAL") {
            obj.physical.combine.push(ele);
            if (ele.asset_class === "LIQUID") {
              obj.all.liquid.push(ele);
              obj.physical.liquid.push(ele);
            } else if (ele.asset_class === "NON LIQUID") {
              obj.all.nonLiquid.push(ele);
              obj.physical.nonLiquid.push(ele);
            }
          }
        });

        function groupByBankAndType(filteredData) {
          const summary = {};

          filteredData?.forEach((item) => {
            const key = `${item?.fundingbank}-${item?.type}`;
            if (!summary[key]) {
              summary[key] = {
                Name: item?.fundingbank,
                Type: item?.type,
                Amount: 0,
                Count: 0,
                child: [],
              };
            }
            summary[key].Count += 1;
            summary[key].Amount += Number(item?.dbamt);
            summary[key].child.push({
              Name: item?.scheme_name,
              Amount: Number(item?.dbamt),
              "Mode of Payment": item?.wartype,
            });
          });

          return Object.values(summary);
        }

        function renderSummaryTable(filteredData) {
          // const filteredData = filterData(asset_class);
          return groupByBankAndType(filteredData);
        }

        function aggregateFunc(data) {
          return data.map((item) => {
            item.child = item.child?.reduce((acc, item) => {
              const existingItem = acc.find((i) => i.Name === item.Name);
              if (existingItem) {
                existingItem.Amount = (
                  parseFloat(existingItem.Amount) + parseFloat(item.Amount)
                ).toFixed(2);
              } else {
                acc.push({ ...item });
              }
              return acc;
            }, []);

            return item;
          });
        }

        const dataAll = {
          all: {
            combine: aggregateFunc(renderSummaryTable(obj.all.combine)),
            liquid: aggregateFunc(renderSummaryTable(obj.all.liquid)),
            nonLiquid: aggregateFunc(renderSummaryTable(obj.all.nonLiquid)),
          },
          electronic: {
            combine: aggregateFunc(renderSummaryTable(obj.electronic.combine)),
            liquid: aggregateFunc(renderSummaryTable(obj.electronic.liquid)),
            nonLiquid: aggregateFunc(
              renderSummaryTable(obj.electronic.nonLiquid)
            ),
          },
          physical: {
            combine: aggregateFunc(renderSummaryTable(obj.physical.combine)),
            liquid: aggregateFunc(renderSummaryTable(obj.physical.liquid)),
            nonLiquid: aggregateFunc(
              renderSummaryTable(obj.physical.nonLiquid)
            ),
          },
        };

        const sortByKeyDescending = (array, key) => {
          return array.sort((a, b) => b[key] - a[key]);
        };

        // Function to sort children by Amount
        const sortChildrenByAmount = (data) => {
          if (data.child) {
            data.child.sort((a, b) => b.Amount - a.Amount);
          }
        };

        // Sort all categories
        const categories = ["all", "electronic", "physical"];
        const types = ["combine", "liquid", "nonLiquid"];

        // Sort each category and type
        categories.forEach((category) => {
          types.forEach((type) => {
            if (dataAll[category][type]) {
              sortByKeyDescending(dataAll[category][type], "Count");
              dataAll[category][type].forEach(sortChildrenByAmount);
            }
          });
        });
        dataObject.fund_details_bank_wise = dataAll;
      };

      const funding_details_calculation = (rows) => {
        let obj = {
          liquid: [],
          nonliquid: [],
          combine: [],
        };
        rows.forEach((ele, ind) => {
          if (ele.medium != "Other Cases") {
            obj.combine.push({
              name: ele.medium,
              tat: ele.tat,
              amount: ele.amount,
              count: ele.count,
            });
          }
          if (ele.asset_class == "LIQUID" && ele.medium != "Other Cases") {
            obj.liquid.push({
              name: ele.medium,
              tat: ele.tat,
              amount: ele.amount,
              count: ele.count,
            });
          }
          if (ele.asset_class == "NON LIQUID" && ele.medium != "Other Cases") {
            obj.nonliquid.push({
              name: ele.medium,
              tat: ele.tat,
              amount: ele.amount,
              count: ele.count,
            });
          }
        });

        dataObject.fund_details = obj;
      };
      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "nigo_summary":
              nigo_summary_calculation(rows);
              break;
            case "platform_wise_summary":
              platform_wise_calculation(rows);
              break;
            case "fund_details_bank_wise":
              funddetails_bankwise_calucation(rows);
              break;
            case "funding_details":
              funding_details_calculation(rows);
              break;
            case "scheme_wise_transaction":
              handle_scheme_wise_transaction(rows);
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
        let purchesQueries = await SwpQueries.findOne(
          { endpoint: "FundDetailsNigoPlatformWiseSummary" },
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
          { fund: fund, date: transaction_date, type: "swp" },
          {
            $set: {
              date: transaction_date,
              "dataObject.nigo_summary_combine":
                dataObject.nigo_summary_combine,
              "dataObject.nigo_summary_liquid": dataObject.nigo_summary_liquid,
              "dataObject.nigo_summary_nonliquid":
                dataObject.nigo_summary_nonliquid,
              "dataObject.platform_wise_combine":
                dataObject.platform_wise_combine,
              "dataObject.platform_wise_liquid":
                dataObject.platform_wise_liquid,
              "dataObject.platform_wise_nonliquid":
                dataObject.platform_wise_nonliquid,
              "dataObject.fund_details_bank_wise":
                dataObject.fund_details_bank_wise,
              "dataObject.fund_details": dataObject.fund_details,
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
      let res_obj = await main();
      return res_obj;
    }
  } catch (e) {
    console.log("Error in funddetails nigo summary provider swp ", e);
    throw new Error(e);
  }
};
