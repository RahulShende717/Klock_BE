import OverViewMasterNew from "../../models/newoverview.model.js";
import CachedData from "../../models/cacheddata.model.js";
import PurchaseQueries from "../../models/purchasequeries.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getPoolObj } from "../../utils/pool.js";
import RedemptionQueries from "../../models/redemptionqueries.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import moment from "moment";

export const fundDetailsVerificationStatus = async (
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
    const keysToCheck = [
      "fund_details_bank_wise",
      "category_wise_summary_funding_date",
      "category_wise_summary_transaction_date",
    ];
    let schema = getFundWiseSchema(fund);
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
      const fund_details_bank_wise_calculation = (rows) => {
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

      const category_wise_summary_transactionDate = (rows) => {
        let obj = {
          combine: [],
          liquid: [],
          nonLiquid: [],
        };

        rows.forEach((ele, ind) => {
          obj.combine.push(ele);
          if (ele.asset_class == "LIQUID") {
            obj.liquid.push(ele);
          } else if (ele.asset_class != "LIQUID") {
            obj.nonLiquid.push(ele);
          }
        });

        function groupByBankAndType(filteredData) {
          const summary = {};
      
          filteredData?.forEach((item, index) => {
            const key = `${item?.asset_class}-${item?.issuancemode}`;
            if (!summary[key]) {
              summary[key] = {
                FundType: item?.asset_class,
                Type: item?.issuancemode,
                Amount: 0,
                Count: 0,
                child: [],
              };
            }
            summary[key].Count += 1;
            summary[key].Amount += Number(item?.amount);
            summary[key].child.push({
              Scheme: item?.schemename,
              Amount: Number(item?.amount),
              IssuanceMode: item?.issuancemode,
              TransactionDate: moment(item?.trdate).format("DD-MM-YYYY"),
              Mode_of_Payment: item?.wartype,
              PaymentMode: item?.paymentmode !== null ? item?.paymentmode : "--",
              FundingDate:
                item?.fundingdate !== null
                  ? moment(item?.fundingdate).format("DD-MM-YYYY")
                  : "--",
            });
          });
      
          return Object.values(summary);
        }

        function renderSummaryTable(filteredData) {
          return groupByBankAndType(filteredData);
        }

        function aggregateFunc(data) {
          return data.map((item) => {
            item.child = item.child?.reduce((acc, item) => {
              
              const existingItem = acc.find((i) => i.Scheme === item.Scheme && i.PaymentMode === item.PaymentMode && i.FundingDate === item.FundingDate);
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
          combine: aggregateFunc(renderSummaryTable(obj.combine)),
            liquid: aggregateFunc(renderSummaryTable(obj.liquid)),
            nonLiquid: aggregateFunc(renderSummaryTable(obj.nonLiquid))
        }

        const sortByKeyDescending = (array, key) => {
          return array.sort((a, b) => b[key] - a[key]);
        };

        // Function to sort children by Amount
        const sortChildrenByAmount = (data) => {
          if (data.child) {
            data.child.sort((a, b) => b.Amount - a.Amount);
          }
        };

        const types = ["combine", "liquid", "nonLiquid"];

        types.forEach((type) => {
          if (dataAll[type]) {
            sortByKeyDescending(dataAll[type], "Count");
            dataAll[type].forEach(sortChildrenByAmount);
          }
        });
        dataObject.category_wise_summary_transaction_date = dataAll;
      };

      const category_wise_summary_fundingDate = (rows) => {
        let obj = {
          combine: [],
          liquid: [],
          nonLiquid: [],
        };

        rows.forEach((ele, ind) => {
          obj.combine.push(ele);
          if (ele.asset_class == "LIQUID") {
            obj.liquid.push(ele);
          } else if (ele.asset_class != "LIQUID") {
            obj.nonLiquid.push(ele);
          }
        });

        function groupByBankAndType(filteredData) {
          const summary = {};
      
          filteredData?.forEach((item, index) => {
            const key = `${item?.asset_class}-${item?.issuancemode}`;
            if (!summary[key]) {
              summary[key] = {
                FundType: item?.asset_class,
                Type: item?.issuancemode,
                Amount: 0,
                Count: 0,
                child: [],
              };
            }
            summary[key].Count += 1;
            summary[key].Amount += Number(item?.amount);
            summary[key].child.push({
              Scheme: item?.schemename,
              Amount: Number(item?.amount),
              IssuanceMode: item?.issuancemode,
              TransactionDate: moment(item?.trdate).format("DD-MM-YYYY"),
              Mode_of_Payment: item?.wartype,
              PaymentMode: item?.paymentmode !== null ? item?.paymentmode : "--",
              FundingDate:
                item?.fundingdate !== null
                  ? moment(item?.fundingdate).format("DD-MM-YYYY")
                  : "--",
            });
          });
      
          return Object.values(summary);
        }

        function renderSummaryTable(filteredData) {
          return groupByBankAndType(filteredData);
        }

        // dataObject.category_wise_summary_funding_date = renderSummaryTable(obj.liquid);

        function aggregateFunc(data) {
          return data.map((item) => {
            item.child = item.child?.reduce((acc, item) => {
              
              const existingItem = acc.find((i) => i.Scheme === item.Scheme && i.PaymentMode === item.PaymentMode && i.TransactionDate === item.TransactionDate);
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
          combine: aggregateFunc(renderSummaryTable(obj.combine)),
            liquid: aggregateFunc(renderSummaryTable(obj.liquid)),
            nonLiquid: aggregateFunc(renderSummaryTable(obj.nonLiquid))
        }

        const sortByKeyDescending = (array, key) => {
          return array.sort((a, b) => b[key] - a[key]);
        };

        // Function to sort children by Amount
        const sortChildrenByAmount = (data) => {
          if (data.child) {
            data.child.sort((a, b) => b.Amount - a.Amount);
          }
        };

        const types = ["combine", "liquid", "nonLiquid"];

        types.forEach((type) => {
          if (dataAll[type]) {
            sortByKeyDescending(dataAll[type], "Count");
            dataAll[type].forEach(sortChildrenByAmount);
          }
        });

        dataObject.category_wise_summary_funding_date = dataAll;
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "fund_details_bank_wise":
              fund_details_bank_wise_calculation(rows);
              break;
            case "category_wise_summary_funding_date":
              category_wise_summary_fundingDate(rows);
              break;
            case "category_wise_summary_transaction_date":
              category_wise_summary_transactionDate(rows);

            // case 'purchase_Endorsement': purchase_Endorsement_Calculation(rows); break;
            // case 'purchase_QualityCheck': purchase_QC_Calculation(rows); break;
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
          { endpoint: "FundDetailsVerificationStatus" },
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
          { fund: fund, date: transaction_date, type: "redemption" },
          {
            $set: {
              date: transaction_date,
              "dataObject.fund_details_bank_wise":
                dataObject.fund_details_bank_wise,
              "dataObject.category_wise_summary_funding_date":
                dataObject.category_wise_summary_funding_date,
              "dataObject.category_wise_summary_transaction_date":
                dataObject.category_wise_summary_transaction_date,
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
      "Error while fetching redemption verifcation status redemption",
      e
    );
    throw new Error(e);
  }
};
