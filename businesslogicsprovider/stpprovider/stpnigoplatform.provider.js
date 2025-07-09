import CachedData from "../../models/cacheddata.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import STPQueries from "../../models/stpqueries.model.js";
import { getPoolObj } from "../../utils/pool.js";

export const StpNigoPlatform = async (trdate, fundcode, snsstatus = false) => {
  try {
    let dataObject = {};
    let transaction_date = trdate;
    let fund = fundcode;
    let schema = getFundWiseSchema(fund);
    const pool = getPoolObj(fund);
    const keysToCheck = [
      "qc_completed_Object",
      "qc_pending_Object",
      "qc_objected_Object",
      "nigo_summary_combine",
      "nigo_summary_liquid",
      "nigo_summary_nonliquid",
      "platform_wise_transactions_combine",
      "platform_wise_transactions_liquid",
      "platform_wise_transactions_nonliquid",
      "scheme_wise_transaction"
    ];

    const isDataPresent = await CachedData.findOne({
      $and: [
        { fund: fund, date: transaction_date, type: "stp" },
        {
          $and: keysToCheck.map((key) => ({
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
        platform_wise_transactions_combine:
          isDataPresent.dataObject.platform_wise_transactions_combine,
        platform_wise_transactions_liquid:
          isDataPresent.dataObject.platform_wise_transactions_liquid,
        platform_wise_transactions_nonliquid:
          isDataPresent.dataObject.platform_wise_transactions_nonliquid,
        qc_objected_Object: isDataPresent.dataObject.qc_objected_Object,
        qc_pending_Object: isDataPresent.dataObject.qc_pending_Object,
        qc_completed_Object: isDataPresent.dataObject.qc_completed_Object,
      };
    } else {
      const obj = {
        qualityCheck: {
          inflow: {
            combine: 0,
            liquid: 0,
            nonLiquid: 0,
          },
          outflow: {
            combine: 0,
            liquid: 0,
            nonLiquid: 0,
          },
        },
      };

      const purchase_QC_Calculation = async (rows) => {
        let qc_completed_Object = {
          inflow: {
            liquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              pending: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
            nonLiquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              pending: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
          },
          outflow: {
            liquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              pending: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
            nonLiquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              pending: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
          },
        };

        let qc_pending_Object = {
          inflow: {
            liquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              pending: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
            nonLiquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              pending: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
          },
          outflow: {
            liquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              pending: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
            nonLiquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              pending: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
          },
        };

        let qc_objected_Object = {
          inflow: {
            liquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              pending: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
            nonLiquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              pending: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
          },
          outflow: {
            liquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              pending: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
            nonLiquid: {
              beyond_TAT: {
                amount: 0,
                sum: 0,
              },
              pending: {
                amount: 0,
                sum: 0,
              },
              within_TAT: {
                amount: 0,
                sum: 0,
              },
            },
          },
        };

        const rowsInflow = rows.filter((item) => item.flow == "Inflow");
        const rowsOutflow = rows.filter((item) => item.flow == "Outflow");

        rowsInflow.forEach((ele, ind) => {
          if (ele.asset_class == "NON LIQUID") {
            if (ele.status == "completed") {
              obj.qualityCheck.inflow.nonLiquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                qc_completed_Object.inflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                qc_completed_Object.inflow.nonLiquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                obj.qualityCheck.liquid = Number(ele.sum);
                qc_completed_Object.inflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                qc_completed_Object.inflow.nonLiquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                qc_pending_Object.inflow.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.inflow.nonLiquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                qc_pending_Object.inflow.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.inflow.nonLiquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                qc_objected_Object.inflow.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_objected_Object.inflow.nonLiquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                qc_objected_Object.inflow.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_objected_Object.inflow.nonLiquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            }
          } else {
            if (ele.status == "completed") {
              obj.qualityCheck.inflow.liquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                qc_completed_Object.inflow.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_completed_Object.inflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                qc_completed_Object.inflow.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_completed_Object.inflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                qc_pending_Object.inflow.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.inflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                qc_pending_Object.inflow.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.inflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                qc_objected_Object.inflow.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_objected_Object.inflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                qc_objected_Object.inflow.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_objected_Object.inflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            }
          }
        });
        rowsOutflow.forEach((ele, ind) => {
          if (ele.asset_class == "NON LIQUID") {
            if (ele.status == "completed") {
              obj.qualityCheck.outflow.nonLiquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                qc_completed_Object.outflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                qc_completed_Object.outflow.nonLiquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                obj.qualityCheck.liquid = Number(ele.sum);
                qc_completed_Object.outflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                qc_completed_Object.outflow.nonLiquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                qc_pending_Object.outflow.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.outflow.nonLiquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                qc_pending_Object.outflow.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.outflow.nonLiquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                qc_objected_Object.outflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                qc_objected_Object.outflow.nonLiquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                qc_objected_Object.outflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                qc_objected_Object.outflow.nonLiquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            }
          } else {
            if (ele.status == "completed") {
              obj.qualityCheck.outflow.liquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                qc_completed_Object.outflow.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_completed_Object.outflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                qc_completed_Object.outflow.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_completed_Object.outflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                qc_pending_Object.outflow.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.outflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                qc_pending_Object.outflow.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.outflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                qc_objected_Object.outflow.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_objected_Object.outflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                qc_objected_Object.outflow.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_objected_Object.outflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            }
          }
        });

        obj.qualityCheck.outflow.combine =
          obj.qualityCheck.outflow.liquid + obj.qualityCheck.outflow.nonLiquid;
        dataObject.qc_completed_Object = qc_completed_Object;
        dataObject.qc_pending_Object = qc_pending_Object;
        dataObject.qc_objected_Object = qc_objected_Object;
      };

      const platform_wise_transactions = (rows, asset_class) => {
        let sortedByAmountInflow = rows
          .slice()
          .filter((item) => item.flow === "Inflow")
          .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
        let sortedByAmountOutflow = rows
          .slice()
          .filter((item) => item.flow === "Outflow")
          .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
        let sortedByCountInflow = rows
          .slice()
          .filter((item) => item.flow === "Inflow")
          .sort((a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn));
        let sortedByCountOutflow = rows
          .slice()
          .filter((item) => item.flow === "Outflow")
          .sort((a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn));

        let name = `platformwise_${asset_class}`;

        dataObject[name] = {
          inflow: {
            sortedByAmount: sortedByAmountInflow,
            sortedByCount: sortedByCountInflow,
          },
          outflow: {
            sortedByAmount: sortedByAmountOutflow,
            sortedByCount: sortedByCountOutflow,
          },
        };
      };

      const handle_scheme_wise_transaction = (rows) => {
        let inflowRows = rows
          .slice()
          .filter((item) => item.flow === "Inflow")
        let outflowRows = rows
          .slice()
          .filter((item) => item.flow === "Outflow")
        const obj = {
          inflow: { combine: [], liquid: [], nonLiquid: [] },
          outflow: { combine: [], liquid: [], nonLiquid: [] }
        };

        // Helper function to aggregate data
        const aggregateData = (assetClass, row, filter) => {
          const targetArray = assetClass === "NON LIQUID" ? obj[filter].nonLiquid : obj[filter].liquid;
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
        inflowRows.forEach(row => {
          aggregateData(row.asset_class, row, "inflow");
        });

        outflowRows.forEach(row => {
          aggregateData(row.asset_class, row, "outflow");
        });

        // Output the result
        obj.inflow.combine = [...obj.inflow.liquid, ...obj.inflow.nonLiquid]
        obj.outflow.combine = [...obj.outflow.liquid, ...obj.outflow.nonLiquid]
        dataObject.scheme_wise_transaction = obj
      };

      const handle_nigo_summary_data = (rows, asset_class) => {
        let name = `nigo_summary_${asset_class}`;

        const nigoInflow = rows.filter((item) => item.flow === "Inflow");
        const nigoOutflow = rows.filter((item) => item.flow === "Outflow");

        dataObject[name] = {
          inflow: nigoInflow,
          outflow: nigoOutflow,
        };
        // const liquidArray = rows.filter(ele =>{ ele.asset_class == 'LIQUID'})
        // const non
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "platform_wise_transactions_combine":
              platform_wise_transactions(rows, "combine");
              break;
            case "platform_wise_transactions_nonliquid":
              platform_wise_transactions(rows, "nonliquid");
              break;
            case "platform_wise_transactions_liquid":
              platform_wise_transactions(rows, "liquid");
              break;
            case "scheme_wise_transaction":
              handle_scheme_wise_transaction(rows);
              break;
            case "nigo_summary_combine":
              handle_nigo_summary_data(rows, "combine");
              break;
            case "nigo_summary_liquid":
              handle_nigo_summary_data(rows, "liquid");
              break;
            case "nigo_summary_nonliquid":
              handle_nigo_summary_data(rows, "nonliquid");
              break;
            case "qc_summary":
              purchase_QC_Calculation(rows);
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
        let purchesQueries = await STPQueries.findOne(
          { endpoint: "NigoPlatformQCSummary" },
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
          { fund: fund, date: transaction_date, type: "stp" },
          {
            $set: {
              date: transaction_date,
              "dataObject.platform_wise_transactions_combine":
                dataObject.platformwise_combine,
              "dataObject.platform_wise_transactions_liquid":
                dataObject.platformwise_liquid,
              "dataObject.platform_wise_transactions_nonliquid":
                dataObject.platformwise_nonliquid,
              "dataObject.nigo_summary_combine":
                dataObject.nigo_summary_combine,
              "dataObject.nigo_summary_liquid": dataObject.nigo_summary_liquid,
              "dataObject.nigo_summary_nonliquid":
                dataObject.nigo_summary_nonliquid,
              "dataObject.qc_completed_Object": dataObject.qc_completed_Object,
              "dataObject.qc_pending_Object": dataObject.qc_pending_Object,
              "dataObject.qc_objected_Object": dataObject.qc_objected_Object,
              "dataObject.scheme_wise_transaction": dataObject.scheme_wise_transaction,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );
        return {
          nigo_summary_combine: cached_data.dataObject.nigo_summary_combine,
          nigo_summary_liquid: cached_data.dataObject.nigo_summary_liquid,
          nigo_summary_nonliquid: cached_data.dataObject.nigo_summary_nonliquid,
          platform_wise_transactions_combine:
            cached_data.dataObject.platform_wise_transactions_combine,
          platform_wise_transactions_liquid:
            cached_data.dataObject.platform_wise_transactions_liquid,
          platform_wise_transactions_nonliquid:
            cached_data.dataObject.platform_wise_transactions_nonliquid,
          qc_objected_Object: cached_data.dataObject.qc_objected_Object,
          qc_pending_Object: cached_data.dataObject.qc_pending_Object,
          qc_completed_Object: cached_data.dataObject.qc_completed_Object,
          scheme_wise_transaction: cached_data.dataObject.scheme_wise_transaction
        };
      };

      const res_obj = await main();
      return res_obj;
    }
  } catch (e) {
    console.log("Error in stpnigoplatform provider", e);
    throw new Error(e);
  }
};
