import PurchaseQueries from "../../models/purchasequeries.model.js";
import CachedData from "../../models/cacheddata.model.js";
import { getPoolObj } from "../../utils/pool.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";

export const purchaseBifurcation = async (
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
    if (pool == null) console.log("No Pool Connections For This AMC", fund);

    const keysToCheck = [
      "transaction_class_bifurcation",
      "nigo_summary_combine",
      "nigo_summary_liquid",
      "nigo_summary_nonliquid",
      "platform_wise_combine",
      "platform_wise_liquid",
      "platform_wise_nonliquid",
      "scheme_wise_transaction",
      // "refund_summary",
    ];

    const isDataPresent = await CachedData.findOne({
      $and: [
        { fund: fund, date: transaction_date, type: "purchase" },
        {
          $and: keysToCheck.map((key) => ({
            [`dataObject.${key}`]: { $exists: true },
          })),
        },
      ],
    });
    if (!snsstatus && isDataPresent != null) {
      return {
        transaction_class_bifurcation:
          isDataPresent.dataObject.transaction_class_bifurcation,
        nigo_summary_combine: isDataPresent.dataObject.nigo_summary_combine,
        nigo_summary_liquid: isDataPresent.dataObject.nigo_summary_liquid,
        nigo_summary_nonliquid: isDataPresent.dataObject.nigo_summary_nonliquid,
        platform_wise_combine: isDataPresent.dataObject.platform_wise_combine,
        platform_wise_liquid: isDataPresent.dataObject.platform_wise_liquid,
        platform_wise_nonliquid:
          isDataPresent.dataObject.platform_wise_nonliquid,
      };
    } else {
      const handle_typewise_bifurcation = (rows) => {
        let transactionClassBifurcation = {
          finalBatchClosed: {
            liquid: {
              ADD: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              NEW: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              SIN: {
                withinTAT: 0,
                beyondTAT: 0,
              },

              IPO: {
                withinTAT: 0,
                beyondTAT: 0,
              },
            },
            nonliquid: {
              ADD: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              NEW: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              SIN: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              IPO: {
                withinTAT: 0,
                beyondTAT: 0,
              },
            },
            combine: {
              ADD: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              NEW: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              SIN: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              IPO: {
                withinTAT: 0,
                beyondTAT: 0,
              },
            },
          },
          nonFinalBatchClosed: {
            liquid: {
              ADD: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              NEW: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              SIN: {
                withinTAT: 0,
                beyondTAT: 0,
              },

              IPO: {
                withinTAT: 0,
                beyondTAT: 0,
              },
            },
            nonliquid: {
              ADD: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              NEW: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              SIN: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              IPO: {
                withinTAT: 0,
                beyondTAT: 0,
              },
            },
            combine: {
              ADD: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              NEW: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              SIN: {
                withinTAT: 0,
                beyondTAT: 0,
              },
              IPO: {
                withinTAT: 0,
                beyondTAT: 0,
              },
            },
          },
        };

        rows.forEach((item) => {
          const assetClassKey =
            item.asset_class === "LIQUID" ? "liquid" : "nonliquid";
          const trType = item.trtype;
          const caseType = item.tat;
          const sum = Number(item.sum);
          const status =
            item.status === "Batchclosed"
              ? "finalBatchClosed"
              : "nonFinalBatchClosed";

          // Update the total sum for the transaction type
          transactionClassBifurcation[status][assetClassKey][trType].total +=
            sum;

          // Update withinTAT or beyondTAT based on the case type
          if (caseType == "within tat") {
            transactionClassBifurcation[status][assetClassKey][
              trType
            ].withinTAT = sum;
          } else if (caseType == "beyond tat") {
            transactionClassBifurcation[status][assetClassKey][
              trType
            ].beyondTAT = sum;
          }
        });

        transactionClassBifurcation.finalBatchClosed.combine.ADD.withinTAT =
          transactionClassBifurcation.finalBatchClosed.liquid.ADD.withinTAT +
          transactionClassBifurcation.finalBatchClosed.nonliquid.ADD.withinTAT;
        transactionClassBifurcation.finalBatchClosed.combine.ADD.beyondTAT =
          transactionClassBifurcation.finalBatchClosed.liquid.ADD.beyondTAT +
          transactionClassBifurcation.finalBatchClosed.nonliquid.ADD.beyondTAT;
        transactionClassBifurcation.finalBatchClosed.combine.IPO.withinTAT =
          transactionClassBifurcation.finalBatchClosed.liquid.IPO.withinTAT +
          transactionClassBifurcation.finalBatchClosed.nonliquid.IPO.withinTAT;
        transactionClassBifurcation.finalBatchClosed.combine.IPO.beyondTAT =
          transactionClassBifurcation.finalBatchClosed.liquid.IPO.beyondTAT +
          transactionClassBifurcation.finalBatchClosed.nonliquid.IPO.beyondTAT;
        transactionClassBifurcation.finalBatchClosed.combine.NEW.withinTAT =
          transactionClassBifurcation.finalBatchClosed.liquid.NEW.withinTAT +
          transactionClassBifurcation.finalBatchClosed.nonliquid.NEW.withinTAT;
        transactionClassBifurcation.finalBatchClosed.combine.NEW.beyondTAT =
          transactionClassBifurcation.finalBatchClosed.liquid.NEW.beyondTAT +
          transactionClassBifurcation.finalBatchClosed.nonliquid.NEW.beyondTAT;
        transactionClassBifurcation.finalBatchClosed.combine.SIN.withinTAT =
          transactionClassBifurcation.finalBatchClosed.liquid.SIN.withinTAT +
          transactionClassBifurcation.finalBatchClosed.nonliquid.SIN.withinTAT;
        transactionClassBifurcation.finalBatchClosed.combine.SIN.beyondTAT =
          transactionClassBifurcation.finalBatchClosed.liquid.SIN.beyondTAT +
          transactionClassBifurcation.finalBatchClosed.nonliquid.SIN.beyondTAT;

        transactionClassBifurcation.nonFinalBatchClosed.combine.ADD.withinTAT =
          transactionClassBifurcation.nonFinalBatchClosed.liquid.ADD.withinTAT +
          transactionClassBifurcation.nonFinalBatchClosed.nonliquid.ADD
            .withinTAT;
        transactionClassBifurcation.nonFinalBatchClosed.combine.ADD.beyondTAT =
          transactionClassBifurcation.nonFinalBatchClosed.liquid.ADD.beyondTAT +
          transactionClassBifurcation.nonFinalBatchClosed.nonliquid.ADD
            .beyondTAT;
        transactionClassBifurcation.nonFinalBatchClosed.combine.IPO.withinTAT =
          transactionClassBifurcation.nonFinalBatchClosed.liquid.IPO.withinTAT +
          transactionClassBifurcation.nonFinalBatchClosed.nonliquid.IPO
            .withinTAT;
        transactionClassBifurcation.nonFinalBatchClosed.combine.IPO.beyondTAT =
          transactionClassBifurcation.nonFinalBatchClosed.liquid.IPO.beyondTAT +
          transactionClassBifurcation.nonFinalBatchClosed.nonliquid.IPO
            .beyondTAT;
        transactionClassBifurcation.nonFinalBatchClosed.combine.NEW.withinTAT =
          transactionClassBifurcation.nonFinalBatchClosed.liquid.NEW.withinTAT +
          transactionClassBifurcation.nonFinalBatchClosed.nonliquid.NEW
            .withinTAT;
        transactionClassBifurcation.nonFinalBatchClosed.combine.NEW.beyondTAT =
          transactionClassBifurcation.nonFinalBatchClosed.liquid.NEW.beyondTAT +
          transactionClassBifurcation.nonFinalBatchClosed.nonliquid.NEW
            .beyondTAT;
        transactionClassBifurcation.nonFinalBatchClosed.combine.SIN.withinTAT =
          transactionClassBifurcation.nonFinalBatchClosed.liquid.SIN.withinTAT +
          transactionClassBifurcation.nonFinalBatchClosed.nonliquid.SIN
            .withinTAT;
        transactionClassBifurcation.nonFinalBatchClosed.combine.SIN.beyondTAT =
          transactionClassBifurcation.nonFinalBatchClosed.liquid.SIN.beyondTAT +
          transactionClassBifurcation.nonFinalBatchClosed.nonliquid.SIN
            .beyondTAT;

        dataObject.transaction_class_bifurcation = transactionClassBifurcation;
      };

      const platform_wise_transactions = (rows, asset_class) => {
        let sortedByAmount = rows
          .slice()
          .sort((a, b) => parseFloat(b.amount) - parseFloat(a.amount));
        let sortedByCount = rows
          .slice()
          .sort((a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn));

        let name = `platformwise_${asset_class}`;

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
          const targetArray =
            assetClass === "NON LIQUID" ? obj.nonLiquid : obj.liquid;
          const existingScheme = targetArray.find(
            (s) => s.scheme_name === row.scheme_name
          );

          if (existingScheme) {
            // Update existing scheme
            existingScheme.reported.count += Number(row.count);
            existingScheme.reported.amount += Number(row.amount);

            if (row.status === "PENDING") {
              existingScheme.pending = {
                count: (existingScheme.pending?.count || 0) + Number(row.count),
                amount:
                  (existingScheme.pending?.amount || 0) + Number(row.amount),
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
                amount: 0,
              },
            };

            if (row.status === "PENDING") {
              newScheme.pending.count += Number(row.count);
              newScheme.pending.amount += Number(row.amount);
            }

            targetArray.push(newScheme);
          }
        };

        // Process each row
        rows.forEach((row) => {
          aggregateData(row.asset_class, row);
        });

        // Output the result
        obj.combine = [...obj.liquid, ...obj.nonLiquid];
        dataObject.scheme_wise_transaction = obj;
      };
      const handle_nigo_summary_data = (rows, asset_class) => {
        let name = `nigo_summary_${asset_class}`;

        dataObject[name] = rows;
        // const liquidArray = rows.filter(ele =>{ ele.asset_class == 'LIQUID'})
        // const non
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "type_wise_purchase_transactions":
              handle_typewise_bifurcation(rows);
              break;
            case "platform_wise_transactions_combine":
              platform_wise_transactions(rows, "combine");
              break;
            case "platform_wise_transactions_nonliquid":
              platform_wise_transactions(rows, "nonliquid");
              break;
            case "platform_wise_transactions_liquid":
              platform_wise_transactions(rows, "liquid");
              break;
            case "nigo_summary_report_combine":
              handle_nigo_summary_data(rows, "combine");
              break;
            case "nigo_summary_report_liquid":
              handle_nigo_summary_data(rows, "liquid");
              break;
            case "nigo_summary_report_nonliquid":
              handle_nigo_summary_data(rows, "nonliquid");
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
            if (queryObj.name !== "Refund") {
              console.log("query", queryObj.name);
              const result = await pool.query(queryObj.query);
              return {
                name: queryObj.name,
                result: result.rows,
              };
            }
          })
        );
      };

      const main = async () => {
        let formattedQueries = [];
        let purchesQueries = await PurchaseQueries.findOne(
          { endpoint: "PurchaseBifurcation" },
          { queriesArray: 1 }
        );

        let queriesArray = purchesQueries.queriesArray;
        queriesArray.map((ele) => {
          if (ele.name !== "Refund") {
            let formattedQuery = ele.query.replace(
              /transactionDate/g,
              `${transaction_date}`
            );
            formattedQuery = formattedQuery.replace(/schema/g, `${schema}`);
            formattedQuery = formattedQuery.replace(/'fund'/g, `'${fund}'`);
            formattedQueries.push({ name: ele.name, query: formattedQuery });
          }
        });

        const all_results = await fetch_results(formattedQueries);

        await calculate_result(all_results);
        const cached_data = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "purchase" },
          {
            $set: {
              date: transaction_date,
              "dataObject.transaction_class_bifurcation":
                dataObject.transaction_class_bifurcation,
              "dataObject.platform_wise_combine":
                dataObject.platformwise_combine,
              "dataObject.platform_wise_liquid": dataObject.platformwise_liquid,
              "dataObject.platform_wise_nonliquid":
                dataObject.platformwise_nonliquid,
              "dataObject.nigo_summary_combine":
                dataObject.nigo_summary_combine,
              "dataObject.nigo_summary_liquid": dataObject.nigo_summary_liquid,
              "dataObject.nigo_summary_nonliquid":
                dataObject.nigo_summary_nonliquid,
              "dataObject.scheme_wise_transaction":
                dataObject.scheme_wise_transaction,
              "dataObject.refund": dataObject.refund,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );

        return {
          transaction_class_bifurcation:
            cached_data.dataObject.transaction_class_bifurcation,
          nigo_summary_combine: cached_data.dataObject.nigo_summary_combine,
          nigo_summary_liquid: cached_data.dataObject.nigo_summary_liquid,
          nigo_summary_nonliquid: cached_data.dataObject.nigo_summary_nonliquid,
          platform_wise_combine: cached_data.dataObject.platform_wise_combine,
          platform_wise_liquid: cached_data.dataObject.platform_wise_liquid,
          platform_wise_nonliquid:
            cached_data.dataObject.platform_wise_nonliquid,
          scheme_wise_transaction:
            cached_data.dataObject.scheme_wise_transaction,
          refund: cached_data.dataObject.refund,
        };
      };

      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log("Error in purchasebifurcation provider purchase", e);
    throw new Error(e);
  }
};
