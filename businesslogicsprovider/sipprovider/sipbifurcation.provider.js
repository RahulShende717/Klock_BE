import CachedData from "../../models/cacheddata.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import SIPQueries from "../../models/sipqueries.model.js";
import { getPoolObj } from "../../utils/pool.js";

export const SIPBifurcation = async (trdate, fundcode, snsstatus = false) => {
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
      "scheme_wise_transaction",
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
        scheme_wise_transaction:
          isDataPresent.dataObject.scheme_wise_transaction,
      };
    } else {
      const handle_typewise_bifurcation = (rows) => {
        let transactionClassBifurcation = {
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
        };

        rows.forEach((item) => {
          const assetClassKey =
            item.asset_class === "LIQUID" ? "liquid" : "nonliquid";
          const trType = item.trtype;
          const caseType = item.case;
          const sum = Number(item.sum);

          // Update the total sum for the transaction type
          transactionClassBifurcation[assetClassKey][trType].total += sum;

          // Update withinTAT or beyondTAT based on the case type
          if (caseType == "with in TAT") {
            transactionClassBifurcation[assetClassKey][trType].withinTAT = sum;
          } else if (caseType == "beyond TAT") {
            transactionClassBifurcation[assetClassKey][trType].beyondTAT = sum;
          }
        });

        transactionClassBifurcation.combine.ADD.withinTAT =
          transactionClassBifurcation.liquid.ADD.withinTAT +
          transactionClassBifurcation.nonliquid.ADD.withinTAT;
        transactionClassBifurcation.combine.ADD.beyondTAT =
          transactionClassBifurcation.liquid.ADD.beyondTAT +
          transactionClassBifurcation.nonliquid.ADD.beyondTAT;
        transactionClassBifurcation.combine.IPO.withinTAT =
          transactionClassBifurcation.liquid.IPO.withinTAT +
          transactionClassBifurcation.nonliquid.IPO.withinTAT;
        transactionClassBifurcation.combine.IPO.beyondTAT =
          transactionClassBifurcation.liquid.IPO.beyondTAT +
          transactionClassBifurcation.nonliquid.IPO.beyondTAT;
        transactionClassBifurcation.combine.NEW.withinTAT =
          transactionClassBifurcation.liquid.NEW.withinTAT +
          transactionClassBifurcation.nonliquid.NEW.withinTAT;
        transactionClassBifurcation.combine.NEW.beyondTAT =
          transactionClassBifurcation.liquid.NEW.beyondTAT +
          transactionClassBifurcation.nonliquid.NEW.beyondTAT;
        transactionClassBifurcation.combine.SIN.withinTAT =
          transactionClassBifurcation.liquid.SIN.withinTAT +
          transactionClassBifurcation.nonliquid.SIN.withinTAT;
        transactionClassBifurcation.combine.SIN.beyondTAT =
          transactionClassBifurcation.liquid.SIN.beyondTAT +
          transactionClassBifurcation.nonliquid.SIN.beyondTAT;

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

      const handle_nigo_summary_data = (rows, asset_class) => {
        let name = `nigo_summary_${asset_class}`;

        dataObject[name] = rows;
        // const liquidArray = rows.filter(ele =>{ ele.asset_class == 'LIQUID'})
        // const non
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
        let purchesQueries = await SIPQueries.findOne(
          { endpoint: "PurchaseBifurcation" },
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
          { fund: fund, date: transaction_date, type: "sip" },
          {
            $set: {
              date: transaction_date,
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
          platform_wise_combine: cached_data.dataObject.platform_wise_combine,
          platform_wise_liquid: cached_data.dataObject.platform_wise_liquid,
          platform_wise_nonliquid:
            cached_data.dataObject.platform_wise_nonliquid,
          scheme_wise_transaction:
            cached_data.dataObject.scheme_wise_transaction,
        };
      };

      const obj = await main();
      return obj;
    }
  } catch (e) {
    console.log("Error sip bifurcation", e);
    throw new Error(e);
  }
};
