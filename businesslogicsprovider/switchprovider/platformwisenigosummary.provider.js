import SwitchQueries from "../../models/switch.model.js";
import CachedData from "../../models/cacheddata.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getPoolObj } from "../../utils/pool.js";

export const platform_nigo_summary_provider = async (
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
      "platform_wise_transactions_combine",
      "platform_wise_transactions_liquid",
      "platform_wise_transactions_nonliquid",
      "nigo_summary_combine",
      "nigo_summary_liquid",
      "nigo_summary_nonliquid",
      "funding_summary_transaction_date",
      "funding_summary_funding_date",
      "transaction_class_bifurcation",
      "scheme_wise_transaction"
    ];

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
      const platform_wise_transactions = (rows) => {
        let platform_wise_transactions_combine = { inflow: [], outflow: [] };
        let platform_wise_transactions_liquid = { inflow: [], outflow: [] };
        let platform_wise_transactions_nonliquid = { inflow: [], outflow: [] };

        const rows_liquid = rows.filter(
          (item) => item.asset_class === "LIQUID"
        );
        const rows_nonliquid = rows.filter(
          (item) => item.asset_class === "NON LIQUID"
        );

        const calculation = (rows, asset_class) => {
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
            .sort(
              (a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn)
            );
          let sortedByCountOutflow = rows
            .slice()
            .filter((item) => item.flow === "Outflow")
            .sort(
              (a, b) => parseInt(b.count_of_txn) - parseInt(a.count_of_txn)
            );
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
        calculation(rows, "combine");
        calculation(rows_liquid, "liquid");
        calculation(rows_nonliquid, "nonliquid");
      };

      const handle_scheme_wise_transaction = (rows) => {
        let inflowRows = rows
          .filter((item) => item.flow === "Inflow")
        let outflowRows = rows
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

      const nigo_summary_calculation = (rows) => {
        let nigo_summary_combine = { inflow: [], outflow: [] };
        let nigo_summary_liquid = { inflow: [], outflow: [] };
        let nigo_summary_nonliquid = { inflow: [], outflow: [] };

        rows.forEach((ele, ind) => {
          if (ele.flow == "Inflow") {
            nigo_summary_combine.inflow.push(ele);
          } else {
            nigo_summary_combine.outflow.push(ele);
          }
          if (ele.asset_class == "LIQUID") {
            if (ele.flow == "Inflow") {
              nigo_summary_liquid.inflow.push(ele);
            } else {
              nigo_summary_liquid.outflow.push(ele);
            }
          } else {
            if (ele.flow == "Inflow") {
              nigo_summary_nonliquid.inflow.push(ele);
            } else {
              nigo_summary_nonliquid.outflow.push(ele);
            }
          }
        });

        dataObject["nigo_summary_combine"] = nigo_summary_combine;
        dataObject["nigo_summary_liquid"] = nigo_summary_liquid;
        dataObject["nigo_summary_nonliquid"] = nigo_summary_nonliquid;
      };

      const funding_summary_trdate = (rows) => {
        let obj = {
          combine: {
            liquid: [],
            debt: [],
            equity: [],
          },
          liquid: {
            liquid: [],
            debt: [],
            equity: [],
          },
          nonliquid: {
            liquid: [],
            debt: [],
            equity: [],
          },
        };
        rows.forEach((item) => {
          if (item.fscheme === "LIQUID") obj.combine.liquid.push(item);
          if (item.fscheme === "DEBT") obj.combine.debt.push(item);
          if (item.fscheme === "EQUITY") obj.combine.equity.push(item);

          if (item.asset_class === "LIQUID") {
            if (item.fscheme === "LIQUID") obj.liquid.liquid.push(item);
            if (item.fscheme === "DEBT") obj.liquid.debt.push(item);
            if (item.fscheme === "EQUITY") obj.liquid.equity.push(item);
          } else if (item.asset_class === "NON LIQUID") {
            if (item.fscheme === "LIQUID") obj.nonliquid.liquid.push(item);
            if (item.fscheme === "DEBT") obj.nonliquid.debt.push(item);
            if (item.fscheme === "EQUITY") obj.nonliquid.equity.push(item);
          }
        });
        dataObject["funding_summary_transaction_date"] = obj;
      };

      const funding_summary_funding_date = (rows) => {
        let obj = {
          combine: {
            liquid: [],
            debt: [],
            equity: [],
          },
          liquid: {
            liquid: [],
            debt: [],
            equity: [],
          },
          nonliquid: {
            liquid: [],
            debt: [],
            equity: [],
          },
        };
        rows.forEach((item) => {
          if (item.fscheme === "LIQUID") obj.combine.liquid.push(item);
          if (item.fscheme === "DEBT") obj.combine.debt.push(item);
          if (item.fscheme === "EQUITY") obj.combine.equity.push(item);

          if (item.asset_class === "LIQUID") {
            if (item.fscheme === "LIQUID") obj.liquid.liquid.push(item);
            if (item.fscheme === "DEBT") obj.liquid.debt.push(item);
            if (item.fscheme === "EQUITY") obj.liquid.equity.push(item);
          } else if (item.asset_class === "NON LIQUID") {
            if (item.fscheme === "LIQUID") obj.nonliquid.liquid.push(item);
            if (item.fscheme === "DEBT") obj.nonliquid.debt.push(item);
            if (item.fscheme === "EQUITY") obj.nonliquid.equity.push(item);
          }
        });
        dataObject["funding_summary_funding_date"] = obj;
      };

      const typewise_transactions = (rows) => {
        let transactionClassBifurcation = {
          finalBatchClosed: {
            inflow: {
              liquid: {
                SWIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SWIA: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTIA: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
              nonliquid: {
                SWIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SWIA: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTIA: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
              combine: {
                SWIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SWIA: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTIA: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
            },
            outflow: {
              liquid: {
                SWOP: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SWOF: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTOP: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTOF: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
              nonliquid: {
                SWOP: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SWOF: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTOP: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTOF: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
              combine: {
                SWOP: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SWOF: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTOP: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTOF: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
            },
          },
          nonFinalBatchClosed: {
            inflow: {
              liquid: {
                SWIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SWIA: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTIA: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
              nonliquid: {
                SWIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SWIA: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTIA: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
              combine: {
                SWIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SWIA: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTIA: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTIN: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
            },
            outflow: {
              liquid: {
                SWOP: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SWOF: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTOP: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTOF: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
              nonliquid: {
                SWOP: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SWOF: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTOP: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTOF: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
              combine: {
                SWOP: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                SWOF: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTOP: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
                LTOF: {
                  withinTAT: 0,
                  beyondTAT: 0,
                },
              },
            },
          }
        };

        const rowsInflow = rows.filter((item) => item.flow === "Inflow");
        const rowsOutflow = rows.filter((item) => item.flow === "Outflow");

        rowsInflow.forEach((item) => {
          const assetClassKey =
            item.asset_class === "LIQUID" ? "liquid" : "nonliquid";
          const trType = item.trtype;
          const caseType = item.tat !== 'within tat' ? "beyond TAT" : "with in TAT";
          const sum = Number(item.sum);
          const status = item.status === "Batchclosed" ? 'finalBatchClosed' : 'nonFinalBatchClosed'

          // Update the total sum for the transaction type
          transactionClassBifurcation[status]["inflow"][assetClassKey][trType].total +=
            sum;

          // Update withinTAT or beyondTAT based on the case type
          if (caseType == "with in TAT") {
            transactionClassBifurcation[status]["inflow"][assetClassKey][
              trType
            ].withinTAT = sum;
          } else if (caseType == "beyond TAT") {
            transactionClassBifurcation[status]["inflow"][assetClassKey][
              trType
            ].beyondTAT = sum;
          }
        });
        rowsOutflow.forEach((item) => {
          const assetClassKey =
            item.asset_class === "LIQUID" ? "liquid" : "nonliquid";
          const trType = item.trtype;
          const caseType = item.tat !== 'within tat' ? "beyond TAT" : "with in TAT";
          const sum = Number(item.sum);
          const status = item.status === "Batchclosed" ? 'finalBatchClosed' : 'nonFinalBatchClosed'

          // Update the total sum for the transaction type
          transactionClassBifurcation[status]["outflow"][assetClassKey][trType].total +=
            sum;

          // Update withinTAT or beyondTAT based on the case type
          if (caseType == "with in TAT") {
            transactionClassBifurcation[status]["outflow"][assetClassKey][
              trType
            ].withinTAT = sum;
          } else if (caseType == "beyond TAT") {
            transactionClassBifurcation[status]["outflow"][assetClassKey][
              trType
            ].beyondTAT = sum;
          }
        });
        transactionClassBifurcation.finalBatchClosed.inflow.combine.SWIN.withinTAT =
          transactionClassBifurcation.finalBatchClosed.inflow.liquid.SWIN.withinTAT +
          transactionClassBifurcation.finalBatchClosed.inflow.nonliquid.SWIN.withinTAT;
        transactionClassBifurcation.finalBatchClosed.inflow.combine.SWIN.beyondTAT =
          transactionClassBifurcation.finalBatchClosed.inflow.liquid.SWIN.beyondTAT +
          transactionClassBifurcation.finalBatchClosed.inflow.nonliquid.SWIN.beyondTAT;
        transactionClassBifurcation.finalBatchClosed.inflow.combine.SWIA.withinTAT =
          transactionClassBifurcation.finalBatchClosed.inflow.liquid.SWIA.withinTAT +
          transactionClassBifurcation.finalBatchClosed.inflow.nonliquid.SWIA.withinTAT;
        transactionClassBifurcation.finalBatchClosed.inflow.combine.SWIA.beyondTAT =
          transactionClassBifurcation.finalBatchClosed.inflow.liquid.SWIA.beyondTAT +
          transactionClassBifurcation.finalBatchClosed.inflow.nonliquid.SWIA.beyondTAT;
        transactionClassBifurcation.finalBatchClosed.inflow.combine.LTIA.withinTAT =
          transactionClassBifurcation.finalBatchClosed.inflow.liquid.LTIA.withinTAT +
          transactionClassBifurcation.finalBatchClosed.inflow.nonliquid.LTIA.withinTAT;
        transactionClassBifurcation.finalBatchClosed.inflow.combine.LTIA.beyondTAT =
          transactionClassBifurcation.finalBatchClosed.inflow.liquid.LTIA.beyondTAT +
          transactionClassBifurcation.finalBatchClosed.inflow.nonliquid.LTIA.beyondTAT;
        transactionClassBifurcation.finalBatchClosed.inflow.combine.LTIN.withinTAT =
          transactionClassBifurcation.finalBatchClosed.inflow.liquid.LTIN.withinTAT +
          transactionClassBifurcation.finalBatchClosed.inflow.nonliquid.LTIN.withinTAT;
        transactionClassBifurcation.finalBatchClosed.inflow.combine.LTIN.beyondTAT =
          transactionClassBifurcation.finalBatchClosed.inflow.liquid.LTIN.beyondTAT +
          transactionClassBifurcation.finalBatchClosed.inflow.nonliquid.LTIN.beyondTAT;

        transactionClassBifurcation.finalBatchClosed.outflow.combine.SWOP.withinTAT =
          transactionClassBifurcation.finalBatchClosed.outflow.liquid.SWOP.withinTAT +
          transactionClassBifurcation.finalBatchClosed.outflow.nonliquid.SWOP.withinTAT;
        transactionClassBifurcation.finalBatchClosed.outflow.combine.SWOP.beyondTAT =
          transactionClassBifurcation.finalBatchClosed.outflow.liquid.SWOP.beyondTAT +
          transactionClassBifurcation.finalBatchClosed.outflow.nonliquid.SWOP.beyondTAT;
        transactionClassBifurcation.finalBatchClosed.outflow.combine.SWOF.withinTAT =
          transactionClassBifurcation.finalBatchClosed.outflow.liquid.SWOF.withinTAT +
          transactionClassBifurcation.finalBatchClosed.outflow.nonliquid.SWOF.withinTAT;
        transactionClassBifurcation.finalBatchClosed.outflow.combine.SWOF.beyondTAT =
          transactionClassBifurcation.finalBatchClosed.outflow.liquid.SWOF.beyondTAT +
          transactionClassBifurcation.finalBatchClosed.outflow.nonliquid.SWOF.beyondTAT;
        transactionClassBifurcation.finalBatchClosed.outflow.combine.LTOP.withinTAT =
          transactionClassBifurcation.finalBatchClosed.outflow.liquid.LTOP.withinTAT +
          transactionClassBifurcation.finalBatchClosed.outflow.nonliquid.LTOP.withinTAT;
        transactionClassBifurcation.finalBatchClosed.outflow.combine.LTOP.beyondTAT =
          transactionClassBifurcation.finalBatchClosed.outflow.liquid.LTOP.beyondTAT +
          transactionClassBifurcation.finalBatchClosed.outflow.nonliquid.LTOP.beyondTAT;
        transactionClassBifurcation.finalBatchClosed.outflow.combine.LTOF.withinTAT =
          transactionClassBifurcation.finalBatchClosed.outflow.liquid.LTOF.withinTAT +
          transactionClassBifurcation.finalBatchClosed.outflow.nonliquid.LTOF.withinTAT;
        transactionClassBifurcation.finalBatchClosed.outflow.combine.LTOF.beyondTAT =
          transactionClassBifurcation.finalBatchClosed.outflow.liquid.LTOF.beyondTAT +
          transactionClassBifurcation.finalBatchClosed.outflow.nonliquid.LTOF.beyondTAT;

        transactionClassBifurcation.nonFinalBatchClosed.inflow.combine.SWIN.withinTAT =
          transactionClassBifurcation.nonFinalBatchClosed.inflow.liquid.SWIN.withinTAT +
          transactionClassBifurcation.nonFinalBatchClosed.inflow.nonliquid.SWIN.withinTAT;
        transactionClassBifurcation.nonFinalBatchClosed.inflow.combine.SWIN.beyondTAT =
          transactionClassBifurcation.nonFinalBatchClosed.inflow.liquid.SWIN.beyondTAT +
          transactionClassBifurcation.nonFinalBatchClosed.inflow.nonliquid.SWIN.beyondTAT;
        transactionClassBifurcation.nonFinalBatchClosed.inflow.combine.SWIA.withinTAT =
          transactionClassBifurcation.nonFinalBatchClosed.inflow.liquid.SWIA.withinTAT +
          transactionClassBifurcation.nonFinalBatchClosed.inflow.nonliquid.SWIA.withinTAT;
        transactionClassBifurcation.nonFinalBatchClosed.inflow.combine.SWIA.beyondTAT =
          transactionClassBifurcation.nonFinalBatchClosed.inflow.liquid.SWIA.beyondTAT +
          transactionClassBifurcation.nonFinalBatchClosed.inflow.nonliquid.SWIA.beyondTAT;
        transactionClassBifurcation.nonFinalBatchClosed.inflow.combine.LTIA.withinTAT =
          transactionClassBifurcation.nonFinalBatchClosed.inflow.liquid.LTIA.withinTAT +
          transactionClassBifurcation.nonFinalBatchClosed.inflow.nonliquid.LTIA.withinTAT;
        transactionClassBifurcation.nonFinalBatchClosed.inflow.combine.LTIA.beyondTAT =
          transactionClassBifurcation.nonFinalBatchClosed.inflow.liquid.LTIA.beyondTAT +
          transactionClassBifurcation.nonFinalBatchClosed.inflow.nonliquid.LTIA.beyondTAT;
        transactionClassBifurcation.nonFinalBatchClosed.inflow.combine.LTIN.withinTAT =
          transactionClassBifurcation.nonFinalBatchClosed.inflow.liquid.LTIN.withinTAT +
          transactionClassBifurcation.nonFinalBatchClosed.inflow.nonliquid.LTIN.withinTAT;
        transactionClassBifurcation.nonFinalBatchClosed.inflow.combine.LTIN.beyondTAT =
          transactionClassBifurcation.nonFinalBatchClosed.inflow.liquid.LTIN.beyondTAT +
          transactionClassBifurcation.nonFinalBatchClosed.inflow.nonliquid.LTIN.beyondTAT;

        transactionClassBifurcation.nonFinalBatchClosed.outflow.combine.SWOP.withinTAT =
          transactionClassBifurcation.nonFinalBatchClosed.outflow.liquid.SWOP.withinTAT +
          transactionClassBifurcation.nonFinalBatchClosed.outflow.nonliquid.SWOP.withinTAT;
        transactionClassBifurcation.nonFinalBatchClosed.outflow.combine.SWOP.beyondTAT =
          transactionClassBifurcation.nonFinalBatchClosed.outflow.liquid.SWOP.beyondTAT +
          transactionClassBifurcation.nonFinalBatchClosed.outflow.nonliquid.SWOP.beyondTAT;
        transactionClassBifurcation.nonFinalBatchClosed.outflow.combine.SWOF.withinTAT =
          transactionClassBifurcation.nonFinalBatchClosed.outflow.liquid.SWOF.withinTAT +
          transactionClassBifurcation.nonFinalBatchClosed.outflow.nonliquid.SWOF.withinTAT;
        transactionClassBifurcation.nonFinalBatchClosed.outflow.combine.SWOF.beyondTAT =
          transactionClassBifurcation.nonFinalBatchClosed.outflow.liquid.SWOF.beyondTAT +
          transactionClassBifurcation.nonFinalBatchClosed.outflow.nonliquid.SWOF.beyondTAT;
        transactionClassBifurcation.nonFinalBatchClosed.outflow.combine.LTOP.withinTAT =
          transactionClassBifurcation.nonFinalBatchClosed.outflow.liquid.LTOP.withinTAT +
          transactionClassBifurcation.nonFinalBatchClosed.outflow.nonliquid.LTOP.withinTAT;
        transactionClassBifurcation.nonFinalBatchClosed.outflow.combine.LTOP.beyondTAT =
          transactionClassBifurcation.nonFinalBatchClosed.outflow.liquid.LTOP.beyondTAT +
          transactionClassBifurcation.nonFinalBatchClosed.outflow.nonliquid.LTOP.beyondTAT;
        transactionClassBifurcation.nonFinalBatchClosed.outflow.combine.LTOF.withinTAT =
          transactionClassBifurcation.nonFinalBatchClosed.outflow.liquid.LTOF.withinTAT +
          transactionClassBifurcation.nonFinalBatchClosed.outflow.nonliquid.LTOF.withinTAT;
        transactionClassBifurcation.nonFinalBatchClosed.outflow.combine.LTOF.beyondTAT =
          transactionClassBifurcation.nonFinalBatchClosed.outflow.liquid.LTOF.beyondTAT +
          transactionClassBifurcation.nonFinalBatchClosed.outflow.nonliquid.LTOF.beyondTAT;

        dataObject.transaction_class_bifurcation = transactionClassBifurcation;
      };
      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "platform_wise_transactions":
              platform_wise_transactions(rows);
              break;
            case "scheme_wise_transaction":
              handle_scheme_wise_transaction(rows);
              break;
            case "nigo_summary":
              nigo_summary_calculation(rows);
              break;
            case "funding_summary_trdate":
              funding_summary_trdate(rows);
              break;
            case "funding_summary_fundingdate":
              funding_summary_funding_date(rows);
              break;
            case "typewise_transactions":
              typewise_transactions(rows);
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
          { endpoint: "PlatformNigoSummary" },
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
              "dataObject.funding_summary_transaction_date":
                dataObject.funding_summary_transaction_date,
              "dataObject.funding_summary_funding_date":
                dataObject.funding_summary_funding_date,
              "dataObject.transaction_class_bifurcation":
                dataObject.transaction_class_bifurcation,
              "dataObject.scheme_wise_transaction": dataObject.scheme_wise_transaction,
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
    console.log("Error in platfromwise nigo summary provider in switch", e);
    throw new Error(e);
  }
};
