import CachedData from "../../models/cacheddata.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import SIPQueries from "../../models/sipqueries.model.js";
import { getPoolObj } from "../../utils/pool.js";

const SIPSummearyData = async (trdate, fundcode, snsstatus = false) => {
  try {
    let dataObject = {};
    let transaction_date = trdate;
    let fund = fundcode;
    let schema = getFundWiseSchema(fund);
    const pool = getPoolObj(fund);

    const keysToCheck = [
      "posting_completed_Object",
      "posting_pending_Object",
      "posting_objected_Object",
      "qc_completed_Object",
      "qc_pending_Object",
      "qc_objected_Object",
      "purchase_Credit_Marked",
      "purchase_Credit_Pending",
      "radialchart",
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
      const obj = {
        creditmark: {
          combine: 0,
          liquid: 0,
          nonLiquid: 0,
        },
        endorsed: {
          combine: 0,
          liquid: 0,
          nonLiquid: 0,
        },
        qualityCheck: {
          combine: 0,
          liquid: 0,
          nonLiquid: 0,
        },
      };
      const purchase_creditMarked_Calculation = async (rows) => {
        let purchase_Credit_Marked = {
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
        };

        let purchase_Credit_Pending = {
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
        };
        rows.forEach((ele, ind) => {
          if (ele.asset_class == "NON LIQUID") {
            if (ele.status == "completed") {
              obj.creditmark.nonLiquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                purchase_Credit_Marked.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                purchase_Credit_Marked.nonLiquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                purchase_Credit_Marked.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                purchase_Credit_Marked.nonLiquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                purchase_Credit_Pending.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                purchase_Credit_Pending.nonLiquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                purchase_Credit_Pending.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                purchase_Credit_Pending.nonLiquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            }
          } else {
            if (ele.status == "completed") {
              obj.creditmark.liquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                purchase_Credit_Marked.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                purchase_Credit_Marked.liquid.within_TAT.sum += Number(ele.sum);
              } else {
                purchase_Credit_Marked.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                purchase_Credit_Marked.liquid.beyond_TAT.sum += Number(ele.sum);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                purchase_Credit_Pending.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                purchase_Credit_Pending.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                purchase_Credit_Pending.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                purchase_Credit_Pending.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            }
          }
        });
        obj.creditmark.combine =
          obj.creditmark.liquid + obj.creditmark.nonLiquid;
        dataObject.purchase_Credit_Marked = purchase_Credit_Marked;
        dataObject.purchase_Credit_Pending = purchase_Credit_Pending;
      };

      const purchase_QC_Calculation = async (rows) => {
        let qc_completed_Object = {
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
        };

        let qc_pending_Object = {
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
        };

        let qc_objected_Object = {
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
        };

        rows.forEach((ele, ind) => {
          if (ele.asset_class == "NON LIQUID") {
            if (ele.status == "completed") {
              obj.qualityCheck.nonLiquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                qc_completed_Object.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_completed_Object.nonLiquid.within_TAT.sum += Number(ele.sum);
              } else {
                obj.qualityCheck.liquid = Number(ele.sum);
                qc_completed_Object.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_completed_Object.nonLiquid.beyond_TAT.sum += Number(ele.sum);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                qc_pending_Object.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.nonLiquid.within_TAT.sum += Number(ele.sum);
              } else {
                qc_pending_Object.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.nonLiquid.beyond_TAT.sum += Number(ele.sum);
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                qc_objected_Object.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_objected_Object.nonLiquid.within_TAT.sum += Number(ele.sum);
              } else {
                qc_objected_Object.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_objected_Object.nonLiquid.beyond_TAT.sum += Number(ele.sum);
              }
            }
          } else {
            if (ele.status == "completed") {
              obj.qualityCheck.liquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                qc_completed_Object.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_completed_Object.liquid.within_TAT.sum += Number(ele.sum);
              } else {
                qc_completed_Object.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_completed_Object.liquid.beyond_TAT.sum += Number(ele.sum);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                qc_pending_Object.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.liquid.within_TAT.sum += Number(ele.sum);
              } else {
                qc_pending_Object.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.liquid.beyond_TAT.sum += Number(ele.sum);
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                qc_objected_Object.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_objected_Object.liquid.within_TAT.sum += Number(ele.sum);
              } else {
                qc_objected_Object.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_objected_Object.liquid.beyond_TAT.sum += Number(ele.sum);
              }
            }
          }
        });

        obj.qualityCheck.combine =
          obj.qualityCheck.liquid + obj.qualityCheck.nonLiquid;
        dataObject.qc_completed_Object = qc_completed_Object;
        dataObject.qc_pending_Object = qc_pending_Object;
        dataObject.qc_objected_Object = qc_objected_Object;
      };

      const purchase_Endorsement_Calculation = async (rows) => {
        let posting_completed_Object = {
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
        };

        let posting_pending_Object = {
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
        };

        let posting_objected_Object = {
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
        };
        rows.forEach((ele, ind) => {
          if (ele.asset_class == "NON LIQUID") {
            if (ele.status == "completed") {
              obj.endorsed.nonLiquid += Number(ele.count);
              if (ele.tat == "within TAT") {
                posting_completed_Object.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                posting_completed_Object.nonLiquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                posting_completed_Object.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                posting_completed_Object.nonLiquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                posting_pending_Object.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                posting_pending_Object.nonLiquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                posting_pending_Object.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                posting_pending_Object.nonLiquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                posting_objected_Object.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                posting_objected_Object.nonLiquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                posting_objected_Object.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                posting_objected_Object.nonLiquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            }
          } else {
            if (ele.status == "completed") {
              obj.endorsed.liquid += Number(ele.count);
              if (ele.tat == "within TAT") {
                posting_completed_Object.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                posting_completed_Object.liquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                posting_completed_Object.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                posting_completed_Object.liquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                posting_pending_Object.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                posting_pending_Object.liquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                posting_pending_Object.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                posting_pending_Object.liquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                posting_objected_Object.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                posting_objected_Object.liquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                posting_objected_Object.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                posting_objected_Object.liquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            }
          }
        });

        obj.endorsed.combine = obj.endorsed.liquid + obj.endorsed.nonLiquid;
        dataObject.posting_completed_Object = posting_completed_Object;
        dataObject.posting_pending_Object = posting_pending_Object;
        dataObject.posting_objected_Object = posting_objected_Object;
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "credit_Marked":
              purchase_creditMarked_Calculation(rows);
              break;
            case "purchase_Endorsement":
              purchase_Endorsement_Calculation(rows);
              break;
            case "purchase_QualityCheck":
              purchase_QC_Calculation(rows);
              break;
          }
        });
      };

      const fetch_results = async (queries) => {
        return Promise.all(
          queries.map(async (queryObj) => {
            // const result = await pool.query(queryObj.query);
            const startTime = Date.now()
              const result = await pool.query(queryObj.query);
              const endTime = Date.now()
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
          { endpoint: "SIPSummaryData" },
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

        dataObject.radialchart = obj; 
        const cached_data = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "sip" },
          {
            $set: {
              date: transaction_date,
              "dataObject.posting_completed_Object":
                dataObject.posting_completed_Object,
              "dataObject.posting_pending_Object":
                dataObject.posting_pending_Object,
              "dataObject.posting_objected_Object":
                dataObject.posting_objected_Object,
              "dataObject.qc_completed_Object": dataObject.qc_completed_Object,
              "dataObject.qc_pending_Object": dataObject.qc_pending_Object,
              "dataObject.qc_objected_Object": dataObject.qc_objected_Object,
              "dataObject.purchase_Credit_Marked":
                dataObject.purchase_Credit_Marked,
              "dataObject.purchase_Credit_Pending":
                dataObject.purchase_Credit_Pending,
              "dataObject.radialchart": dataObject.radialchart,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );
        return dataObject;
      };
      let res_obj = await main();
      return res_obj;
    }
  } catch (e) {
    console.log("Error in sipsummary provider ", e);
    throw new Error(e);
  }
};

export { SIPSummearyData };
