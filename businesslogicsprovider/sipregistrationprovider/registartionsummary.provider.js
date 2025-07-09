import CachedData from "../../models/cacheddata.model.js";
import SipRegistrationQueries from "../../models/sipregistration.model.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolObj } from "../../utils/pool.js";

const registration_summary_provider = async (
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
      "registration_completed_Object",
      "registration_pending_Object",
      "registration_rejected_Object",
      "qc_completed_Object",
      "qc_pending_Object",
      "qc_objected_Object",
      "dispatch_objected_Object",
      "dispatch_pending_Object",
      "dispatch_completed_Object",
      "radialchart",
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
        registration_completed_Object:
          isDataPresent.dataObject.registration_completed_Object,
        registration_pending_Object:
          isDataPresent.dataObject.registration_pending_Object,
        registration_rejected_Object:
          isDataPresent.dataObject.registration_rejected_Object,
        qc_completed_Object: isDataPresent.dataObject.qc_completed_Object,
        qc_pending_Object: isDataPresent.dataObject.qc_pending_Object,
        qc_objected_Object: isDataPresent.dataObject.qc_objected_Object,
        dispatch_completed_Object:
          isDataPresent.dataObject.dispatch_completed_Object,
        dispatch_Credit_Marked: isDataPresent.dataObject.dispatch_Credit_Marked,
        dispatch_Credit_Pending:
          isDataPresent.dataObject.dispatch_Credit_Pending,
        radialchart: isDataPresent.dataObject.radialchart,
        dispatch_completed_Object: isDataPresent.dataObject.dispatch_completed_Object,
        dispatch_objected_Object: isDataPresent.dataObject.dispatch_objected_Object,
        dispatch_pending_Object: isDataPresent.dataObject.dispatch_pending_Object,
      };
    } else {
      const obj = {
        dispatch: {
          combine: 0,
          liquid: 0,
          nonLiquid: 0,
        },
        registration: {
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
      const qc_tat_Calculation = async (rows) => {
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
          combine: {
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
          combine: {
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
          combine: {
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
              obj.qualityCheck.nonLiquid += Number(ele.count);
              if (ele.tat == "within TAT") {
                qc_completed_Object.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_completed_Object.nonLiquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                qc_completed_Object.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_completed_Object.nonLiquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                qc_pending_Object.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.nonLiquid.within_TAT.sum += Number(ele.count);
              } else {
                qc_pending_Object.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.nonLiquid.beyond_TAT.sum += Number(ele.count);
              }
            }
          } else {
            if (ele.status == "completed") {
              obj.qualityCheck.liquid += Number(ele.count);
              if (ele.tat == "within TAT") {
                qc_completed_Object.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_completed_Object.liquid.within_TAT.sum += Number(ele.count);
              } else {
                qc_completed_Object.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_completed_Object.liquid.beyond_TAT.sum += Number(ele.count);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                qc_pending_Object.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.liquid.within_TAT.sum += Number(ele.count);
              } else {
                qc_pending_Object.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                qc_pending_Object.liquid.beyond_TAT.sum += Number(ele.count);
              }
            }
          }
        });

        // qc_completed_Object.combine.beyond_TAT = qc_completed_Object.liquid.beyond_TAT + qc_completed_Object.nonLiquid.beyond_TAT
        // qc_objected_Object.combine.beyond_TAT = qc_objected_Object.combine
        obj.qualityCheck.combine =
          obj.qualityCheck.liquid + obj.qualityCheck.nonLiquid;
        dataObject.qc_completed_Object = qc_completed_Object;
        dataObject.qc_pending_Object = qc_pending_Object;
        dataObject.qc_objected_Object = qc_objected_Object;
      };

      const registration_tat_Calculation = async (rows) => {
        let registration_completed_Object = {
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

        let registration_pending_Object = {
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
          combine: {
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

        let registration_rejected_Object = {
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
          combine: {
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
              obj.registration.nonLiquid += Number(ele.count);
              if (ele.tat == "within TAT") {
                registration_completed_Object.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                registration_completed_Object.nonLiquid.within_TAT.sum +=
                  Number(ele.count);
              } else {
                registration_completed_Object.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                registration_completed_Object.nonLiquid.beyond_TAT.sum +=
                  Number(ele.count);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                registration_pending_Object.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                registration_pending_Object.nonLiquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                registration_pending_Object.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                registration_pending_Object.nonLiquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            } else if (ele.status == "rejected") {
              if (ele.tat == "within TAT") {
                registration_rejected_Object.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                registration_rejected_Object.nonLiquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                registration_rejected_Object.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                registration_rejected_Object.nonLiquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            }
          } else {
            if (ele.status == "completed") {
              obj.registration.liquid += Number(ele.count);
              if (ele.tat == "within TAT") {
                registration_completed_Object.liquid.within_TAT.amount +=
                  Number(ele.amount);
                registration_completed_Object.liquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                registration_completed_Object.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                registration_completed_Object.liquid.beyond_TAT.sum += Number(
                  ele.count
                );

              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                registration_pending_Object.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                registration_pending_Object.liquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                registration_pending_Object.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                registration_pending_Object.liquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            } else if (ele.status == "rejected") {
              if (ele.tat == "within TAT") {
                registration_rejected_Object.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                registration_rejected_Object.nonLiquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                registration_rejected_Object.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                registration_rejected_Object.nonLiquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            }
          }
        });

        obj.registration.combine =
          obj.registration.liquid + obj.registration.nonLiquid;
        dataObject.registration_completed_Object =
          registration_completed_Object;
        dataObject.registration_pending_Object = registration_pending_Object;
        dataObject.registration_rejected_Object = registration_rejected_Object;
      };

      const dispatch_tat_Calculation = (rows) => {
        let dispatch_completed_Object = {
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
          combine: {
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

        let dispatch_pending_Object = {
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
          combine: {
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

        let dispatch_objected_Object = {
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
          combine: {
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
              obj.dispatch.nonLiquid += Number(ele.count);
              if (ele.tat == "within TAT") {
                dispatch_completed_Object.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                dispatch_completed_Object.nonLiquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                dispatch_completed_Object.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                dispatch_completed_Object.nonLiquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                dispatch_pending_Object.nonLiquid.within_TAT.amount += Number(
                  ele.amount
                );
                dispatch_pending_Object.nonLiquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                dispatch_pending_Object.nonLiquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                dispatch_pending_Object.nonLiquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            }
          } else {
            if (ele.status == "completed") {
              obj.dispatch.liquid += Number(ele.count);
              if (ele.tat == "within TAT") {
                dispatch_completed_Object.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                dispatch_completed_Object.liquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                dispatch_completed_Object.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                dispatch_completed_Object.liquid.beyond_TAT.sum += Number(
                  ele.count
                );

              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                dispatch_pending_Object.liquid.within_TAT.amount += Number(
                  ele.amount
                );
                dispatch_pending_Object.liquid.within_TAT.sum += Number(
                  ele.count
                );
              } else {
                dispatch_pending_Object.liquid.beyond_TAT.amount += Number(
                  ele.amount
                );
                dispatch_pending_Object.liquid.beyond_TAT.sum += Number(
                  ele.count
                );
              }
            }
          }
        });

        obj.dispatch.combine = obj.dispatch.liquid + obj.dispatch.nonLiquid;
        dataObject.dispatch_completed_Object = dispatch_completed_Object;
        dataObject.dispatch_pending_Object = dispatch_pending_Object;
        dataObject.dispatch_objected_Object = dispatch_objected_Object;
      };

      dataObject.radialchart = obj;

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "QC_TAT":
              qc_tat_Calculation(rows);
              break;
            case "Registration_TAT":
              registration_tat_Calculation(rows);
              break;
            case "Dispatch TAT":
              dispatch_tat_Calculation(rows);
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
        let quries = await SipRegistrationQueries.findOne(
          { endpoint: "Registration_Summary" },
          { queriesArray: 1 }
        );
        let queriesArray = quries.queriesArray;
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
          { fund: fund, date: transaction_date, type: "sip_registration" },
          {
            $set: {
              date: transaction_date,
              "dataObject.qc_completed_Object": dataObject.qc_completed_Object,
              "dataObject.qc_pending_Object": dataObject.qc_pending_Object,
              "dataObject.qc_objected_Object": dataObject.qc_objected_Object,
              "dataObject.registration_completed_Object":
                dataObject.registration_completed_Object,
              "dataObject.registration_pending_Object":
                dataObject.registration_pending_Object,
              "dataObject.registration_rejected_Object":
                dataObject.registration_rejected_Object,
              "dataObject.dispatch_completed_Object":
                dataObject.dispatch_completed_Object,
              "dataObject.dispatch_pending_Object":
                dataObject.dispatch_pending_Object,
              "dataObject.dispatch_objected_Object":
                dataObject.dispatch_objected_Object,
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

      const data = await main();
      return data;
    }
  } catch (e) {
    console.log("Error in sipsummary provider ", e);
    throw new Error(e);
  }
};

export { registration_summary_provider };
