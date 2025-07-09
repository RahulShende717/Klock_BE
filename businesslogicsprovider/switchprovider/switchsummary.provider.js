import SwitchQueries from "../../models/switch.model.js";
import CachedData from "../../models/cacheddata.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getPoolObj } from "../../utils/pool.js";

export const switchsummary_provider = async (
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
      "qc_completed_Object",
      "qc_pending_Object",
      "qc_objected_Object",
      "endorsed_completed_Object",
      "endorsed_pending_Object",
      "endorsed_objected_Object",
      "radialchart",
      "funding_objected_Object",
      "funding_pending_Object",
      "funding_completed_Object",
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
      const obj = {
        inflow: {
          qualityCheck: {
            combine: 0,
            liquid: 0,
            nonLiquid: 0,
          },
          endorsed: {
            combine: 0,
            liquid: 0,
            nonLiquid: 0,
          },
          funding: {
            combine: 0,
            liquid: 0,
            nonLiquid: 0,
          },
        },
        outflow: {
          qualityCheck: {
            combine: 0,
            liquid: 0,
            nonLiquid: 0,
          },
          endorsed: {
            combine: 0,
            liquid: 0,
            nonLiquid: 0,
          },
          funding: {
            combine: 0,
            liquid: 0,
            nonLiquid: 0,
          },
        },
      };
      const qc_summary_calculation = async (rows) => {
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
              obj.inflow.qualityCheck.nonLiquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                qc_completed_Object.inflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                qc_completed_Object.inflow.nonLiquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                // obj.inflow.qualityCheck.liquid = Number(ele.sum);
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
              obj.inflow.qualityCheck.liquid += Number(ele.sum);
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
              obj.outflow.qualityCheck.nonLiquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                qc_completed_Object.outflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                qc_completed_Object.outflow.nonLiquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                // obj.outflow.qualityCheck.liquid = Number(ele.sum);
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
              obj.outflow.qualityCheck.liquid += Number(ele.sum);
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

        obj.outflow.qualityCheck.combine =
          Number(obj.outflow.qualityCheck.liquid) +
          Number(obj.outflow.qualityCheck.nonLiquid);
        obj.inflow.qualityCheck.combine =
          Number(obj.inflow.qualityCheck.liquid) +
          Number(obj.inflow.qualityCheck.nonLiquid);
        dataObject.qc_completed_Object = qc_completed_Object;
        dataObject.qc_pending_Object = qc_pending_Object;
        dataObject.qc_objected_Object = qc_objected_Object;
      };

      const endorsement_summary_calculation = async (rows) => {
        let endorsed_completed_Object = {
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

        let endorsed_pending_Object = {
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

        let endorsed_objected_Object = {
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
              obj.inflow.endorsed.nonLiquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                endorsed_completed_Object.inflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                endorsed_completed_Object.inflow.nonLiquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                // obj.inflow.endorsed.nonLiquid += Number(ele.sum);
                endorsed_completed_Object.inflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                endorsed_completed_Object.inflow.nonLiquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                endorsed_pending_Object.inflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                endorsed_pending_Object.inflow.nonLiquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                endorsed_pending_Object.inflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                endorsed_pending_Object.inflow.nonLiquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                endorsed_objected_Object.inflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                endorsed_objected_Object.inflow.nonLiquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                endorsed_objected_Object.inflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                endorsed_objected_Object.inflow.nonLiquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            }
          } else {
            if (ele.status == "completed") {
              obj.inflow.endorsed.liquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                endorsed_completed_Object.inflow.liquid.within_TAT.amount +=
                  Number(ele.amount);
                endorsed_completed_Object.inflow.liquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                endorsed_completed_Object.inflow.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                endorsed_completed_Object.inflow.liquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                endorsed_pending_Object.inflow.liquid.within_TAT.amount +=
                  Number(ele.amount);
                endorsed_pending_Object.inflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                endorsed_pending_Object.inflow.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                endorsed_pending_Object.inflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                endorsed_objected_Object.inflow.liquid.within_TAT.amount +=
                  Number(ele.amount);
                endorsed_objected_Object.inflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                endorsed_objected_Object.inflow.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                endorsed_objected_Object.inflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            }
          }
        });
        rowsOutflow.forEach((ele, ind) => {
          if (ele.asset_class == "NON LIQUID") {
            if (ele.status == "completed") {
              obj.outflow.endorsed.nonLiquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                endorsed_completed_Object.outflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                endorsed_completed_Object.outflow.nonLiquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                // obj.outflow.endorsed.liquid += Number(ele.);
                endorsed_completed_Object.outflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                endorsed_completed_Object.outflow.nonLiquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                endorsed_pending_Object.outflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                endorsed_pending_Object.outflow.nonLiquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                endorsed_pending_Object.outflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                endorsed_pending_Object.outflow.nonLiquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                endorsed_objected_Object.outflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                endorsed_objected_Object.outflow.nonLiquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                endorsed_objected_Object.outflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                endorsed_objected_Object.outflow.nonLiquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            }
          } else {
            if (ele.status == "completed") {
              obj.outflow.endorsed.liquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                endorsed_completed_Object.outflow.liquid.within_TAT.amount +=
                  Number(ele.amount);
                endorsed_completed_Object.outflow.liquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                endorsed_completed_Object.outflow.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                endorsed_completed_Object.outflow.liquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                endorsed_pending_Object.outflow.liquid.within_TAT.amount +=
                  Number(ele.amount);
                endorsed_pending_Object.outflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                endorsed_pending_Object.outflow.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                endorsed_pending_Object.outflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                endorsed_objected_Object.outflow.liquid.within_TAT.amount +=
                  Number(ele.amount);
                endorsed_objected_Object.outflow.liquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                endorsed_objected_Object.outflow.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                endorsed_objected_Object.outflow.liquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            }
          }
        });

        obj.outflow.endorsed.combine =
          Number(obj.outflow.endorsed.liquid) +
          Number(obj.outflow.endorsed.nonLiquid);
        obj.inflow.endorsed.combine =
          Number(obj.inflow.endorsed.liquid) +
          Number(obj.inflow.endorsed.nonLiquid);
        dataObject.endorsed_completed_Object = endorsed_completed_Object;
        dataObject.endorsed_pending_Object = endorsed_pending_Object;
        dataObject.endorsed_objected_Object = endorsed_objected_Object;
      };

      const funding_summary_calculation = (rows) => {
        let funding_completed_Object = {
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

        let funding_pending_Object = {
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

        let funding_objected_Object = {
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
              obj.inflow.funding.nonLiquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                funding_completed_Object.inflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                funding_completed_Object.inflow.nonLiquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                // obj.inflow.funding.liquid = Number(ele.sum);
                funding_completed_Object.inflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                funding_completed_Object.inflow.nonLiquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                funding_pending_Object.inflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                funding_pending_Object.inflow.nonLiquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                funding_pending_Object.inflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                funding_pending_Object.inflow.nonLiquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                funding_objected_Object.inflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                funding_objected_Object.inflow.nonLiquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                funding_objected_Object.inflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                funding_objected_Object.inflow.nonLiquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            }
          } else {
            if (ele.status == "completed") {
              obj.inflow.funding.liquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                funding_completed_Object.inflow.liquid.within_TAT.amount +=
                  Number(ele.amount);
                funding_completed_Object.inflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                funding_completed_Object.inflow.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                funding_completed_Object.inflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                funding_pending_Object.inflow.liquid.within_TAT.amount +=
                  Number(ele.amount);
                funding_pending_Object.inflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                funding_pending_Object.inflow.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                funding_pending_Object.inflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                funding_objected_Object.inflow.liquid.within_TAT.amount +=
                  Number(ele.amount);
                funding_objected_Object.inflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                funding_objected_Object.inflow.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                funding_objected_Object.inflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            }
          }
        });
        rowsOutflow.forEach((ele, ind) => {
          if (ele.asset_class == "NON LIQUID") {
            if (ele.status == "completed") {
              obj.outflow.funding.nonLiquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                funding_completed_Object.outflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                funding_completed_Object.outflow.nonLiquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                // obj.outflow.funding.liquid = Number(ele.sum);
                funding_completed_Object.outflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                funding_completed_Object.outflow.nonLiquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                funding_pending_Object.outflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                funding_pending_Object.outflow.nonLiquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                funding_pending_Object.outflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                funding_pending_Object.outflow.nonLiquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                funding_objected_Object.outflow.nonLiquid.within_TAT.amount +=
                  Number(ele.amount);
                funding_objected_Object.outflow.nonLiquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                funding_objected_Object.outflow.nonLiquid.beyond_TAT.amount +=
                  Number(ele.amount);
                funding_objected_Object.outflow.nonLiquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            }
          } else {
            if (ele.status == "completed") {
              obj.outflow.funding.liquid += Number(ele.sum);
              if (ele.tat == "within TAT") {
                funding_completed_Object.outflow.liquid.within_TAT.amount +=
                  Number(ele.amount);
                funding_completed_Object.outflow.liquid.within_TAT.sum +=
                  Number(ele.sum);
              } else {
                funding_completed_Object.outflow.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                funding_completed_Object.outflow.liquid.beyond_TAT.sum +=
                  Number(ele.sum);
              }
            } else if (ele.status == "pending") {
              if (ele.tat == "within TAT") {
                funding_pending_Object.outflow.liquid.within_TAT.amount +=
                  Number(ele.amount);
                funding_pending_Object.outflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                funding_pending_Object.outflow.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                funding_pending_Object.outflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            } else if (ele.status == "objected") {
              if (ele.tat == "within TAT") {
                funding_objected_Object.outflow.liquid.within_TAT.amount +=
                  Number(ele.amount);
                funding_objected_Object.outflow.liquid.within_TAT.sum += Number(
                  ele.sum
                );
              } else {
                funding_objected_Object.outflow.liquid.beyond_TAT.amount +=
                  Number(ele.amount);
                funding_objected_Object.outflow.liquid.beyond_TAT.sum += Number(
                  ele.sum
                );
              }
            }
          }
        });

        obj.outflow.funding.combine =
          Number(obj.outflow.funding.liquid) +
          Number(obj.outflow.funding.nonLiquid);
        obj.inflow.funding.combine =
          Number(obj.inflow.funding.liquid) +
          Number(obj.inflow.funding.nonLiquid);
        dataObject.funding_completed_Object = funding_completed_Object;
        dataObject.funding_pending_Object = funding_pending_Object;
        dataObject.funding_objected_Object = funding_objected_Object;
      };

      dataObject.radialchart = obj;

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "qc_summary":
              qc_summary_calculation(rows);
              break;
            case "endorsement_summary":
              endorsement_summary_calculation(rows);
              break;
            case "funding_summary":
              funding_summary_calculation(rows);
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
        let purchesQueries = await SwitchQueries.findOne(
          { endpoint: "SwitchSummary" },
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
              "dataObject.qc_completed_Object": dataObject.qc_completed_Object,
              "dataObject.qc_pending_Object": dataObject.qc_pending_Object,
              "dataObject.qc_objected_Object": dataObject.qc_objected_Object,
              "dataObject.endorsed_completed_Object":
                dataObject.endorsed_completed_Object,
              "dataObject.endorsed_pending_Object":
                dataObject.endorsed_pending_Object,
              "dataObject.endorsed_objected_Object":
                dataObject.endorsed_objected_Object,
              "dataObject.radialchart": dataObject.radialchart,
              "dataObject.funding_objected_Object":
                dataObject.funding_objected_Object,
              "dataObject.funding_pending_Object":
                dataObject.funding_pending_Object,
              "dataObject.funding_completed_Object":
                dataObject.funding_completed_Object,
            },
          },
          {
            upsert: true,
            new: true,
          }
        );
        return cached_data.dataObject;
      };

      const res_obj = await main();
      return res_obj;
    }
  } catch (e) {
    console.log("Error in switch summary provider", e);
    throw new Error(e);
  }
};
