import CachedData from "../../models/cacheddata.model.js";
import SIPQueries from "../../models/sipqueries.model.js";
import { getFundWiseSchema } from "../../utils/schemamapping.js";
import { getPoolByFund } from "../../lib/postgressConnect.js";
import { getPoolObj } from "../../utils/pool.js";

export const sipTransactionJourney = async (
  trDate,
  fundcode,
  snsstatus = false
) => {
  try {
    let dataObject = {};
    let transaction_date = trDate;
    let fund = fundcode;
    let schema = getFundWiseSchema(fund);
    const pool = getPoolObj(fund);
    const keysToCheck = [
      "eligible_for_debit",
      "debit_success",
      "debit_failed",
      "pa_acknowledged",
      "debit_instructions",
      "instant_rejection",
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
      const eligible_for_debit_calculation = (rows) => {
        let obj = {
          eligible_for_debit_combine: {
            amount: 0,
            count: 0,
          },
          eligible_for_debit_liquid: {
            amount: 0,
            count: 0,
          },
          eligible_for_debit_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        rows.forEach((ele, ind) => {
          obj.eligible_for_debit_combine.amount += Number(ele.amt);
          obj.eligible_for_debit_combine.count += Number(ele.cnt);
          if (ele.asset_class == "LIQUID") {
            obj.eligible_for_debit_liquid.amount += Number(ele.amt);
            obj.eligible_for_debit_liquid.count += Number(ele.cnt);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.eligible_for_debit_nonliquid.amount += Number(ele.amt);
            obj.eligible_for_debit_nonliquid.count += Number(ele.cnt);
          }
        });
        dataObject.eligible_for_debit = obj;
      };
      const debit_successAndFailed_calculation = (rows) => {
        let debit_success_obj = {
          debit_success_combine: {
            amount: 0,
            count: 0,
          },
          debit_success_liquid: {
            amount: 0,
            count: 0,
          },
          debit_success_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        let debit_success_lic_obj = {
          debit_success_combine: {
            amount: 0,
            count: 0,
          },
          debit_success_liquid: {
            amount: 0,
            count: 0,
          },
          debit_success_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        let debit_failed_obj = {
          debit_failed_combine: {
            amount: 0,
            count: 0,
          },
          debit_failed_liquid: {
            amount: 0,
            count: 0,
          },
          debit_failed_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        let debit_failed_lic_obj = {
          debit_failed_combine: {
            amount: 0,
            count: 0,
          },
          debit_failed_liquid: {
            amount: 0,
            count: 0,
          },
          debit_failed_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        if (fund == "102") {
          rows.forEach((ele, ind) => {
            if (ele.status == "DEBIT SUCCESS") {
              debit_success_lic_obj.debit_success_combine.amount += Number(
                ele.amt
              );
              debit_success_lic_obj.debit_success_combine.count += Number(
                ele.cnt
              );
              if (ele.asset_class == "LIQUID") {
                debit_success_lic_obj.debit_success_liquid.amount += Number(
                  ele.amt
                );
                debit_success_lic_obj.debit_success_liquid.count += Number(
                  ele.cnt
                );
              }
              if (ele.asset_class == "NON LIQUID") {
                debit_success_lic_obj.debit_success_nonliquid.amount += Number(
                  ele.amt
                );
                debit_success_lic_obj.debit_success_nonliquid.count += Number(
                  ele.cnt
                );
              }
            } else {
              debit_failed_lic_obj.debit_failed_combine.amount += Number(
                ele.amt
              );
              debit_failed_lic_obj.debit_failed_combine.count += Number(
                ele.cnt
              );
              if (ele.asset_class == "LIQUID") {
                debit_failed_lic_obj.debit_failed_liquid.amount += Number(
                  ele.amt
                );
                debit_failed_lic_obj.debit_failed_liquid.count += Number(
                  ele.cnt
                );
              }
              if (ele.asset_class == "NON LIQUID") {
                debit_failed_lic_obj.debit_failed_nonliquid.amount += Number(
                  ele.amt
                );
                debit_failed_lic_obj.debit_failed_nonliquid.count += Number(
                  ele.cnt
                );
              }
            }
          });
        } else {
          rows.forEach((ele, ind) => {
            if (ele.status == "DEBIT SUCCESS") {
              debit_success_obj.debit_success_combine.amount += Number(ele.amt);
              debit_success_obj.debit_success_combine.count += Number(ele.cnt);
              if (ele.asset_class == "LIQUID") {
                debit_success_obj.debit_success_liquid.amount += Number(
                  ele.amt
                );
                debit_success_obj.debit_success_liquid.count += Number(ele.cnt);
              }
              if (ele.asset_class == "NON LIQUID") {
                debit_success_obj.debit_success_nonliquid.amount += Number(
                  ele.amt
                );
                debit_success_obj.debit_success_nonliquid.count += Number(
                  ele.cnt
                );
              }
            } else {
              debit_failed_obj.debit_failed_combine.amount += Number(ele.amt);
              debit_failed_obj.debit_failed_combine.count += Number(ele.cnt);
              if (ele.asset_class == "LIQUID") {
                debit_failed_obj.debit_failed_liquid.amount += Number(ele.amt);
                debit_failed_obj.debit_failed_liquid.count += Number(ele.cnt);
              }
              if (ele.asset_class == "NON LIQUID") {
                debit_failed_obj.debit_failed_nonliquid.amount += Number(
                  ele.amt
                );
                debit_failed_obj.debit_failed_nonliquid.count += Number(
                  ele.cnt
                );
              }
            }
          });
        }

        if (fund == "102") {
          dataObject.debit_success = debit_success_lic_obj;
          dataObject.debit_failed = debit_failed_lic_obj;
        } else {
          dataObject.debit_success = debit_success_obj;
          dataObject.debit_failed = debit_failed_obj;
        }
      };
      const pa_acknowledged_calculation = (rows) => {
        let obj = {
          pa_acknowledged_combine: {
            amount: 0,
            count: 0,
          },
          pa_acknowledged_liquid: {
            amount: 0,
            count: 0,
          },
          pa_acknowledged_nonliquid: {
            amount: 0,
            count: 0,
          },
        };
        let licObj = {
          pa_acknowledged_combine: {
            amount: 0,
            count: 0,
          },
          pa_acknowledged_liquid: {
            amount: 0,
            count: 0,
          },
          pa_acknowledged_nonliquid: {
            amount: 0,
            count: 0,
          },
        };
        if (fund === "102") {
          rows.forEach((ele, ind) => {
            licObj.pa_acknowledged_combine.amount += Number(ele.amt);
            licObj.pa_acknowledged_combine.count += Number(ele.cnt);
            if (ele.asset_class == "LIQUID") {
              licObj.pa_acknowledged_liquid.amount += Number(ele.amt);
              licObj.pa_acknowledged_liquid.count += Number(ele.cnt);
            }
            if (ele.asset_class == "NON LIQUID") {
              licObj.pa_acknowledged_nonliquid.amount += Number(ele.amt);
              licObj.pa_acknowledged_nonliquid.count += Number(ele.cnt);
            }
          });
        } else {
          rows.forEach((ele, ind) => {
            obj.pa_acknowledged_combine.amount += Number(ele.amt);
            obj.pa_acknowledged_combine.count += Number(ele.cnt);
            if (ele.asset_class == "LIQUID") {
              obj.pa_acknowledged_liquid.amount += Number(ele.amt);
              obj.pa_acknowledged_liquid.count += Number(ele.cnt);
            }
            if (ele.asset_class == "NON LIQUID") {
              obj.pa_acknowledged_nonliquid.amount += Number(ele.amt);
              obj.pa_acknowledged_nonliquid.count += Number(ele.cnt);
            }
          });
        }
        if (fund === "102") {
          dataObject.pa_acknowledged = licObj;
        } else dataObject.pa_acknowledged = obj;
      };
      const Debit_instructions_calculation = (rows) => {
        let obj = {
          debit_instructions_combine: {
            amount: 0,
            count: 0,
          },
          debit_instructions_liquid: {
            amount: 0,
            count: 0,
          },
          debit_instructions_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        let licObj = {
          debit_instructions_combine: {
            amount: 0,
            count: 0,
          },
          debit_instructions_liquid: {
            amount: 0,
            count: 0,
          },
          debit_instructions_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        if (fund == "102") {
          rows.forEach((ele, ind) => {
            licObj.debit_instructions_combine.amount += Number(ele.amt);
            licObj.debit_instructions_combine.count += Number(ele.cnt);
            if (ele.asset_class == "LIQUID") {
              licObj.debit_instructions_liquid.amount += Number(ele.amt);
              licObj.debit_instructions_liquid.count += Number(ele.cnt);
            }
            if (ele.asset_class == "NON LIQUID") {
              licObj.debit_instructions_nonliquid.amount += Number(ele.amt);
              licObj.debit_instructions_nonliquid.count += Number(ele.cnt);
            }
          });
        } else {
          rows.forEach((ele, ind) => {
            obj.debit_instructions_combine.amount += Number(ele.amt);
            obj.debit_instructions_combine.count += Number(ele.cnt);
            if (ele.asset_class == "LIQUID") {
              obj.debit_instructions_liquid.amount += Number(ele.amt);
              obj.debit_instructions_liquid.count += Number(ele.cnt);
            }
            if (ele.asset_class == "NON LIQUID") {
              obj.debit_instructions_nonliquid.amount += Number(ele.amt);
              obj.debit_instructions_nonliquid.count += Number(ele.cnt);
            }
          });
        }

        if (fund == "102") {
          dataObject.debit_instructions = licObj;
        } else {
          dataObject.debit_instructions = obj;
        }
      };

      const instant_rejection_calculation = (rows) => {
        let obj = {
          instant_rejection_combine: {
            amount: 0,
            count: 0,
          },
          instant_rejection_liquid: {
            amount: 0,
            count: 0,
          },
          instant_rejection_nonliquid: {
            amount: 0,
            count: 0,
          },
        };

        rows.forEach((ele, ind) => {
          obj.instant_rejection_combine.amount += Number(ele.amt);
          obj.instant_rejection_combine.count += Number(ele.cnt);
          if (ele.asset_class == "LIQUID") {
            obj.instant_rejection_liquid.amount += Number(ele.amt);
            obj.instant_rejection_liquid.count += Number(ele.cnt);
          }
          if (ele.asset_class == "NON LIQUID") {
            obj.instant_rejection_nonliquid.amount += Number(ele.amt);
            obj.instant_rejection_nonliquid.count += Number(ele.cnt);
          }
        });
        dataObject.instant_rejection = obj;
      };

      const calculate_result = async (array) => {
        array.forEach((item) => {
          const rows = item.result;
          switch (item.name) {
            case "eligible_for_debit":
              eligible_for_debit_calculation(rows);
              break;

            case "debit_successAndFailed":
              debit_successAndFailed_calculation(rows);
              break;

            case "debit_successAndFailed_lic":
              debit_successAndFailed_calculation(rows);
              break;

            case "pa_acknowledged":
              pa_acknowledged_calculation(rows);
              break;

            case "Debit_instructions":
              Debit_instructions_calculation(rows);
              break;

            case "Debit_instructions_lic":
              Debit_instructions_calculation(rows);
              break;

            case "instant_rejection":
              instant_rejection_calculation(rows);
              break;

            case "pa_acknowledged_lic":
              pa_acknowledged_calculation(rows, fund);
              break;

            // case 'nigo_summary_report_liquid': handle_nigo_summary_data(rows, "liquid"); break;
            // case 'nigo_summary_report_nonliquid': handle_nigo_summary_data(rows, "nonliquid"); break;
          }
        });
      };

      const fetch_results = async (queries) => {
        return Promise.all(
          queries.map(async (queryObj) => {
            try {
              const result = await pool.query(queryObj.query);
              return {
                name: queryObj.name,
                result: result.rows,
              };
            } catch (e) {
              console.log(queryObj.name);
            }
          })
        );
      };

      const main = async () => {
        let formattedQueries = [];
        let redemptionQueries = await SIPQueries.findOne(
          { endpoint: "sipTransactionJourney" },
          { queriesArray: 1 }
        );
        let queriesArray = redemptionQueries.queriesArray;
        queriesArray.map((ele) => {
          let formattedQuery = ele.query.replace(
            /'transactionDate'/g,
            `'${transaction_date}'`
          );
          formattedQuery = formattedQuery.replace(/schema/g, `${schema}`);
          formattedQuery = formattedQuery.replace(/'fund'/g, `'${fund}'`);

          formattedQueries.push({ name: ele.name, query: formattedQuery });
        });

        // let dummy=[]
        //   dummy.push(formattedQueries[1])

        const lic_queries = [
          {
            name: "debit_successAndFailed_lic",
            query: `Select asset_class,Status,count(distinct trxn) cnt,sum(amount) amt from(
Select distinct asset_class,(case when active_flag='Y' then 'DEBIT SUCCESS' else 'DEBIT FAILED' end) as Status,
	concat(fund,scheme,plan,folio,ihno) as trxn,cast(cramt as decimal) as amount from 
	lic.final_batchclosed where trtype='SIN' and substring(trdt,1,10)='${transaction_date}'
	and exists (Select distinct * from lic.mandatebillingtransaction_mirror nm where nm.ihno=ihno and nm.transtage='TRANSACTION_SETTLED')
UNION
Select distinct asset_class,(case when active_flag='Y' then 'DEBIT SUCCESS' else 'DEBIT FAILED' end) as Status,
	concat(fund,scheme,plan,folio,ihno) as trxn,cast(cramt as decimal) as amount from
	lic.final_non_batchclosed  where trtype='SIN' and substring(trdt,1,10)='${transaction_date}' 
    and exists (Select distinct * from lic.mandatebillingtransaction_mirror nm where nm.ihno=ihno and nm.transtage='TRANSACTION_SETTLED')
	)a group by 1,2`,
          },
          {
            name: "pa_acknowledged_lic",
            query: `Select asset_class,count(distinct trxn) cntamt,sum(amount) amt from(
Select distinct asset_class,concat(fund,scheme,plan,folio,ihno) as trxn,cast(cramt as decimal) as amount from 
	lic.final_batchclosed where trtype='SIN' and active_flag='Y' and substring(trdt,1,10)='${transaction_date}'
	and exists (Select distinct * from lic.mandatebillingtransaction_mirror nm where nm.ihno=ihno)
UNION
Select distinct asset_class,concat(fund,scheme,plan,folio,ihno) as trxn,cast(cramt as decimal) as amount from
	lic.final_non_batchclosed  where trtype='SIN' and active_flag='Y' and substring(trdt,1,10)='${transaction_date}'
	and exists (Select distinct * from lic.mandatebillingtransaction_mirror nm where nm.ihno=ihno)
	)a group by 1`,
          },
          {
            name: "Debit_instructions_lic",
            query: `Select asset_class,count(distinct trxn) as cnt,sum(amount) as amt from(
Select distinct asset_class,concat(fund,scheme,plan,folio,ihno) as trxn,cast(cramt as decimal) as amount from 
	lic.final_batchclosed where trtype='SIN' and active_flag='Y' and substring(trdt,1,10)='${transaction_date}'
	and exists (Select distinct * from lic.mandatebillingtransaction_mirror nm where nm.ihno=ihno) 
UNION
Select distinct asset_class,concat(fund,scheme,plan,folio,ihno) as trxn,cast(cramt as decimal) as amount from
	lic.final_non_batchclosed  where trtype='SIN' and active_flag='Y' and substring(trdt,1,10)='${transaction_date}'
	and exists (Select distinct * from lic.mandatebillingtransaction_mirror nm where nm.ihno=ihno) 
	)a group by 1`,
          },
        ];

        if (fund == "102") {
          lic_queries.forEach((ele, ind) => {
            formattedQueries.push(ele);
          });
        }
        const all_results = await fetch_results(formattedQueries);

        await calculate_result(all_results);

        const cached_data = await CachedData.findOneAndUpdate(
          { fund: fund, date: transaction_date, type: "sip" },
          {
            $set: {
              date: transaction_date,
              "dataObject.eligible_for_debit": dataObject.eligible_for_debit,
              "dataObject.debit_success": dataObject.debit_success,
              "dataObject.debit_failed": dataObject.debit_failed,
              "dataObject.pa_acknowledged": dataObject.pa_acknowledged,
              "dataObject.debit_instructions": dataObject.debit_instructions,
              "dataObject.instant_rejection": dataObject.instant_rejection,
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
    console.log("Error in siptransaction Journey ", e);
    throw new Error(e);
  }
};
