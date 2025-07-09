import express from "express";
import axios from "axios";
const router = express.Router();
import OverViewMasterNew from "../../models/newoverview.model.js";
import DataLoadLogs from "../../models/dataloadlogs.js";
import moment from "moment";
import { handle_cached_calender_data } from "../datestatus.controller.js";
import CachedData from "../../models/cacheddata.model.js";
import PurchaseTimeSeries from "../../models/purchasetimeseris.model.js";
import { purchaseBifurcation } from "../../businesslogicsprovider/purchaseprovider/purchasebifurcation.provider.js";
import { purchaseSummearyData } from "../../businesslogicsprovider/purchaseprovider/purchasesummary.provider.js";
import { purchaseTimeSeriesData } from "../../businesslogicsprovider/purchaseprovider/purchasetimeseries.provider.js";
import { PurchaseTransactionData } from "../../businesslogicsprovider/purchaseprovider/purchasetransactions.provider.js";
import { pendingtransactionsData } from "../../businesslogicsprovider/purchaseprovider/pendingtransactions.provider.js";
import { handleTopCities } from "../../businesslogicsprovider/overviewprovider/topcities.provider.js";
import { handle_top_pending_transaction } from "../../businesslogicsprovider/overviewprovider/toppendingtransaction.provider.js";
import { handleTRBifurcation } from "../../businesslogicsprovider/overviewprovider/trbifurcation.provider.js";
import { handle_stage_bifurcation } from "../../businesslogicsprovider/overviewprovider/trstagebifurcation.provider.js";
import { getFundCode } from "../../utils/fundCodeMapping.js";
import { transaction_history_provider } from "../../businesslogicsprovider/redemptionprovider/transactionhistory.provider.js";
import { redemptionSummary } from "../../businesslogicsprovider/redemptionprovider/redemptionsummary.provider.js";
import { pendingtrxnandreasons } from "../../businesslogicsprovider/redemptionprovider/pendingtransactions.provider.js";
import { nigoPlatformCategoryWise } from "../../businesslogicsprovider/redemptionprovider/nigoplatformcategorywisesummary.provider.js";
import { iteratorObject } from "../../utils/snsTracker.js";
import { SIPBifurcation } from "../../businesslogicsprovider/sipprovider/sipbifurcation.provider.js";
import { sipAlertDebitFailuerUpfront } from "../../businesslogicsprovider/sipprovider/sipAlertsDebitFailuerUpfront.provider.js";
import { fundDetailsVerificationStatus } from "../../businesslogicsprovider/redemptionprovider/funddetailsverificationstatus.provider.js";
import { pendingtrxn_rejectedreason_provider } from "../../businesslogicsprovider/switchprovider/pendingandrejectedreasons.provider.js";
import { platform_nigo_summary_provider } from "../../businesslogicsprovider/switchprovider/platformwisenigosummary.provider.js";
import { switchalerts_verificationstatus } from "../../businesslogicsprovider/switchprovider/switchalertsverificationstatus.provider.js";
import { switchsummary_provider } from "../../businesslogicsprovider/switchprovider/switchsummary.provider.js";
import { StpNigoPlatform } from "../../businesslogicsprovider/stpprovider/stpnigoplatform.provider.js";
import { rejectedDeletedReasons } from "../../businesslogicsprovider/stpprovider/stprejectedanddeletedreasons.provider.js";
import { stpTransactionData } from "../../businesslogicsprovider/stpprovider/stptransactions.provider.js";
import { funddetails_nigo_platform_summary_provider } from "../../businesslogicsprovider/swpprovider/funddetailsnigoplatformsummary.provider.js";
import { swp_summary_provider } from "../../businesslogicsprovider/swpprovider/swpsummary.provider.js";
import { swp_timeseries_provider } from "../../businesslogicsprovider/swpprovider/timeseries.provider.js";
import { pendingtransactionsDataTillDate } from "../../businesslogicsprovider/sipprovider/pendingtransactionTillDate.provider.js";
import { SIPSummearyData } from "../../businesslogicsprovider/sipprovider/sipsummary.provider.js";
import { SIPTimeSeriesData } from "../../businesslogicsprovider/sipprovider/siptimeseries.provider.js";
import { sipTransactionJourney } from "../../businesslogicsprovider/sipprovider/sipTransactionJourney.provider.js";
import { SIPTransactionData } from "../../businesslogicsprovider/sipprovider/siptransactions.provider.js";
import { switch_transction_provider } from "../../businesslogicsprovider/switchprovider/switchtransaction.provider.js";
import { pending_trxn_provider_stp } from "../../businesslogicsprovider/stpprovider/stppendingtransactions.provider.js";
import { pending_txn_swp } from "../../businesslogicsprovider/swpprovider/penidngtrxn.provider.js";
import { transaction_history_provider_swp } from "../../businesslogicsprovider/swpprovider/swtransactions.provider.js";
import { getLastSevenDays } from "../../utils/sevenDaysCash.js";
import mongoose from "mongoose";
import { sip_pendingtransactionsData } from "../../businesslogicsprovider/sipprovider/pendingtransaction.provider.js";
import { sipRegstrationJourney } from "../../businesslogicsprovider/sipregistrationprovider/registrationjourney.provider.js";
import TimeseriesCachedData from "../../models/timeseries.model.js";
import { timeseries_provider } from "../../businesslogicsprovider/redemptionprovider/timeseries.provider.js";
import { switch_timeseries_provider } from "../../businesslogicsprovider/switchprovider/timeseries.provider.js";
import { STPTimeSeriesData } from "../../businesslogicsprovider/stpprovider/stptimeseries.provider.js";
import TrjourneycachedData from "../../models/trjourneycacheddata.model.js";
import { topPendingSipRegistrationController } from "../sipregistrationcontrollers/topPendingSIPreg.controller.js";
import { sipRegistrationTrHistory } from "../../businesslogicsprovider/sipregistrationprovider/sipRegTrhistory.provider.js";
import { sipAlertAndUpfrontreason } from "../../businesslogicsprovider/sipregistrationprovider/sipAlertAndUpfrontreason.provider.js";
import { registration_summary_provider } from "../../businesslogicsprovider/sipregistrationprovider/registartionsummary.provider.js";
import { nigo_platform_provider } from "../../businesslogicsprovider/sipregistrationprovider/nigoplatform.provider.js";
import { topPendingSipregistration } from "../../businesslogicsprovider/sipregistrationprovider/topPendingSIPReg.provider.js";
import { timeseries_provider_sip } from "../../businesslogicsprovider/sipregistrationprovider/timeseries.provider.js";
import { refundTransactionsData } from "../../businesslogicsprovider/purchaseprovider/refundTransactions.provider.js";
import { cleanCache } from "../../businesslogicsprovider/allPendingTransactionAndReasons.provider.js";

router.post("/", express.text(), async (req, res) => {
  try {
    let payloadStr = req.body;
    const snsresponse = JSON.parse(payloadStr);
    res.status(200).send("Ok");
    if (req.header("x-amz-sns-message-type") === "SubscriptionConfirmation") {
      //Confirm subscription
      const subScribeurl = snsresponse.SubscribeURL;
      console.log("subScribeURL: ", subScribeurl);
      const res = axios.get(subScribeurl);
    } else if (req.header("x-amz-sns-message-type") === "Notification") {
      // recieve message

      if (snsresponse["Message"].includes("Completed")) {
        let fund = getFundCode(snsresponse["Message"].split(" ")[0]);
        let trnx = snsresponse["Message"].split(" ")[2];

        // let fund = req.body.fund
        // let trnx = req.body.trans

        // let fund=req.body.fund
        // let trnx=req.body.trans

        const dataload_completion_timestamp = snsresponse["Timestamp"];
        const date = moment(dataload_completion_timestamp);
        date.add(5, "hours");
        date.add(30, "minutes");
        const formattedDay = date.format("Do");
        const month = date.format("MMMM");
        const time = date.format("h:mm A");
        let dataload_completiontime = `${formattedDay} ${month}, ${time}`;

        await DataLoadLogs.findOneAndUpdate(
          { fund: fund },
          { completiontimestamp: dataload_completiontime },
          { upsert: true }
        );

        const currentDate = moment().format("YYYY-MM-DD");
        const startDate = moment(currentDate)
          .startOf("month")
          .format("YYYY-MM-DD");
        const endDate = moment(currentDate).endOf("month").format("YYYY-MM-DD");
        let lastsevendays = getLastSevenDays(currentDate);
        const cachedData = async () => {

          let deletedTransactions = []
           if(trnx === "purchase"){
            deletedTransactions = [, "sip_transaction-calendar"]
            }else if(trnx === 'sip'){
              deletedTransactions = [ "sip_registration", "sip_registration-calendar"]
            }

          const response = await CachedData.deleteMany({
            fund: fund,
            type: { $in: [ trnx, `${trnx}-calendar`,...deletedTransactions] }
            // date: { $lt: lastsevendays[lastsevendays.length - 1] },
          });

          let deleteTransactions = [];
          deleteTransactions =  trnx == 'purchase' ? ["sip_transaction"] : ["sip_registration"]
          const deletedtrjourneycacheddata =
            await TrjourneycachedData.deleteMany({
              fund: fund,
              type: { $in: [ trnx, ...deleteTransactions] }
              // date: { $lt: lastsevendays[lastsevendays.length - 1] },
            });

          await cleanCache(fund, trnx)
          await TimeseriesCachedData.deleteMany({ fund: fund,type: { $in: [ trnx, "sip_registration" , "sip_transaction"] } });
          // const deletedTimeseriesCachedData = await TimeseriesCachedData.deleteMany({})
          // return new Promise(async (resolve, reject) => {
          try {
            // while (iteratorObject[fund].flag === true && iteratorObject[fund].initiator < iteratorObject[fund].toalaCount) {
            // iteratorObject[fund].flag = false;
            // while (
            //   iteratorObject[fund].flag === true &&
            //   iteratorObject[fund].trans.length != 0
            // ) {
            // iteratorObject[fund].flag = false;

            if (trnx == "purchase") {
              await handle_cached_calender_data(fund, "purchase");
              await purchaseBifurcation(currentDate, fund, true);
              await purchaseSummearyData(currentDate, fund, true);
              await PurchaseTransactionData(currentDate, fund, true);
              await pendingtransactionsData(currentDate, fund, true);
              await purchaseTimeSeriesData(startDate, endDate, fund, true);
              await refundTransactionsData(currentDate, fund, true)

              await handle_cached_calender_data(fund, "sip_transaction");
              await sip_pendingtransactionsData(currentDate, fund, true);
              await sipAlertDebitFailuerUpfront(currentDate, fund, true);
              await SIPBifurcation(currentDate, fund, true);
              await SIPSummearyData(currentDate, fund, true);
              // await sipTransactionJourney(currentDate, fund, true);
              await SIPTransactionData(currentDate, fund, true);
              // iteratorObject[fund].trans.splice(
              //   iteratorObject[fund].trans.indexOf("purchase"),
              //   1
              // );
              // iteratorObject[fund].flag = true;
            }

            if (trnx == "sip") {
              await handle_cached_calender_data(fund, "sip_registration");
              await topPendingSipregistration(currentDate, fund, true);
              await sipRegistrationTrHistory(currentDate, fund, true);
              await sipAlertAndUpfrontreason(currentDate, fund, true);
              await sipRegstrationJourney(currentDate, fund, true);
              await registration_summary_provider(currentDate, fund, true);
              await nigo_platform_provider(currentDate, fund, true);
              // await SIPBifurcation(currentDate, fund, true);
              // await sipAlertDebitFailuerUpfront(currentDate, fund, true);
              // await SIPSummearyData(currentDate, fund, true);
              // await sipTransactionJourney(currentDate, fund, true);
              // await SIPTransactionData(currentDate, fund, true);
              // await sip_pendingtransactionsData(currentDate, fund, true);
              // await sipRegstrationJourney(currentDate, fund, true);
              // await SIPTimeSeriesData(startDate, endDate, fund, true);
              // iteratorObject[fund].trans.splice(
              //   iteratorObject[fund].trans.indexOf("sip"),
              //   1
              // );
              // iteratorObject[fund].flag = true;

              // await PurchaseTransactionData(currentDate, fund);
              // await pendingtransactionsData(currentDate, fund);
            }

            if (trnx == "redemption") {
              await handle_cached_calender_data(fund, "redemption");
              await fundDetailsVerificationStatus(currentDate, fund, true);
              await nigoPlatformCategoryWise(currentDate, fund, true);
              await pendingtrxnandreasons(currentDate, fund, true);
              await redemptionSummary(currentDate, fund, true);
              await transaction_history_provider(currentDate, fund, true);
              await timeseries_provider(startDate, endDate, fund, true);
              // iteratorObject[fund].trans.splice(
              //   iteratorObject[fund].trans.indexOf("redemption"),
              //   1
              // );
              // iteratorObject[fund].flag = true;
            }
            if (trnx == "switch") {
              await handle_cached_calender_data(fund, "switch");
              await pendingtrxn_rejectedreason_provider(
                currentDate,
                fund,
                true
              );
              await platform_nigo_summary_provider(currentDate, fund, true);
              await switchalerts_verificationstatus(currentDate, fund, true);
              await switchsummary_provider(currentDate, fund, true);
              await switch_transction_provider(currentDate, fund, true);
              await switch_timeseries_provider(startDate, endDate, fund, true);
              // iteratorObject[fund].trans.splice(
              //   iteratorObject[fund].trans.indexOf("switch"),
              //   1
              // );

              // iteratorObject[fund].flag = true;
            }

            if (trnx == "stp") {
              await handle_cached_calender_data(fund, "stp");
              await StpNigoPlatform(currentDate, fund, true);
              await pending_trxn_provider_stp(currentDate, fund, true);
              await rejectedDeletedReasons(currentDate, fund, true);
              await stpTransactionData(currentDate, fund, true);
              await STPTimeSeriesData(startDate, endDate, fund, true);
              // iteratorObject[fund].trans.splice(
              //   iteratorObject[fund].trans.indexOf("stp"),
              //   1
              // );
              // iteratorObject[fund].flag = true;
            }

            if (trnx == "swp") {
              await handle_cached_calender_data(fund, "swp");
              await funddetails_nigo_platform_summary_provider(
                currentDate,
                fund,
                true
              );
              await pending_txn_swp(currentDate, fund, true);
              await swp_summary_provider(currentDate, fund, true);
              await transaction_history_provider_swp(currentDate, fund, true);
              await swp_timeseries_provider(startDate, endDate, fund, true);
              // iteratorObject[fund].trans.splice(
              //   iteratorObject[fund].trans.indexOf("swp"),
              //   1
              // );
              // iteratorObject[fund].flag = true;
            }
            // }

            // resolve();
          } catch (error) {
            console.log("Errror while caching for currentdate", error);
            // reject(error);
          }
        };
        await cachedData();

        // let sevenDayflag = await mongoose.connection
        //   .collection("weeklyFlag")
        //   .findOne({ fund: fund });
        // await handle_cached_calender_data(fund);
        //logic for updating seven days
        // if it is not matched with currentDate and flag is false , will cache for lastsevendays
        // if (sevenDayflag[`${trnx}`]["lastUpdate"] != currentDate) {
        //   if (trnx == "sip") {
        //     const fetch_results = async (queries) => {
        //       return Promise.all(
        //         lastsevendays.map(async (ele) => {
        //           // await SIPBifurcation(ele, fund, true);
        //           // await sipAlertDebitFailuerUpfront(ele, fund, true);
        //           // await SIPSummearyData(ele, fund, true);
        //           // await sipTransactionJourney(ele, fund, true);
        //           // await SIPTransactionData(ele, fund, true);
        //           // await sip_pendingtransactionsData(ele, fund, true);
        //           // await sipRegstrationJourney(ele, fund, true);
        //           await topPendingSipregistration(ele, fund, true);
        //           await sipRegistrationTrHistory(ele, fund, true);
        //           await sipAlertAndUpfrontreason(ele, fund, true);
        //           await sipRegstrationJourney(ele, fund, true);
        //           await registration_summary_provider(ele, fund, true);
        //           await nigo_platform_provider(ele, fund, true);
        //           await timeseries_provider_sip(startDate, endDate, fund, true);
        //         })
        //       );
        //     };
        //     await fetch_results();
        //     let jobCompletionTime = moment()
        //       .add(5, "hours")
        //       .add(30, "minutes")
        //       .format("hh:mm:ss");
        //     await mongoose.connection.collection("weeklyFlag").findOneAndUpdate(
        //       { fund: fund },
        //       {
        //         $set: {
        //           "sip.lastUpdate": currentDate,
        //           "sip.jobCompletionTime": jobCompletionTime,
        //         },
        //       }
        //     );
        //   }

        //   if (trnx == "purchase") {
        //     const fetch_results = async (queries) => {
        //       return Promise.all(
        //         lastsevendays.map(async (ele) => {
        //           await pendingtransactionsData(ele, fund, true);
        //           await purchaseBifurcation(ele, fund, true);
        //           await purchaseSummearyData(ele, fund, true);
        //           await PurchaseTransactionData(ele, fund, true);
        //         })
        //       );
        //     };
        //     await fetch_results();
        //     let jobCompletionTime = moment()
        //       .add(5, "hours")
        //       .add(30, "minutes")
        //       .format("hh:mm:ss");
        //     await mongoose.connection.collection("weeklyFlag").findOneAndUpdate(
        //       { fund: fund },
        //       {
        //         $set: {
        //           "purchase.lastUpdate": currentDate,
        //           "purchase.jobCompletionTime": jobCompletionTime,
        //         },
        //       }
        //     );
        //   }

        //   if (trnx == "redemption") {
        //     const fetch_results = async (queries) => {
        //       return Promise.all(
        //         lastsevendays.map(async (ele) => {
        //           await fundDetailsVerificationStatus(ele, fund, true);
        //           await nigoPlatformCategoryWise(ele, fund, true);
        //           await pendingtrxnandreasons(ele, fund, true);
        //           await redemptionSummary(ele, fund, true);
        //           await transaction_history_provider(ele, fund, true);
        //         })
        //       );
        //     };
        //     await fetch_results();
        //     let jobCompletionTime = moment()
        //       .add(5, "hours")
        //       .add(30, "minutes")
        //       .format("hh:mm:ss");
        //     await mongoose.connection.collection("weeklyFlag").findOneAndUpdate(
        //       { fund: fund },
        //       {
        //         $set: {
        //           "redemption.lastUpdate": currentDate,
        //           "redemption.jobCompletionTime": jobCompletionTime,
        //         },
        //       }
        //     );
        //   }

        //   if (trnx == "switch") {
        //     const fetch_results = async (queries) => {
        //       return Promise.all(
        //         lastsevendays.map(async (ele) => {
        //           iteratorObject[fund].trans.splice(
        //             iteratorObject[fund].trans.indexOf("switch"),
        //             1
        //           );

        //           await pendingtrxn_rejectedreason_provider(ele, fund, true);
        //           await platform_nigo_summary_provider(ele, fund, true);
        //           await switchalerts_verificationstatus(ele, fund, true);
        //           await switchsummary_provider(ele, fund, true);
        //           await switch_transction_provider(ele, fund, true);
        //         })
        //       );
        //     };
        //     await fetch_results();
        //     let jobCompletionTime = moment()
        //       .add(5, "hours")
        //       .add(30, "minutes")
        //       .format("hh:mm:ss");
        //     await mongoose.connection.collection("weeklyFlag").findOneAndUpdate(
        //       { fund: fund },
        //       {
        //         $set: {
        //           "switch.lastUpdate": currentDate,
        //           "switch.jobCompletionTime": jobCompletionTime,
        //         },
        //       }
        //     );
        //   }

        //   if (trnx == "stp") {
        //     const fetch_results = async (queries) => {
        //       return Promise.all(
        //         lastsevendays.map(async (ele) => {
        //           await StpNigoPlatform(ele, fund, true);
        //           await pending_trxn_provider_stp(ele, fund, true);
        //           await rejectedDeletedReasons(ele, fund, true);
        //           await stpTransactionData(ele, fund, true);
        //         })
        //       );
        //     };
        //     await fetch_results();
        //     let jobCompletionTime = moment()
        //       .add(5, "hours")
        //       .add(30, "minutes")
        //       .format("hh:mm:ss");
        //     await mongoose.connection.collection("weeklyFlag").findOneAndUpdate(
        //       { fund: fund },
        //       {
        //         $set: {
        //           "stp.lastUpdate": currentDate,
        //           "stp.jobCompletionTime": jobCompletionTime,
        //         },
        //       }
        //     );
        //   }

        //   if (trnx == "swp") {
        //     const fetch_results = async (queries) => {
        //       return Promise.all(
        //         lastsevendays.map(async (ele) => {
        //           await funddetails_nigo_platform_summary_provider(
        //             ele,
        //             fund,
        //             true
        //           );
        //           await pending_txn_swp(ele, fund, true);
        //           await swp_summary_provider(ele, fund, true);
        //           await transaction_history_provider_swp(ele, fund, true);
        //         })
        //       );
        //     };
        //     await fetch_results();
        //     let jobCompletionTime = moment()
        //       .add(5, "hours")
        //       .add(30, "minutes")
        //       .format("hh:mm:ss");
        //     await mongoose.connection.collection("weeklyFlag").findOneAndUpdate(
        //       { fund: fund },
        //       {
        //         $set: {
        //           "swp.lastUpdate": currentDate,
        //           "swp.jobCompletionTime": jobCompletionTime,
        //         },
        //       }
        //     );
        //   }
        // }
      } else {
        res.status(500).send("Error!");
        throw new Error(`Invalid message type ${payloadStr.Type}`);
      }
    }
  } catch (error) {
    console.log(error);
  }

  // try {

  //   let payloadStr = req.body;
  //   const snsresponse = JSON.parse(payloadStr);
  //   res.status(200).send("Ok");
  //   if (req.header("x-amz-sns-message-type") === "SubscriptionConfirmation") {
  //     //Confirm subscription
  //     const subScribeurl = payload.SubscribeURL;
  //     const res = axios.get(subScribeurl);

  //   } else if (req.header("x-amz-sns-message-type") === "Notification") {
  //     // recieve message
  //     if (snsresponse['Message'] == 'Completed') {
  // const dataload_completion_timestamp = snsresponse['Timestamp']
  // const date = moment(dataload_completion_timestamp);
  // date.add(5, 'hours');
  // date.add(30, 'minutes');
  // const formattedDay = date.format('Do');
  // const month = date.format('MMMM');
  // const time = date.format('h:mm A');
  // let dataload_completiontime = `${formattedDay} ${month}, ${time}`;
  // await DataLoadLogs.findOneAndUpdate({}, { completiontimestamp: dataload_completiontime }, { upsert: true })
  // const response = await CachedData.deleteMany({});
  // const response1 = await PurchaseTimeSeries.deleteMany({})

  // await handle_cached_calender_data()
  // const currentDate = moment().format('YYYY-MM-DD');
  // const fund = req.body.fund
  // let finalFlag=0;

  // //logic for updating seven days
  // if(sevenDayflag[fund]!=currentDate){
  //   const response = await CachedData.deleteMany({"fund":fund});
  //   sevenDayflag[fund]=currentDate;
  //   let lastsevendays= getLastSevenDays(currentDate)

  //   const fetch_results = async (queries) => {
  //     return Promise.all(
  //       lastsevendays.map(async (ele) => {
  //         await purchaseBifurcation(ele, fund);
  //         await purchaseSummearyData(ele, fund);
  //         await PurchaseTransactionData(ele, fund);
  //         await pendingtransactionsData(ele, fund);
  //       })
  //     );
  //   };
  //   await  fetch_results();
  // }

  // if (iteratorObject[fund].toalaCount != 2)
  //   iteratorObject[fund].toalaCount += 1;

  // const cachedData = () => {
  //   return new Promise(async (resolve, reject) => {
  //     try {
  //       while (iteratorObject[fund].flag === true && iteratorObject[fund].initiator < iteratorObject[fund].toalaCount) {
  //         iteratorObject[fund].flag = false;

  //         const currentDate = moment().format('YYYY-MM-DD');
  //         await purchaseBifurcation(currentDate, fund);
  //         await purchaseSummearyData(currentDate, fund);
  //         await PurchaseTransactionData(currentDate, fund);
  //         await pendingtransactionsData(currentDate, fund);

  //         iteratorObject[fund].initiator++;
  //         iteratorObject[fund].flag = true;
  //         finalFlag+=1;
  //       }

  //       resolve();
  //     } catch (error) {
  //       reject(error);
  //     }
  //   });
  // };

  // await cachedData();

  // if(finalFlag==iteratorObject[fund].toalaCount){
  //  iteratorObject[fund].toalaCount = 0;
  // iteratorObject[fund].initiator = 0;
  // }

  //     }

  //   } else {
  //     res.status(500).send("Error!");
  //     throw new Error(`Invalid message type ${payload.Type}`);
  //   }
  // } catch (error) {
  //   console.log(error);
  //   res.status(500).send("Error!");
  // }
});

export { router as useSNSStatusProd };
