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
import { iteratorObject } from "../../utils/snsTracker.js";

router.post("/", express.text(), async (req, res) => {
  try {
    let payloadStr = req.body;
    const snsresponse = JSON.parse(payloadStr);
    res.status(200).send("Ok");
    if (req.header("x-amz-sns-message-type") === "SubscriptionConfirmation") {
      //Confirm subscription
      const subScribeurl = payload.SubscribeURL;
      const res = axios.get(subScribeurl);

    } else if (req.header("x-amz-sns-message-type") === "Notification") {
      // recieve message
      if (snsresponse["Message"] == "Completed") {
        const dataload_completion_timestamp = snsresponse["Timestamp"];
        const date = moment(dataload_completion_timestamp);
        date.add(5, "hours");
        date.add(30, "minutes");
        const formattedDay = date.format("Do");
        const month = date.format("MMMM");
        const time = date.format("h:mm A");
        let dataload_completiontime = `${formattedDay} ${month}, ${time}`;

        await DataLoadLogs.findOneAndUpdate(
          {},
          { completiontimestamp: dataload_completiontime },
          { upsert: true }
        );
        const response = await CachedData.deleteMany({});
        const response1 = await PurchaseTimeSeries.deleteMany({});
        await handle_cached_calender_data();
        const currentDate = moment().format("YYYY-MM-DD");
        const fund = "102";

        await purchaseBifurcation(currentDate, fund);
        await purchaseSummearyData(currentDate, fund);
        // await purchaseTimeSeriesData(startDate, endDate, fund);
        await PurchaseTransactionData(currentDate, fund);
        ``;
        await pendingtransactionsData(currentDate, fund);

        await handleTopCities(currentDate, fund);
        await handle_top_pending_transaction(currentDate, fund);
        await handleTRBifurcation(currentDate, fund);
        // await handle_transaction_histories(currentDate, fund);
        await handle_stage_bifurcation(currentDate, fund);
      }
    } else {
      res.status(500).send("Error!");
      throw new Error(`Invalid message type ${payload.Type}`);
    }
  } catch (error) {
    console.log(error);
    res.status(500).send("Error!");
  }
});

export { router as useSNSStatusUat };
