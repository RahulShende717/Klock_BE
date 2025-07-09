import { pending_trxn_provider_stp } from "../../businesslogicsprovider/stpprovider/stppendingtransactions.provider.js";

export const pendingtrxn_controller = async (req, res) => {
  try {
    const transaction_date = req.body.transaction_date;
    const fund = req.body.fund;
    const data = await pending_trxn_provider_stp(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error in pending transaction controller purchase", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
