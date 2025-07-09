import { pendingtrxn_rejectedreason_provider } from "../../businesslogicsprovider/switchprovider/pendingandrejectedreasons.provider.js";

export const pending_rejected_trxn = async (req, res) => {
  try {
    const { fund, transaction_date } = req.body;
    const data = await pendingtrxn_rejectedreason_provider(
      transaction_date,
      fund
    );
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error in purchase timeseris controller", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
