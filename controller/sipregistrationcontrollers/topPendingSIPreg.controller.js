import { topPendingSipregistration } from "../../businesslogicsprovider/sipregistrationprovider/topPendingSIPReg.provider.js";


export const topPendingSipRegistrationController = async (req, res) => {
  try {
    let dataObject = {};
    let transaction_date = req.body.transaction_date;
    let fund = req.body.fund;
    const result = await topPendingSipregistration(transaction_date, fund);
    res.status(200).send(result);
  } catch (e) {
    // console.log("Error in pending transaction controller purchase", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
