import { sipAlertAndUpfrontreason } from "../../businesslogicsprovider/sipregistrationprovider/sipAlertAndUpfrontreason.provider.js";
export const sipAlertAndUpfrontreasons_controller = async (req, res) => {
  try {
    const transaction_date = req.body.transaction_date;
    const fund = req.body.fund;
    const data = await sipAlertAndUpfrontreason(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error while fetching typeiwise and platform wise data", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
