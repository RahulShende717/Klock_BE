import { handle_stage_bifurcation } from "../../businesslogicsprovider/overviewprovider/trstagebifurcation.provider.js";

export const trxn_stage_bifurcation_controller = async (req, res) => {
  try {
    const transaction_date = req.body.currentDate;
    const fund = req.body.fund;
    const data = await handle_stage_bifurcation(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    console.log("Error while fetching top pending transactions", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
