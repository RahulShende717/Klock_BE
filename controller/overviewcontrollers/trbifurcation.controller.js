import { handleTRBifurcation } from "../../businesslogicsprovider/overviewprovider/trbifurcation.provider.js";

export const tr_bifurcation_controller = async (req, res) => {
  try {
    const transaction_date = req.body.currentDate;
    const fund = req.body.fund;
    const data = await handleTRBifurcation(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    console.log("Error while fetching top pending transactions", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
