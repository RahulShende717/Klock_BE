import { sipRegstrationJourney } from "../../businesslogicsprovider/sipregistrationprovider/registrationjourney.provider.js";
export const sip_registration_journey_controller = async (req, res) => {
  try {
    const transaction_date = req.body.transaction_date;
    const fund = req.body.fund;
    const data = await sipRegstrationJourney(transaction_date, fund);
    res.status(200).send(data);
  } catch (e) {
    // console.log("Error while fetching typeiwise and platform wise data", e);
    res.status(500).send({ message: "Internal Server Error" });
  }
};
