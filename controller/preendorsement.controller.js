import mongoose from "mongoose";
import ExcelJs from "exceljs"
import path from "path"
import fs from 'fs';
import { time, timeEnd } from "console";
import { fileURLToPath } from 'url';
import { uploadAndSignS3File } from "../middleware/excelfileuploaders.js";
import moment from "moment";
import { fundDetailsVerificationStatus } from "../businesslogicsprovider/redemptionprovider/funddetailsverificationstatus.provider.js";

export const transaction_overview = async (req, res) => {
  try {
    const data =  res.locals.data;
  
    const validatedData = data['ResultSet1'][0];
    const failedData = data['ResultSet2'][0];
    const keys = Object.keys(validatedData).filter((key => key!== 'Total No of Transactions validated'))
    const responseObject = {
        "validated": {
        },
        "failed":{
        }
    }

    let totalValidated = 0;
    let totalFailed = 0;

    for (const key of keys) {
    responseObject["validated"][key] = validatedData[key];
    responseObject["failed"][key] = failedData[key];

    const validTransaction = validatedData[key];
    const failedTransaction = failedData[key];

    totalValidated += validTransaction;
    totalFailed += failedTransaction;
  
  }

  const totalTransaction = totalValidated + totalFailed
  const validatedPercentage = ((totalValidated / totalTransaction) * 100).toFixed(2);
  responseObject['totalTransaction'] = totalTransaction;
  responseObject['totalValidated'] = totalValidated;
  responseObject['totalFailed'] = totalFailed;
  responseObject['validatedPercentage'] = validatedPercentage;

  res.status(200).send({success: true, data: responseObject});
  } catch (error) {
    console.log("error in transaction overview", error);
    res.status(500).send({success: false, message:"Failed to fetch transaction overview"});
  }
};


export const error_transaction_count = async (req, res) => {
  try {
    const responseData =  res.locals.data.ErrorTransactions;
    res.status(200).send({success: true, data: responseData})
  } catch (error) {
    console.log("error in fetching total error transaction", error);
    res.status(500).send({success: false, message:"Failed to fetch error transaction count"});
  }
};

export const error_transactions = async (req, res,next) => {
  try {
    const responseData = res.locals.data.TopErrorTransactions
    const {type, fund } = req.body;
    console.log("Bnbhb ", type, fund)
    
    if(type == 'download'){
      if(responseData.length === 0) return res.status(200).send({success: true, data: responseData})
      
    const dateStr = moment().format('YYYY-MM-DD_HH-mm-ss');
    let reportName = `failedtransactions${dateStr}`
    let sheetName = "failed-transactions"
    const filePath = path.join('preendorsementReports', `${reportName}.xlsx`);

    const workbook = new ExcelJs.Workbook();
    const worksheet = workbook.addWorksheet(sheetName);
    const headers = Object.keys(responseData[0]);
    console.log("Hedaers are:", headers)

    // Write headers
   worksheet.columns = headers.map((header,ind) => ({ header: header, key: header, width: (responseData[0][header]) ? responseData[0][header].toString().length + 10 : 5 }));
   const headerRow = worksheet.getRow(1);
   headerRow.height = 20
   headerRow.eachCell({includeEmpty:true}, (cell) => { 
   cell.font = { bold: true, color: { argb: 'FFFFFFFF' } }; // White font
   cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '0XFF0D47A1' }};
   });
  
    // Stream rows in chunks
    for (let i = 0; i < responseData.length; i++) {
       const rowData = Object.values(responseData[i]).map(value => value ?? "");
       const dataRow = worksheet.addRow(rowData);
       dataRow.height = 20
    }

    try {
        await workbook.xlsx.writeFile(filePath); // Finalize file writing
        console.log("File pathis:", filePath)
        const signedURL = await uploadAndSignS3File(filePath, reportName, fund)
        return res.status(200).send({success: true, signedurl: signedURL})

    } catch (error) {
        console.error(`Failed to write file: ${error}`);
        return res.status(500).send({success: false, message:"Failed to fetch all error transaction"});
    }
    }

    return res.status(200).send({success: true, data: responseData})
  
   
  } catch (error) {
    console.log("error in fetching all transaction", error);
    res.status(500).send({success: false, message:"Failed to fetch all error transaction"});
  }
};

export const ihno_error_transactions = async(req,res) =>{
  try{
    const responseData = res.locals.data.TopErrorTransactions
    res.status(200).send({success: true, data: responseData})
  }catch(e){
     console.log("error in fetching all ihno failed transaction", error);
    res.status(500).send({success: false, message:"Failed to fetch all error transaction"});
  }
}