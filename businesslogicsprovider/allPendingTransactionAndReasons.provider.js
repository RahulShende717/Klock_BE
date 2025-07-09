import { FormatterOptions } from "fast-csv";
import PendingTransactionAndReasonQueries from "../models/pendingTransactionsAndReasons.model.js";
import { getPoolObj } from "../utils/pool.js";
import { getFundWiseSchema } from "../utils/schemamapping.js";
import AWS from "aws-sdk";
import {exec} from "child_process";

// Initialize AWS SDK with region
AWS.config.update({ region: "ap-south-1" });

const s3 = new AWS.S3();
const ssm = new AWS.SSM();

export const allPendingTransactionAndReasons = async (req, res) => {

  const { fund, transaction_type, transaction_date, asset_class, type, sipType } = req.body;
  const fileName = `All_Pending_${type}_${asset_class}`;
  const bucket = process.env.BUCKET_NAME;

  const key = transaction_type=='sip'?`Application-Download-Pending-TXNS/${fund}/${transaction_type}/${sipType}/${transaction_date}/`:`Application-Download-Pending-TXNS/${fund}/${transaction_type}/${transaction_date}/`;
  try {
    // Check if the file exists in S3
    await s3.headObject({
      Bucket: bucket,
      Key: key + fileName + '.csv',
    }).promise();

    console.log("File is found on S3 at the given location");

    // Generate a signed URL for the file
    const signedUrl = await s3.getSignedUrlPromise('getObject', {
      Bucket: bucket,
      Key: key + fileName + '.csv',
    });
    console.log("File is found on S3 at the given location", signedUrl);

    return res.status(200).send({ signedUrl: signedUrl });

  } catch (error) {
    // If file is not found
    if (error.code === 'NotFound') {
      console.log("File does not exist in the S3 bucket");

      const instanceId =  process.env["INSTANCEID"]; // EC2 instance ID
      const pool = getPoolObj(fund);
      const schema = getFundWiseSchema(fund);
      const queryName = `pending_${type}_${asset_class}`;

      // Fetch the query template from database
      let query = await PendingTransactionAndReasonQueries.findOne(
        {
          endpoint: transaction_type === "sip" ? `${transaction_type}_${sipType}` : transaction_type,
        },
        { queriesArray: { $elemMatch: { name: queryName } } }
      );

      console.log("Query Name:", queryName, "Transaction Type:", transaction_type);

      // Format the query with dynamic values
      let queriesArray = query.queriesArray[0];
      console.log("Queries Array:", queriesArray);

      let formattedQuery = queriesArray.query.replace(/transactionDate/g, transaction_date)
        .replace(/schema/g, schema)
        .replace(/'fund'/g, `'${fund}'`);

        // console.log("Formatted Query is:", formattedQuery)

      const copyQuery = `COPY(${formattedQuery}) TO '/tmp/${fileName}.csv' WITH CSV HEADER`;

      try {
        // Execute the query to fetch data
        const result = await pool.query(copyQuery);
        if (result) {
          // Move the file to S3 using EC2 instance
          const command = `aws s3 mv /tmp/${fileName}.csv s3://${bucket}/${key}`;
          const params = {
            InstanceIds: [instanceId],
            DocumentName: 'AWS-RunShellScript',
            Parameters: { commands: [command] },
          };

          try {
            // Step 1: Run SSM Command
            const commandId = await runSSMCommand(params);
            console.log("SSM run command id:", commandId)
         
            // Step 2: Check Command Status
            // console.log("0000000000000000",commandId)
            const isSuccess = await checkCommandStatus(commandId,instanceId);
            // if (!isSuccess) {
            //   console.error('Failed to move movie data to S3.');
            //   return;
            // }
         
            const signedUrl = await s3.getSignedUrlPromise('getObject', {
              Bucket: bucket,
              Key: key + fileName + '.csv',
            });
            console.log("File is found on S3 at the given location", signedUrl);
        
            return res.status(200).send({ signedUrl: signedUrl });

          } catch (error) {
            console.error('Error:', error);
          }
          
        }
        
      } catch (queryError) {
        console.error('Error executing query or writing to file:', queryError);
        return res.status(500).send({ message: "Error executing database query or saving file." });
      }
    } else {
      // Handle other errors (e.g., permission issues)
      console.error('Error checking file existence:', error);
      return res.status(500).send({ message: "Error checking file existence in S3." });
    }
  }
};


const runSSMCommand = async (params) => {
 
  console.log('Running SSM command to upload movie data to S3...');
  const result = await ssm.sendCommand(params).promise();
  return result.Command.CommandId;
};
 
const checkCommandStatus = async (commandId, instanceId) => {
  const params = {
    CommandId: commandId,
    InstanceId: instanceId,
  };
  
  console.log("Checking command status with params:", params);

  try {
    // Check the initial invocation status

    await new Promise((resolve)=>setTimeout(resolve,5000));
    const result = await ssm.getCommandInvocation(params).promise();
    console.log("Results are:", result)
    let status = result.Status;
    // Polling loop to check the command status
    while (status === 'InProgress' || status === 'Pending') {
      console.log(`Command status: ${status}, waiting for completion...`);
      // Wait 500ms before polling again
      await new Promise(resolve => setTimeout(resolve, 500)); // Adjust delay if necessary

      const updatedResult = await ssm.getCommandInvocation(params).promise();
      status = updatedResult.Status;
      console.log(`Command status: ${status}`);
    }

    if (status === 'Success') {
      console.log('SSM command executed successfully.');
      return true;
    } else {
      console.error('SSM command failed with status:', status);
      return false;
    }

  } catch (error) {
    console.error("Error checking command status:", error);
    if (error.code === 'InvocationDoesNotExist') {
      console.error("The specified command invocation does not exist. This could be due to an invalid CommandId or InstanceId.");
    }
    return false;
  }
};

export const cleanCache=async(fund, trnx)=>
  { 
    try
    {
    // trnx = (trnx == 'sip') ? 'sip_registration': "sip_transaction"
    const command = `aws s3 rm --recursive s3://${process.env.BUCKET_NAME}/Application-Download-Pending-TXNS/${fund}/${trnx}/`
    exec(command,(error,stdout,stderr)=>{ 
      if(error){ console.log("-----------errrorrr",error) } 
      if(stderr){ console.log("------stderr-----errrorrr",error) 
      } 
      console.log("Success") 
    }) 
    }catch(e){
      console.log("Error in deleting pending transactions :", e)
    }
  }


 