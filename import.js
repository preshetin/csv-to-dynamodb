import AWS from 'aws-sdk';
import parse from 'csv-parser';

const tableHeaders = [
  'item0', 'item1', 'item2',
  'item3', 'item4', 'item5',
  'item6', 'item7', 'item8',
  'item9', 'item10', 'item11',
  'item12', 'item13', 'item14',
  'item15', 'item16', 'item17',
  'item18', 'licenceId'
];

const s3 = new AWS.S3();
const dynamoDb = new AWS.DynamoDB.DocumentClient();

export const main = (event, context, callback) => {

  const data_arr = [];
  const bucket = event.Records[0].s3.bucket.name;
  const objectkey = event.Records[0].s3.object.key;
  const params = { Bucket: bucket, Key: objectkey };

  console.log('importing file: ', objectkey);

  var file = s3.getObject(params).createReadStream();

  file.pipe(parse(tableHeaders))
    .on('data', function (data) {
      const parsed_data = JSON.parse(JSON.stringify(data));
      data_arr.push(parsed_data);
    })
    .on('end', function () {
      update();
    });

  function update() {
    try {
      data_arr.forEach(item => {
        const params = getParams(item);
        dynamoDb.update(params, (err, data) => {
          if (err) {
            console.error(err);
            throw new Error('Error while updating dynamodb table');
          }
        });
      });
    } catch (err) {
      console.error(err);
    }
  }

  function getParams(item) {
    return {
      TableName: process.env.tableName,
      Key: {
        licenceId: item.licenceId,
        sk: item.item0
      },
      UpdateExpression: "SET item1 = :item1, item2 = :item2",
      ExpressionAttributeValues: {
        ":item1": item.item1 || null,
        ":item2": item.item2 || null
      },
      ReturnValues: "ALL_NEW"
    };
  }
};
