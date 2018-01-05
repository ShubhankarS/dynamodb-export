const AWS = require('aws-sdk');
const async = require('async');

AWS.config.update({
	region: 'ap-southeast-1'
});

var dynamodb = new AWS.DynamoDB({
	apiVersion: '2012-08-10'
});

const SQS = new AWS.SQS({
	apiVersion: '2012-11-05'
});


//TODO replace queue URL
const QUEUE_URL = "https://sqs.ap-southeast-1.amazonaws.com/test-queue";

const batchSize = 500;

var params = {
	TableName: "test-table",
	Limit: batchSize,
	ExclusiveStartKey: null
};

async.forever(
	function(callback) {
		dynamodb.scan(params, function(err, data) {
			if (err) {
				console.log(err, err.stack, params.ExclusiveStartKey)
				callback(err)
			} else {
				var docs = data.Items;

				docs = docs.map((entry) => {
					//transform documents as needed
				});

				if (!docs || docs.length == 0) {
					return callback("All documents finished!");
				}

				console.log('got docs', docs.length);
				console.log('last eval key', data.LastEvaluatedKey);

				if (data.LastEvaluatedKey) {
					params.ExclusiveStartKey = data.LastEvaluatedKey;
				}

				var toSend = {
					MessageBody: JSON.stringify(docs),
					QueueUrl: QUEUE_URL,
					DelaySeconds: 0
				};

				SQS.sendMessage(toSend, function(err, data) {
					if (err) {
						console.log("Error pushing to queue", QUEUE_URL, err.stack);
						callback(err);
					} else {
						console.log(`Successfully processed ${docs.length} records.`);
						if (!data.LastEvaluatedKey) {
							return callback("All documents finished!");
						} else {
							callback();
						}
					}
				});
			}
		});
	},
	function(err) {
		console.log('err', err);
		console.log('last eval was', params.ExclusiveStartKey);
	}
);