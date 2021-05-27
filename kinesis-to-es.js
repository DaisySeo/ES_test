/*
 * Sample node.js code for AWS Lambda to upload the JSON documents
 * pushed from Kinesis to Amazon Elasticsearch.
 *
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */

/* == Imports == */
var AWS = require('aws-sdk');
var path = require('path');

/* == Globals == */
var esDomain = {
    region: 'ap-northeast-2',
    endpoint: 'https://search-kinesis-es-4fnrewsr2tzt4mhbaiqj3t4zqm.ap-northeast-2.es.amazonaws.com',
    index: 'new_index',
};
var endpoint = new AWS.Endpoint(esDomain.endpoint);
/*
 * The AWS credentials are picked up from the environment.
 * They belong to the IAM role assigned to the Lambda function.
 * Since the ES requests are signed using these credentials,
 * make sure to apply a policy that allows ES domain operations
 * to the role.
 */
var creds = new AWS.EnvironmentCredentials('AWS');


/* Lambda "main": Execution begins here */
exports.handler = function(event, context) {
    //console.log('kinesis-to-es lambda execution happening', JSON.stringify(event, null, '  '));
    event.records.forEach(function(record) {
        //var jsonDoc = new Buffer(record.data, 'base64');
        var jsonDoc = Buffer.from(record.data, 'base64');
        postToES(JSON.stringify(jsonDoc), context);
    });
    
    return 
//     var jsonDoc = {
//   "invocationId": "d3611af2-3d72-4ee4-880c-65c92cc5564a",
//   "deliveryStreamArn": "arn:aws:firehose:ap-northeast-2:113825065781:deliverystream/kinesis-to-es",
//   "region": "ap-northeast-2",
// };
//     postToES(JSON.stringify(jsonDoc), context);
};


/*
 * Post the given document to Elasticsearch
 */
function postToES(doc, context) {
    var req = new AWS.HttpRequest(endpoint);

    console.log("doc: ", doc);
    
    req.method = 'POST';
    req.path = path.join('/', esDomain.index, '1');
    req.region = esDomain.region;
    req.headers['presigned-expires'] = false;
    req.headers['Host'] = endpoint.host;
    req.headers['Content-Type'] = 'application/json';
    req.body = doc;

    var signer = new AWS.Signers.V4(req , 'es');  // es: service code
    signer.addAuthorization(creds, new Date());

    var send = new AWS.NodeHttpClient();
    send.handleRequest(req, null, function(httpResp) {
        var respBody = "";
        httpResp.on('data', function (chunk) {
            respBody += chunk;
        });
        httpResp.on('end', function (chunk) {
            console.log('Response: ' + respBody);
            context.succeed('Lambda added document ' + doc);
        });
    }, function(err) {
        console.log('Error: ' + err);
        context.fail('Lambda failed with error ' + err);
    });
}
