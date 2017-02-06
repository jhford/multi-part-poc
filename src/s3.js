let http = require('http');
let https = require('https');
let fs = require('fs');
let crypto = require('crypto');
let streamToBuffer = require('stream-to-buffer');
require('source-map-support').install();

let libxmljs = require('libxmljs');
let aws = require('aws-sdk');
let s3 = new aws.S3({
  region: 'us-east-1',
});
let aws4 = require('aws4');

const BUCKET = process.env.BUCKET || 'multi-part2';
const KEY = process.env.KEY || 'test-file';
const FILE = process.env.FILE || 'rando.dat';

async function fileInfo(file) {
  let md5 = crypto.createHash('md5');
  let sha256 = crypto.createHash('sha256');

  return new Promise((res, rej) => {
    let stream = fs.createReadStream(file);
    let size = 0;

    stream.on('data', data => {
      md5.update(data);
      sha256.update(data);
      size += data.length;
    });

    stream.on('end', () => {
      let md5hash = md5.digest('hex');
      let sha256hash = sha256.digest('hex');
      console.log(`File "${file}" has MD5: ${md5hash}, SHA256: ${sha256hash}`);
      res({
        md5: md5hash,
        sha256: sha256hash,
        size: size,
      });
    });
    
    stream.on('error', err => {
      rej(err);
    });
  });

}

async function createBucket(name=BUCKET) {
  try {
    await s3.createBucket({
      Bucket: name,
      /*CreateBucketConfiguration: {
        LocationConstraint: 'US', // Ensure we use region which only supports v4
      },*/
    }).promise();
  } catch (err) {
    if (err.code !== 'BucketAlreadyOwnedByYou') {
      throw err;
    }
  }
  console.log('created bucket: ' + name);
}

async function removeBucket(name=BUCKET) {
  try {
    await s3.deleteBucket({
      Bucket: name,
    }).promise();
  } catch (err) {
    if (err.code !== 'NoSuchBucket') {
      throw err;
    }
  }
  console.log('removed bucket: ' + name);
}

/**
 * Parse the raw body of an aws response.  This will throw an error if detected
 */
function parseAwsResponse(body) {
  let doc = libxmljs.parseXml(body);
  let rootTagName = doc.root().name();
  // First, let's see 
  if (rootTagName === 'Error') {
    let children = doc.root().childNodes();
    let code, message, requestId, hostId, resource;
    for (let child of children) {
      switch (child.name()) {
        case 'Code':
          code = child.text() || 'AWSProvidedNoCodeError';
          break;;
        case 'Message':
          message = child.text() || 'AWS provided no message';
          break;;
        case 'RequestId':
          requestId = child.text() || undefined;
          break;;
        case 'HostId':
          hostId = child.text() || undefined;
          break;;
        case 'Resource':
          resource = child.text() || undefined;
        default:
          break;;
      }
    }
    let err = new Error(message);
    err.code = code;
    err.resource = resource;
    err.requestId = requestId;
    err.hostId = hostId;
    throw err;
  }

  //Need to handle this:
  //  <?xml version="1.0" encoding="UTF-8"?>
  //<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>multi-part</Bucket><Key>test-file</Key><UploadId>ZOgEVK0dkbQidjy_RoRPKwUYshHz017rM5ayxeGWW_X4W1Ap3vP8wbCNj6qyD9VTRTzAkD9Av5dYCn6yyWyfUjTa54riJ0ZIwWLwmv_SOVJDv33Z1sLHrryKn3LVWb8U</UploadId></InitiateMultipartUploadResult>
}

async function awsRequest(opts) {
  let fullOpts = aws4.sign(opts);
  console.log(JSON.stringify(fullOpts, null, 2));
  return new Promise((res, rej) => {
    let request = https.request(fullOpts);
    request.on('error', rej);

    console.log('making request');

    request.on('response', response => {
      console.log('status code: ' + response.statusCode);
      console.dir(response.headers);
      let chunks = []

      response.on('data', chunk => {
        chunks.push(chunk);
      });

      response.on('end', () => {
        try {
          let body = Buffer.concat(chunks);
          console.log('request complete, ' + body.length + ' bytes');
          if (body.length > 0) {
            res(parseAwsResponse(body));
          } else {
            res();
          }
        } catch (err) {
          rej(err);
        }
      });
    
    });

    request.end();
  });
}

async function normalMultiPart(fileinfo) {
  // http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
  let result = await awsRequest({
    service: 's3',
    region: 'us-east-1',
    protocol: 'https:',
    hostname: `${BUCKET}.s3.amazonaws.com`,
    method: 'POST',
    path: `/${KEY}?uploads`,
    headers: {
    }
  });
  if (result) {
    console.log(result.toString());
  }
  
  return result;
}

async function main() {
  let fileinfo = await fileInfo(FILE);
  await createBucket();
  await normalMultiPart(fileinfo);
  //await removeBucket();
  return 'success';
}

console.log('S3');
main().then(console.log, console.error); 
