let http = require('http');
let https = require('https');
let urllib = require('url');
let fs = require('fs');
let crypto = require('crypto');
let streamToBuffer = require('stream-to-buffer');
let _ = require('lodash');
let assert = require('assert');
require('source-map-support').install();

let libxmljs = require('libxmljs');
let xml2json = require('xml2json');
let aws = require('aws-sdk');
let s3 = new aws.S3({
  region: 'us-east-1',
});
let aws4 = require('aws4');

const BUCKET = process.env.BUCKET || 'multi-part2';
const KEY = process.env.KEY || 'test-file';
const FILE = process.env.FILE || 'rando.dat';
const REGION = process.env.REGION || 'us-east-1';
// Default to 25mb
const CHUNKSIZE = Number.parseInt(process.env.CHUNKSIZE) || 1024 * 1024 * 25

// See https://github.com/nodejs/node/issues/2363#issuecomment-278498852
function parseSession(buf) {
  return {
    sessionId: buf.slice(17, 17+32).toString('hex'),
    masterKey: buf.slice(51, 51+48).toString('hex')
  };
}

function wiresharkRequest(req) {
  req.once('socket', (s) => {
    s.once('secureConnect', () => {
      let session = parseSession(s.getSession());
      // session.sessionId and session.masterKey should be hex strings
      fs.appendFileSync('sslkeylog.log', `RSA Session-ID:${session.sessionId} Master-Key:${session.masterKey}\n`);
    });
  });
}

async function fileInfo(file) {
  let chunkhashes = [];
  let size = fs.statSync(file).size;
  let fullChunks = size / CHUNKSIZE;
  let partialChunk = size % CHUNKSIZE;

  let md5 = crypto.createHash('md5');
  let sha256 = crypto.createHash('sha256');

  // Position in the current chunk
  let positionInC;
  let cSha256;
  let cMd5;

  function resetCHash() {
    if (cMd5 && cSha256) {
      chunkhashes.push({
        md5: cMd5.digest('hex'),
        sha256: cSha256.digest('hex'),
        size: positionInC,
      });
    }
    positionInC = 0;
    cSha256 = crypto.createHash('sha256');
    cMd5 = crypto.createHash('md5');
  }

  function updateCHash(buf) {
    cSha256.update(buf);
    cMd5.update(buf);
    positionInC += buf.length;
  }

  resetCHash();

  return new Promise((res, rej) => {
    let stream = fs.createReadStream(file);
    let size = 0;

    stream.on('data', data => {
      md5.update(data);
      sha256.update(data);
      size += data.length;

      let bytesProcessed = 0;

      // The simple case is that the entire data message is midway through a
      // chunk.  We'll only do more complex processing in othercases
      if (data.length + positionInC < data.length) {
        updateCHash(data);
      } else {
        // Look at the buffer starting from here
        let offset = 0;
        let buf = data;

        while (data.length - offset > 0) {
          buf = data.slice(offset);
          let bytesLeftInC = CHUNKSIZE - positionInC - offset;
          if (bytesLeftInC > data.length - offset) {
            // we're reading to the end of the message
            offset = data.length;
            updateCHash(buf);
          } else {
            let myBuf = data.slice(offset, offset + bytesLeftInC);
            updateCHash(myBuf);
            resetCHash();
            offset += bytesLeftInC
            buf = buf.slice(offset);
          }
        }
      }
    });

    stream.on('end', () => {
      let md5hash = md5.digest('hex');
      let sha256hash = sha256.digest('hex');
      res({
        md5: md5hash,
        sha256: sha256hash,
        size: size,
        chunkhashes: chunkhashes
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
  //console.log('created bucket: ' + name);
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
  //console.log('removed bucket: ' + name);
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

    if (err.code === 'SignatureDoesNotMatch') {
      for (let child of children) {
        switch (child.name()) {
          case 'AWSAccessKeyId':
            err.accessKeyId = child.text() || undefined;
            break;;
          case 'StringToSign':
            err.stringToSign = child.text() || undefined;
            break;;
          case 'CanonicalRequest':
            err.canonicalRequest = child.text() || undefined;
            break;;
          default:
            break;;
        }
      }
    }
    throw err;
  }

  return xml2json.toJson(body, {object: true});
}

async function awsRequest(opts) {
  opts = _.defaults({}, opts);
  opts.service = 's3';
  opts.region = REGION;
  opts.protocol = 'https:';
  opts.signQuery = true;
  opts = aws4.sign(opts);
  console.log(opts);

  //console.log(JSON.stringify(opts, null, 2));
  return new Promise((res, rej) => {
    let request = https.request(opts);

    wiresharkRequest(request);

    request.on('error', rej);

    //console.log('making request');

    request.on('response', response => {
      //console.log('status code: ' + response.statusCode);
      ////console.dir(response.headers);
      let chunks = []

      response.on('data', chunk => {
        chunks.push(chunk);
      });

      response.on('end', () => {
        try {
          let body = Buffer.concat(chunks);
          //console.log('request complete, ' + body.length + ' bytes');
          if (body.length > 0) {
            let response = parseAwsResponse(body);
            res(response);
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

async function uploadPart(partinfo, bodyStream) {
  let method = partinfo.method;
  let url = partinfo.url;
  let headers = partinfo.headers;
  let uploadId = partinfo.uploadId;
  let partnum = partinfo.partnum;
  let size = partinfo.size;
  let sha256 = partinfo.sha256;
  let md5 = partinfo.md5;

  //console.log('uploading part ' + url);
  //console.dir(partinfo);

  return new Promise((res, rej) => {
    let bitsandbobs = urllib.parse(url);
    bitsandbobs.headers = headers;
    bitsandbobs.method = method;
    //console.log('Bits and bobs:');
    //console.dir(bitsandbobs);
    //let request = https.request(bitsandbobs);

    let reqOpts = {
      service: 's3',
      region: REGION,
      protocol: 'https:',
      hostname: `${BUCKET}.s3.amazonaws.com`,
      method: 'PUT',
      path: `/${KEY}?partNumber=${partnum}&uploadId=${uploadId}`,
      headers: {
        // case matters to aws4 library :/
        'X-Amz-Content-Sha256': sha256,
        'content-length': size,
        'content-type': 'application/octet-stream',
        'content-md5': md5,
        // No metadata on part upload
        //'x-amz-meta-header-from-upload-part': 'true',
      },
      signQuery: true,
    }

    let signedOpts = aws4.sign(reqOpts);
    console.log('Signed Part Upload Opts');
    console.dir(signedOpts);
    let request = https.request(signedOpts);

    wiresharkRequest(request);

    request.on('error', rej);

    request.on('response', response => {
      response.on('error', rej);
      let body = [];

      response.on('data', chunk => {
        body.push(chunk);
      });

      response.on('end', () => {
        try {
          if (response.statusCode >= 200 && response.statusCode < 300) {
            res(response.headers.etag);
          } else {
            body = Buffer.concat(body);
            ////console.log(body.toString());
            parseAwsResponse(body);
            // Above line should throw because we only expect a body if there's an error
            rej();
          }
        } catch (err) {
          rej(err);
        }
      });
    });

    let uploadedB = 0;
    let uploadedH = crypto.createHash('sha256');
    bodyStream.on('data', chunk => {
      uploadedB += chunk.length;
      uploadedH.update(chunk);
    });

    bodyStream.on('end', () => {
      console.log('Uploaded ' + uploadedB + ' bytes with SHA256 of ' + uploadedH.digest('hex'));
    });

    bodyStream.pipe(request);

  });
}


function s3PartInfo(opts, list) {
  let reqOpts = {
    service: 's3',
    region: REGION,
    protocol: 'https:',
    hostname: `${opts.bucket}.s3.amazonaws.com`,
    method: 'PUT',
    path: `/${opts.key}?partNumber=${opts.partnum}&uploadId=${opts.uploadId}`,
    headers: {
      // case matters to aws4 library :/
      'X-Amz-Content-Sha256': opts.sha256,
      'content-length': opts.size,
      'content-type': 'application/octet-stream',
      // No metadata on part upload
      //'x-amz-meta-header-from-upload-part': 'true',
    }
  }

  let signedRequest = aws4.sign(reqOpts)

  list.add(
    signedRequest.method,
    signedRequest.protocol + '//' + reqOpts.hostname + reqOpts.path,
    signedRequest.headers,
    opts.uploadId,
    opts.partnum,
    opts.size,
    opts.sha256,
    opts.md5,
  );
}

function completeUploadBody(etags) {

}

class RequestList {
  constructor() {
    this.requests = [];
  }

  add(method, url, headers, uploadId, partnum, size, sha256, md5) {
    this.requests.push({method, url, headers, uploadId, partnum, size, sha256, md5});
  }

  display() {
    for (let r of this.requests) {
      //console.log(`METHOD: ${r.method} URL: ${r.url} HEADERS: ${JSON.stringify(r.headers, null, 2)}`);
    }
  }
}

async function normalMultiPart(fileinfo) {
  let reqList = new RequestList();
  // This is back to being done on the server
  // http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadInitiate.html
  let initiateUpload = await awsRequest({
    hostname: `${BUCKET}.s3.amazonaws.com`,
    method: 'POST',
    path: `/${KEY}?uploads`,
    headers: {
      // I want to see which metadata headers from which calls end up staying
      // on the final object
      'x-amz-meta-header-from-initiate': 'true',
    }
  });

  let uploadId = initiateUpload.InitiateMultipartUploadResult.UploadId;
  //console.log('Got UploadID of ' + uploadId);

  let i = 1;
  for (let chunk of fileinfo.chunkhashes) {
    let chunkInfo = s3PartInfo({
      bucket: BUCKET,
      key:KEY,
      partnum: i,
      uploadId: uploadId,
      size: chunk.size,
      sha256: chunk.sha256,
      md5: chunk.md5,
    }, reqList);
    i++;
  }

  reqList.display();

  // not doing this with the RequestList class to ensure that we
  // can do the requests using serialised data
  try {
    for (let x = 0 ; x < reqList.requests.length ; x++) {
      let start = x * CHUNKSIZE;
      let end = start + CHUNKSIZE - 1;
      
      // ugly, messy code but i just want to double check that we're
      // correctly getting offsets, etc
      let lala = crypto.createHash('sha256');
      let lalasize = 0;
      let tohash = fs.createReadStream(FILE, {start, end});
      tohash.on('data', chunk => {
        lalasize += chunk.length;
        lala.update(chunk);
      });

      tohash.on('end', () => {
        //console.log(`True Size: ${lalasize} True sha256: ${lala.digest('hex')}`);
      });

      let rs = fs.createReadStream(FILE, {start, end});

      let result = await uploadPart(reqList.requests[x], rs);
      ////console.dir(result);
      //console.log('uploaded part ' + x);
    }
  } catch (err) {
    //console.log('==================================');
    //console.log('Aborting failed upload');
    ////console.log(JSON.stringify(err, null, 2));
    //console.dir(err);
    let abortUpload = await awsRequest({
      hostname: `${BUCKET}.s3.amazonaws.com`,
      method: 'DELETE',
      path: `/${KEY}?uploadId=${uploadId}`,
      headers: {
        // I want to see which metadata headers from which calls end up staying
        // on the final object
        'x-amz-meta-header-from-abort': 'true',
      }
    });
    throw err;
  }

  
}

async function main() {
  let fileinfo = await fileInfo(FILE);
  //console.log(fileinfo);
  await createBucket();
  await normalMultiPart(fileinfo);
  //await removeBucket();
  return 'success';
}

console.log('S3');
main().then(console.log, console.error); 
