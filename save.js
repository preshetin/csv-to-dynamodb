import fetch from 'node-fetch';
import AWS from 'aws-sdk';

const s3 = new AWS.S3();

export const main = (event, context, callback) => {
  fetch(event.image_url)
    .then((response) => {
      if (response.ok) {
        return response;
      }
      return Promise.reject(new Error(
            `Failed to fetch ${response.url}: ${response.status} ${response.statusText}`));
    })
    .then(response => response.buffer())
    .then(buffer => (
      s3.putObject({
        Bucket: process.env.BUCKET,
        Key: `${event.key}-${Date.now()}`,
        Body: buffer,
      }).promise()
    ))
    .then(v => callback(null, v), callback);
};

