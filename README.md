This is a repository which contains an example of how to do multipart uploads
using S3, Azure and eventually Google Cloud Storage.

The goal of this repository is to determine whether each service supports
splitting the API endpoint calls for initialization and commiting, as well as
all auth, passing only a generic list of HTTP request metadata for a generic
client to run.
