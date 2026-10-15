# Arrow recipe

This recipe derives from ConanCenter `arrow/24.0.0` revision
`c0aacc43b72b6013b1bcc1d75f4747dc`. It downloads the SHA256-verified Apache source
and applies the patches listed in `conandata.yml`.

The S3 client disables HTTP redirects whenever an explicit endpoint is supplied.
OtterStax requires such an endpoint for every S3 alias. This keeps HTTP responses
from sending requests to a destination outside the administrator configuration.
Arrow's public `S3Options` does not expose the AWS client's redirect policy.

Export this recipe before installing application dependencies:

```sh
conan export cmake/conan-recipes/arrow --version=24.0.0
```

`tests/test_s3_tls.py` checks the configured destination and separate HTTP/HTTPS
servers that must receive no redirected requests during downloads or uploads.
It also checks CA/hostname validation, successful ingestion and staging cleanup.

Sources: [Arrow S3 client construction](https://github.com/apache/arrow/blob/apache-arrow-24.0.0/cpp/src/arrow/filesystem/s3fs.cc),
[AWS redirect policy](https://docs.aws.amazon.com/sdk-for-cpp/latest/api/aws-cpp-sdk-core/html/namespace_aws_1_1_client.html).
