First create a kms key


```
aws kms create-key \
  --description "CMK for S3 Tables Iceberg encryption" \
  --key-usage ENCRYPT_DECRYPT \
  --origin AWS_KMS \
  --region eu-central-1 \
  --profile personal
```

response
```
{
    "KeyMetadata": {
        "AWSAccountId": "..",
        "KeyId": "..",
        "Arn": "..",
        "CreationDate": "2026-05-01T11:30:20.151000+02:00",
        "Enabled": true,
        "Description": "CMK for S3 Tables Iceberg encryption",
        "KeyUsage": "ENCRYPT_DECRYPT",
        "KeyState": "Enabled",
        "Origin": "AWS_KMS",
        "KeyManager": "CUSTOMER",
        "CustomerMasterKeySpec": "SYMMETRIC_DEFAULT",
        "KeySpec": "SYMMETRIC_DEFAULT",
        "EncryptionAlgorithms": [
            "SYMMETRIC_DEFAULT"
        ],
        "MultiRegion": false,
        "CurrentKeyMaterialId": "..."
    }
}
```



Create human friendly alias

```
aws kms create-alias \
  --alias-name alias/iceberg-s3tables-key \
  --target-key-id ... \
  --region eu-central-1 \
  --profile personal
```


Create an S3Tables bucket in the AWS S3Tables UI.
Encrypt the table with AWS KMS (NOT SSE!). Choose the key id created above.

create a namespace in the table bucket

```
aws s3tables create-namespace \
  --table-bucket-arn <s3_table_bucket_arn> \
  --namespace demo \
  --region eu-central-1 \
  --profile personal
```


Get key Policy 

```
aws kms get-key-policy \
  --key-id c8c8c71f-b88c-4b7d-b681-fda958561d11 \
  --policy-name default \
  --profile personal \
  --region eu-central-1 \
  --output text > key_policy.json
```

Add the following JSON to the statement array

```
{
    "Sid": "AllowS3TablesMaintenance",
    "Effect": "Allow",
    "Principal": {
        "Service": "maintenance.s3tables.amazonaws.com"
    },
    "Action": [
        "kms:GenerateDataKey",
        "kms:Decrypt",
        "kms:DescribeKey"
    ],
    "Resource": "*"
}
```

Push the policy back up. I had to flatten the JSON for some reason.
```
aws kms put-key-policy \
  --key-id <key_id> \
  --policy-name default \
  --profile personal \
  --region eu-central-1 --policy '{"Version":"2012-10-17","Id":"key-default-1","Statement":[{"Sid":"Enable IAM User Permissions","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::<accound_id>:root"},"Action":"kms:*","Resource":"*"},{"Sid":"AllowS3TablesMaintenance","Effect":"Allow","Principal":{"Service":"maintenance.s3tables.amazonaws.com"},"Action":["kms:GenerateDataKey","kms:Decrypt","kms:DescribeKey"],"Resource":"*"}]}'
``` 

Run the `create_and_encrypted_table.py` script



