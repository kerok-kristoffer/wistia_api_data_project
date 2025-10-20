# Infrastructure Setup (Infra)

This document captures the **minimum infrastructure** needed to run the Wistia pipeline and how it was set up. It also records the exact IAM permissions applied in the AWS Console so reviewers can reproduce the environment.

> Scope: S3 buckets (raw/silver/gold), IAM role permissions for Lambda to write to `raw/`, Secrets Manager notes, and quick verification steps. KMS is optional and not required at this stage.

---

## 1) Prerequisites

- AWS account with access to:
  - **S3**
  - **IAM**
  - **Secrets Manager** (read access for the Lambda execution role)
- AWS CLI configured (optional if you use the console exclusively)
- Python 3.10+ and `boto3` (for the bucket bootstrap script)
- Project `.env` (local development) with values such as:
  ```env
  AWS_REGION=us-east-1
  RAW_BUCKET=de-wistia-raw-us-east-1-123456789012
  SILVER_BUCKET=de-wistia-silver-us-east-1-123456789012
  GOLD_BUCKET=de-wistia-gold-us-east-1-123456789012
  S3_PREFIX=wistia/
  ```

> **Naming tip:** Bucket names must be globally unique. The pattern above is just an example; replace the account ID and region accordingly.


---

## 2) Create S3 Buckets (raw / silver / gold)

We use a small Python bootstrap helper so this project stays OS-agnostic and repeatable.

### 2.1 Files
- `infra/bootstrap.py` — idempotent bucket creator with optional prefix “folder” structure.
- `.env` — supplies names and region (or pass flags/args directly).

### 2.2 Example `.env` keys
```env
AWS_REGION=us-east-1
RAW_BUCKET=de-wistia-raw-us-east-1-123456789012
SILVER_BUCKET=de-wistia-silver-us-east-1-123456789012
GOLD_BUCKET=de-wistia-gold-us-east-1-123456789012
S3_PREFIX=wistia/
```

### 2.3 Install deps and run
```bash
# (from repo root)
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -U pip boto3 python-dotenv

python infra/bootstrap.py \
  --raw-bucket   "$RAW_BUCKET" \
  --silver-bucket "$SILVER_BUCKET" \
  --gold-bucket   "$GOLD_BUCKET" \
  --prefix        "$S3_PREFIX" \
  --region        "$AWS_REGION"
```

The script will:
- Create the three buckets if missing.
- Create logical prefixes (pseudo-folders) under the raw bucket, e.g.
  - `s3://$RAW_BUCKET/$S3_PREFIXraw/`
  - `s3://$RAW_BUCKET/$S3_PREFIXraw/events/YYYY/MM/DD/`
  - `s3://$RAW_BUCKET/$S3_PREFIXraw/media/YYYY/MM/DD/`

> **KMS:** We are **not** enabling a customer-managed KMS key yet. SSE-S3 (AES-256) is sufficient for now. If you later adopt SSE-KMS, you’ll add key grants & IAM permissions.


---

## 3) IAM: Lambda Execution Role — Inline S3 Policy

We attached a minimal **inline policy** to the Lambda execution role so it can **list** the raw bucket and **write** objects under `raw/` within the project prefix.

> Console path: **IAM → Roles → (your Lambda role) → Permissions tab → Add permissions → Create inline policy → JSON**

### 3.1 Policy JSON (update placeholders)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListRaw",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::<RAW_BUCKET>"
    },
    {
      "Sid": "WriteRaw",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:AbortMultipartUpload",
        "s3:PutObjectTagging"
      ],
      "Resource": "arn:aws:s3:::<RAW_BUCKET>/<PREFIX>raw/*"
    }
  ]
}
```

**Replace:**
- `<RAW_BUCKET>` → e.g. `de-wistia-raw-us-east-1-123456789012`
- `<PREFIX>` → e.g. `wistia/` (include trailing slash if you use one)

**Why two different ARNs?**
- `ListBucket` targets the **bucket**: `arn:aws:s3:::bucket`
- `PutObject` targets **objects** in a path: `arn:aws:s3:::bucket/prefix/*`

### 3.2 Trust relationship (verify)
- Role **Trust relationships** should include:
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": { "Service": "lambda.amazonaws.com" },
        "Action": "sts:AssumeRole"
      }
    ]
  }
  ```

### 3.3 Secrets Manager access (suggested minimal)
If your Lambda reads a single secret (Wistia API token, media IDs, HMAC key), prefer **least privilege** instead of the broad `SecretsManagerReadWrite`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadSpecificSecrets",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret"
      ],
      "Resource": [
        "arn:aws:secretsmanager:us-east-1:<ACCOUNT_ID>:secret:wistia/api-token-??????",
        "arn:aws:secretsmanager:us-east-1:<ACCOUNT_ID>:secret:wistia/media-ids-??????",
        "arn:aws:secretsmanager:us-east-1:<ACCOUNT_ID>:secret:wistia/hmac-key-??????"
      ]
    }
  ]
}
```

Update ARNs to match your actual secret names. You can either add this as a separate inline policy or attach a managed policy you curate.


---

## 4) Secrets Manager — Secret layout (reference)

Create these secrets (names are examples):
- `wistia/api-token` → JSON: `{ "api_token": "..." }`
- `wistia/media-ids` → JSON: `{ "ids": ["<id1>", "<id2>"] }`
- `wistia/hmac-key` → JSON: `{ "salt": "...", "pepper": "...", "secret": "..." }`

Your Lambda can load the ARNs via env vars (e.g., `WISTIA_SECRET_ARN`, etc.) and fetch them at runtime.


---

## 5) Quick Verification

### 5.1 CLI smoke test (optional)
```bash
aws s3 cp README.md s3://$RAW_BUCKET/$S3_PREFIXraw/test/hello.txt
aws s3 ls s3://$RAW_BUCKET/$S3_PREFIXraw/test/
```

### 5.2 Lambda smoke test
- Deploy a tiny handler that writes a small JSON to `s3://$RAW_BUCKET/$S3_PREFIXraw/test/`.
- Invoke it in the console with a dummy payload.
- Confirm the object shows up in S3. If you get `AccessDenied`, compare the **object key** with the **policy ARN**.


---

## 6) Change Log

- **2025-10-19**: Initial creation of raw/silver/gold buckets via `infra/bootstrap.py`.
- **2025-10-20**: Added inline S3 write policy to Lambda role (list bucket + put objects to `raw/` only).
- **2025-10-20**: Documented minimal Secrets Manager policy for least privilege.

> As the project evolves (Silver/Gold writes, Glue jobs), extend IAM with additional statements targeting those paths (e.g., `arn:aws:s3:::<SILVER_BUCKET>/<PREFIX>silver/*`).


---

## Appendix A: Full S3 Raw Writer Policy (example with concrete values)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListRaw",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::de-wistia-raw-us-east-1-123456789012"
    },
    {
      "Sid": "WriteRaw",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:AbortMultipartUpload",
        "s3:PutObjectTagging"
      ],
      "Resource": "arn:aws:s3:::de-wistia-raw-us-east-1-123456789012/wistia/raw/*"
    }
  ]
}
```
Replace with your own bucket and prefix if different.
