# Connecting GitHub Actions to AWS (OIDC, no stored keys)

GitHub Actions gets short-lived AWS credentials by exchanging its OIDC token for a role
in your account. Only this repository's `aws-dev` environment can assume the role, and
you can require your own approval on that environment before anything runs.

## 1. Create the role and state bucket (once, about 2 minutes)

Option A, AWS console: CloudFormation, then Create stack, then upload
`infra/bootstrap/github-oidc.yaml`, name the stack `gridlake-bootstrap`, tick
"I acknowledge that AWS CloudFormation might create IAM resources with custom names",
and create it.

Option B, AWS CLI (signed in to the account):

```bash
aws cloudformation deploy \
  --template-file infra/bootstrap/github-oidc.yaml \
  --stack-name gridlake-bootstrap \
  --capabilities CAPABILITY_NAMED_IAM
aws cloudformation describe-stacks --stack-name gridlake-bootstrap --query "Stacks[0].Outputs"
```

If the stack fails because the GitHub OIDC provider already exists, re-run it with the
parameter `CreateOidcProvider=false`.

## 2. Configure the repository

In GitHub, open the repo, then Settings:

1. Environments, New environment `aws-dev`. Under "Required reviewers" add yourself.
2. Secrets and variables, Actions, Variables tab:
   * `AWS_DEPLOY_ROLE_ARN` = the `DeployRoleArn` stack output
   * `TF_STATE_BUCKET` = the `StateBucket` stack output

## 3. Deploy

Actions, then **deploy**, then Run workflow. Leave "apply" unticked to see a plan only; tick it
to apply after you approve the environment. To tear everything down, run
`terraform destroy` with the same backend settings.

## What the role can do

`PowerUserAccess` (every service except IAM and Organizations) plus IAM rights limited to
roles named `gridlake-*`. Tighten further for production by replacing `PowerUserAccess`
with service-scoped policies.

## Cost note

Idle cost is small but not zero: KMS keys (about $1/month each), Kinesis on-demand stream
hours, the hourly Step Functions schedule and Lambda invocations, CloudWatch logs and S3
storage. Destroy the stack when you are not demoing it.
