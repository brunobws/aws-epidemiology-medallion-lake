# IAM — Políticas de Acesso (Infrastructure as Code)

Pasta com os documentos JSON das **inline policies** das roles IAM usadas no projeto.
Estes arquivos são a **fonte da verdade** das permissões — qualquer alteração aqui
é automaticamente aplicada na AWS via GitHub Actions (`deploy_iam_policies.yml`).

## Estrutura

```
aws/iam/
├── lambda/
│   └── BronzeBreweryPolicy.json   → role: bws-lambda-role
└── glue/
    └── GlueInlinePolicy.json      → role: bws-glue-role
```

## Roles e seus serviços

| Role | Serviço | Funções / Jobs |
|---|---|---|
| `bws-lambda-role` | AWS Lambda | BronzeApiCaptureInfoDengue, BronzeApiCaptureIbgeMunicipios, BronzeApiCaptureIbgePopulacao, BronzeS3CaptureSinanNotif, CleanFolder |
| `bws-glue-role` | AWS Glue | bronze_to_silver, silver_to_gold |

## Como alterar uma permissão

1. Edite o arquivo `.json` correspondente nesta pasta
2. Faça commit e push para `master`
3. O GitHub Actions detecta a mudança e aplica automaticamente via `aws iam put-role-policy`

## O que é seguro versionar aqui?

✅ **Seguro** — policy documents só definem *permissões*. Não contêm credenciais.  
❌ **Nunca versionar** — Access Keys, Secret Keys, `.env`, `terraform.tfstate`

## Conta AWS

Account ID: `580148408154` | Região: `sa-east-1`
