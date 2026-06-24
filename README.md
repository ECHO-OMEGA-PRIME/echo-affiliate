# Echo Affiliate

> AI-powered affiliate & referral program management for ECHO Prime (v1.0.0).
> Programs, affiliate applications, tracking links and creatives, conversion
> tracking, payouts, and fraud rules — a Cloudflare Worker on D1 + KV.

Private to Echo Prime Technologies.

## Flow

You run **programs**. Partners **apply** to become **affiliates**, get **tracking
links** and **creatives**, and drive **conversions** (recorded via `/convert`).
**Fraud rules** screen activity; approved earnings become **payouts**. Analytics,
a **leaderboard**, and **AI insights** show what's working.

## API (auth: `X-Echo-API-Key`)

| Route | Purpose |
|---|---|
| `/` , `/health` | Service info / liveness |
| `/programs` | Affiliate programs |
| `/affiliates` | Affiliate accounts |
| `/apply` | Submit an affiliate application |
| `/links` | Tracking links |
| `/creatives` | Marketing creatives |
| `/convert` | Record a conversion (tracking endpoint) |
| `/conversions` | Conversion records |
| `/payouts` · `/payouts/generate` | Payouts (list / generate) |
| `/fraud-rules` | Fraud-detection rules |
| `/analytics/overview` · `/trends` · `/leaderboard` | Analytics |
| `/ai/insights` | AI-generated insights |
| `/activity` | Activity log |
| `/export` | Data export |

Each resource path responds to `GET` (list/read) and `POST` (create), plus
`PUT`/`DELETE` on `/:id` where applicable.

## Bindings

`DB` (D1), KV cache, and service bindings — declared in `wrangler.toml`.

## Develop

```bash
npm install
npx wrangler dev       # local Worker
npx wrangler deploy    # deploy
```

Secrets/bindings live in `wrangler.toml` / the Cloudflare dashboard — never commit them.

## License

Proprietary — © Echo Prime Technologies. All rights reserved.
