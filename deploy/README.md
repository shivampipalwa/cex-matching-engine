# Deploying

One VM running one `docker compose` stack: Postgres, Redis, `engine`,
`db_writer`, `api`, a one-shot `migrate`, the market-maker `bot`, and Caddy
serving the frontend and proxying the API on the same origin.

Target here is Oracle Cloud's Always Free Ampere tier (2 OCPU / 12 GB / 200 GB
disk, no time limit), but nothing below is Oracle-specific except steps 1–2.

## 1. The VM

Oracle Cloud → Compute → Instances → Create instance.

| Field | Value |
|---|---|
| Image | Ubuntu 24.04 |
| Shape | `VM.Standard.A1.Flex` (Ampere, ARM64) |
| OCPUs / memory | 2 / 12 GB — the whole Always Free Ampere allocation |
| Boot volume | 100 GB (Always Free gives 200 GB total) |
| SSH key | upload your public key |

Two things that will bite you:

- Always Free resources only exist in your **home region**, chosen at signup
  and not changeable.
- `Out of host capacity` on Ampere is routine. Try each availability domain in
  the region, then retry later — it frees up.

## 2. Open 80 and 443

Two separate firewalls, and forgetting the second one is the single most common
reason a fresh Oracle box looks dead.

**Cloud side** — VCN → Security Lists → default → add ingress rules:
source `0.0.0.0/0`, TCP, destination ports `80` and `443`.

**Host side** — Oracle's Ubuntu image ships iptables rules that drop everything
but SSH:

```bash
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 80 -j ACCEPT
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 443 -j ACCEPT
sudo netfilter-persistent save
```

## 3. DNS

At the registrar, point the domain at the instance's public IP:

```
A   @     <public-ip>
A   www   <public-ip>
```

Confirm it has propagated before step 7 — Caddy asks Let's Encrypt for a
certificate on first boot, and that fails if the name doesn't resolve to this
box yet:

```bash
dig +short yourdomain.com
```

**If the domain is on Cloudflare**, leave the records **DNS-only** (grey cloud)
for the first boot. Proxied records intercept the HTTP-01 challenge and the
certificate never issues. Once it's live you can turn the proxy on, but set SSL
mode to *Full (strict)* — anything less breaks the WebSocket upgrade or loops
redirects.

## 4. Docker

```bash
sudo apt update && sudo apt install -y docker.io docker-compose-v2 git
sudo usermod -aG docker $USER
```

Log out and back in for the group to apply.

## 5. Frontend build

The frontend is served as static files by Caddy, so build it wherever and copy
the output up. Building on your Mac keeps Node off the server:

```bash
# on your machine, in the frontend repo
npm run build
scp -r dist ubuntu@<public-ip>:~/fe-dist
```

Do **not** set `VITE_API_BASE` / `VITE_WS_BASE` for this build. A production
build already defaults to the relative `/api` and `wss://<host>/api`, which is
what makes the same-origin setup work.

## 6. Configure

```bash
git clone https://github.com/shivampipalwa/cex-matching-engine.git
cd cex-matching-engine/deploy
cp .env.example .env
```

Edit `.env`:

```
SITE_ADDRESS=yourdomain.com
POSTGRES_PASSWORD=<openssl rand -hex 16>
JWT_SECRET=<openssl rand -hex 32>
BOT_PASSWORD=<openssl rand -hex 16>
FRONTEND_DIST=/home/ubuntu/fe-dist
```

`SITE_ADDRESS` as a bare hostname is what triggers automatic TLS. Use `:80` to
bring the stack up before DNS is ready, then switch it and
`docker compose up -d caddy`.

## 7. Up

```bash
docker compose up -d --build
```

First build compiles the whole Rust workspace on 2 ARM cores — allow 15–20
minutes. Afterwards, layer caching makes rebuilds much shorter unless
`Cargo.toml` changed.

Watch the certificate get issued:

```bash
docker compose logs -f caddy
```

## 8. Claim the admin account

Whoever signs up first is account 1, and `ADMIN_ACCOUNT_ID=1` is the only
account allowed to list markets. By default that's the bot's maker account,
which then lists `SOL-USD` itself — fine if you don't care.

To be the admin yourself, sign up **before** the bot ever starts:

```bash
docker compose up -d --build --scale bot=0

curl -X POST https://yourdomain.com/api/auth/signup \
  -H 'content-type: application/json' \
  -d '{"email":"you@example.com","password":"..."}'
# -> {"token":"..."}

curl -X POST https://yourdomain.com/api/admin/pairs \
  -H "Authorization: Bearer <that token>" \
  -H 'X-Client-Order-Id: 1' -H 'content-type: application/json' \
  -d '{"pair":"SOL-USD"}'

docker compose up -d bot
```

The market must be listed by you in this case — the bot will log
`Could not list SOL-USD — not the admin account` and carry on trading whatever
is already listed.

## 9. Verify

```bash
curl https://yourdomain.com/api/book/SOL-USD     # levels, growing as the bot works
curl https://yourdomain.com/api/candles/SOL-USD?interval=15m
docker compose logs -f bot                       # a trade every few ticks
```

Then open `https://yourdomain.com` and watch the tape move.

## Keeping it alive

**Idle reclamation.** Oracle reclaims an Always Free instance if, over 7 days,
its 95th-percentile CPU *and* network *and* memory are all below 20%. This
stack idling under a slow bot can sit under all three. Options, cheapest
first:

- Lower `BOT_TICK_MS` (e.g. `1500`) so CPU and network stay meaningfully busy.
  Oracle-specific, and the opposite of what a small box wants — every tick is a
  full quote ladder cancelled and reposted, so the tick rate sets how fast
  `orders` and `trades` grow. On a 1GB/slow-disk VM raise it instead.
- Provision 1 OCPU / 6 GB instead of 2/12 — same workload, higher percentages.
- Upgrade the tenancy to Pay As You Go. Always Free resources stay free and
  reclamation no longer applies. Set a budget alert if you do this, since you
  can then create things that *do* bill.

**Storage** is bounded by the retention sweep in `db_writer`
(`ORDER_RETENTION_DAYS` / `TRADE_RETENTION_DAYS`) and by stream trimming behind
the engine and book snapshots. Nothing here grows without limit, so 100 GB is
far more than enough.

## Operating it

```bash
docker compose logs -f engine
docker compose ps
docker compose exec postgres pg_dump -U postgres exchange | gzip > backup.sql.gz

git pull && docker compose up -d --build     # deploy an update
```

Redis is the durable command log, not a cache — it runs with `appendonly yes`
and its volume matters as much as Postgres's. The engine and book snapshots
live in the `snapshots` volume; deleting it is safe but forces a full replay of
whatever is left in the streams.
