# RICHSTOX Disk Space Report
**Datum:** 2026-02-25

## Aktuální stav disku

```
/dev/nvme0n6    9.8G  9.6G  195M  99% /app
```

### Proč je disk tak malý?

Emergent hosting poskytuje **dedikovaný 10GB NVMe volume** (`/dev/nvme0n6`) pro aplikační data. Tento volume je **sdílený** mezi více mount pointy:

| Mount Point | Velikost | Popis |
|-------------|----------|-------|
| `/app` | ~654 MB | Zdrojový kód aplikace |
| `/data/db` | **~8.1 GB** | MongoDB data |
| `/root` | ~864 MB | Home adresář |
| `/var/log` | ~9 MB | System logy |
| `/etc/supervisor` | < 1 MB | Supervisor config |

**Hlavní spotřebitel: MongoDB databáze (`richstox_prod`) zabírá 8.1 GB z 10 GB.**

---

## Co lze bezpečně smazat

### 1. node_modules (572 MB) - BEZPEČNÉ SE SMAZAT
```bash
rm -rf /app/frontend/node_modules
yarn install --production --frozen-lockfile  # Reinstall bez dev dependencies
```
**Riziko:** Žádné pro produkci. Lze kdykoliv reinstalovat.
**Úspora:** ~200-300 MB (produkční deps jsou menší)

### 2. .git (1.4 MB) - BEZPEČNÉ SE SMAZAT
```bash
rm -rf /app/.git
```
**Riziko:** Ztratíte lokální git historii. Emergent platform si drží svou kopii.
**Úspora:** ~1.4 MB (po předchozím čištění už je malý)

### 3. Python cache (760 KB) - BEZPEČNÉ SE SMAZAT
```bash
find /app/backend -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
```
**Riziko:** Žádné. Automaticky se regeneruje.
**Úspora:** ~760 KB

### 4. Build artifacts - BEZPEČNÉ SE SMAZAT
```bash
rm -rf /app/frontend/.next 2>/dev/null
rm -rf /app/frontend/.expo 2>/dev/null
rm -rf /app/frontend/.metro-cache 2>/dev/null
```
**Riziko:** Pomalejší první load, automaticky se regeneruje.
**Úspora:** Variable

---

## Jak navýšit disk na Emergent hostingu

### Krok 1: Kontaktujte Emergent Support
Emergent platform nepodporuje self-service disk resize. Musíte:
1. Otevřít support ticket
2. Požádat o větší volume size

### Krok 2: Alternativní řešení - External MongoDB
Pokud potřebujete více prostoru pro data:
1. **MongoDB Atlas** (cloud) - Free tier má 512 MB, placené plány mají neomezené místo
2. Změňte `MONGO_URL` v `/app/backend/.env` na externí cluster
3. Migrujte data pomocí `mongodump/mongorestore`

### Krok 3: Data archivace
Pro historická data starší než X let:
1. Vytvořte archivní collection (`stock_prices_archive`)
2. Přesuňte stará data z hlavní collection
3. Exportujte archiv do S3/externího storage

---

## Doporučení pro okamžité uvolnění místa

**Rychlý script pro uvolnění ~300 MB:**
```bash
# 1. Vyčistit Python cache
find /app/backend -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null

# 2. Vyčistit build artifacts
rm -rf /app/frontend/.expo /app/frontend/.metro-cache 2>/dev/null

# 3. Truncate staré logy
truncate -s 0 /var/log/supervisor/*.log 2>/dev/null

# 4. Reinstall node_modules bez dev dependencies (největší úspora)
cd /app/frontend && rm -rf node_modules && yarn install --production
```

---

## Monitoring příkazy

```bash
# Celkový stav disku
df -h /app

# Top spotřebitelé
du -sh /app/* /data/db /root 2>/dev/null | sort -rh

# MongoDB collections breakdown
mongo richstox_prod --eval "db.stats()"
```

---

## Závěr

Disk není "jen 10 GB" - je to **sdílený volume pro celou aplikaci včetně MongoDB**. Hlavní spotřebitel je databáze (8.1 GB). Pro production provoz doporučuji:

1. **Krátkodobě:** Vyčistit dev artifacts (node_modules, cache)
2. **Střednědobě:** Archivovat historická data starší než 10 let
3. **Dlouhodobě:** Migrace na MongoDB Atlas s větší kapacitou
