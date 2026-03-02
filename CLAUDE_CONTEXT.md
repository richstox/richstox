# RICHSTOX — Claude Context (Single Source of Truth)

---

## JAK TENTO SOUBOR POUŽÍVAT — KROK ZA KROKEM

### Jak ho přidat do projektu (udělej jen jednou)

1. Stáhni tento soubor do počítače
2. Otevři VS Code
3. Vlevo vidíš složky projektu — najdi nejvyšší složku kde jsou vedle sebe složky `backend/` a `frontend/`
4. Přetáhni tento soubor myší do té nejvyšší složky
5. Otevři terminál ve VS Code (horní menu → Terminal → New Terminal)
6. Napiš tento příkaz a stiskni Enter: `git add CLAUDE_CONTEXT.md`
7. Napiš tento příkaz a stiskni Enter: `git commit -m "Add CLAUDE_CONTEXT.md"`
8. Napiš tento příkaz a stiskni Enter: `git push`
9. Hotovo — soubor je v GitHubu

### Jak ho použít na začátku každého nového chatu s Claudem

1. Otevři claude.ai
2. Klikni na ikonu sponky (📎) vedle textového pole
3. Vyber soubor `CLAUDE_CONTEXT.md`
4. Claude okamžitě zná celý projekt — nepotřebuješ mu nic vysvětlovat

---

## 👤 Uživatel
- Jméno: Richard (kurtarichard@gmail.com)
- Není IT — instrukce musí být vždy krok za krokem, klik za klikem
- Komunikace: česky
- Styl: kompletní instrukce najednou, žádné zbytečné otázky, soubory ke stažení místo copy-paste kódu

---

## 🏗️ Infrastruktura

| Část | Technologie | URL |
|------|-------------|-----|
| Frontend | Expo / React Native Web | https://jocular-faun-27ea7b.netlify.app |
| Backend | FastAPI (Python) | https://richstox-production.up.railway.app |
| Databáze | MongoDB Atlas | cluster: richstox, db: richstox_prod |
| Kód | GitHub | github.com/richstox/richstox (branch: main) |

---

## 📁 Klíčové soubory

```
backend/server.py                           ← hlavní backend, API endpointy
backend/scheduler.py                        ← scheduler daemon
backend/whitelist_service.py                ← Universe Seed logika
backend/services/admin_overview_service.py  ← Admin Panel data
frontend/app/admin.tsx                      ← Admin Panel UI
frontend/contexts/AuthContext.tsx           ← Google OAuth auth
```

---

## 🔐 Autentizace
- Google OAuth přes vlastní backend endpoint `/api/auth/google`
- Admin endpointy: guard přes `get_session_token_from_request(request)` + `is_admin(db, session_token)`
- Frontend posílá: `Authorization: Bearer ${sessionToken}`

---

## ⚙️ Pravidla pro vývoj

### NEPORUŠITELNÁ PRAVIDLA
1. Jeden úkol najednou — nezačínat druhý bez schválení prvního
2. Žádné změny bez Richardova schválení — vždy nejdřív diff návrh, pak čekat na "APPROVED"
3. Scheduler: změny jsou možné jen po explicitním schválení Richarda (diff → APPROVED). Bez schválení se scheduler nemění.
4. Nikdy nevymýšlet endpointy, názvy jobů ani cesty — nejdřív ověřit v kódu

### Workflow pro každou změnu
1. Analyzuj existující kód
2. Pošli diff plán (soubory + co se mění)
3. Čekej na Richardovo schválení
4. Teprve pak implementuj a poskytni soubory ke stažení

---

## 🕐 Timezone pravidlo
- Všechny timestampy do MongoDB: Europe/Prague
- Pattern: `datetime.now(ZoneInfo("Europe/Prague"))`
- Import: `from zoneinfo import ZoneInfo`

---

## 📅 Scheduler — přehled jobů

| Job | Čas | Den |
|-----|-----|-----|
| universe_seed | 04:00 | Neděle |
| price_sync | 04:00 | Po–So |
| sp500tr_update | 04:15 | Po–So |
| fundamentals_sync | 04:30 | Po–So |
| backfill_gaps | 04:45 | Po–So |
| key_metrics | 05:00 | Po–So |
| pain_cache | 05:00 | Po–So |
| backfill_all | 05:00 | Po–So (manual only) |
| peer_medians | 05:30 | Po–So |
| news_refresh | 13:00 | Denně |
| admin_report | 06:00 | Po–So |

---

## 🔄 Stav implementace — po deploy OVĚŘIT

Tyto věci jsou navržené a nakódované, ale je třeba ověřit že fungují v produkci:

- Universe Seed endpoint: `POST /api/admin/jobs/universe-seed`
- `is_manual: True` u universe_seed v `admin_overview_service.py`
- Run Now tlačítko pro Universe Seed v `admin.tsx`

### Ověření po deploy:
1. Otevři Admin Panel
2. Najdi sekci "Not Scheduled Today" → rozbal ji
3. Najdi "universe seed" → musí mít zelené tlačítko **Run Now**
4. Klikni Run Now → musí vrátit `completed` nebo `failed`
5. Zkontroluj MongoDB kolekci `ops_job_runs` → musí být záznam s `triggered_by: "admin_manual"`

### Hotové a otestované:
- Google přihlášení
- Admin Panel (načítání dat)
- Logout
- Scheduler (všechny joby běží automaticky)

### Aktuální stav databáze:
- Databáze je prázdná — tickery ještě nebyly načteny
- Universe Seed je třeba spustit ručně z Admin Panelu po ověření

---

## 🔗 API endpointy — Universe Seed

Frontend volá přesně tuto URL (zkopírováno z `admin.tsx`):
```
POST /api/admin/jobs/universe-seed
```
Vždy používej `/api/admin/jobs/universe-seed` — nikdy bez `/api/` prefixu.

---

## 🚀 Deploy postup — KROK ZA KROKEM

1. Ulož změny ve VS Code (`Ctrl+S` na každém upraveném souboru)
2. Otevři terminál ve VS Code (horní menu → Terminal → New Terminal)
3. Napiš a stiskni Enter: `git add .`
4. Napiš a stiskni Enter: `git commit -m "popis co jsi změnil"`
5. Napiš a stiskni Enter: `git push`
6. Otevři railway.app — počkej až backend zezelená (cca 2 minuty)
7. Otevři netlify.com — počkej až frontend zezelená (cca 2 minuty)
8. Otevři aplikaci a ověř změny

---

## 🛑 STOP podmínky
- Po splnění úkolu STOP — čekat na další instrukci od Richarda
- Nikdy nepokračovat na další úkol bez explicitního pokynu
- Pokud nemůžu číst soubory / běžet bash, STOP a vyžádej si upload konkrétního souboru.
