#!/usr/bin/env python3
"""
Signalpost baseline: Norwegian company-info agent.

Usage:
  python signalpost.py discover --count 1200 --out orgnrs.txt   # collect org numbers
  python signalpost.py run --input orgnrs.txt --out profiles/   # build/update profiles

Dependencies: pip install httpx
NOTE: endpoints/field names below follow Brreg's public API docs as I know them;
verify against https://data.brreg.no/enhetsregisteret/api/docs before submitting.
"""
import argparse, asyncio, hashlib, json, os, re, sys, time
from html import unescape
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from datetime import datetime, timezone
from pathlib import Path

import httpx

BASE = "https://data.brreg.no"
ENHET = BASE + "/enhetsregisteret/api/enheter/{}"
ROLLER = BASE + "/enhetsregisteret/api/enheter/{}/roller"
REGNSKAP = BASE + "/regnskapsregisteret/regnskap/{}"
LIST = BASE + "/enhetsregisteret/api/enheter"

CONCURRENCY = 5  # be polite to the public API


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def valid_orgnr(n: str) -> bool:
    """Mod-11 checksum for Norwegian organisation numbers (guards against bad input)."""
    if not (n.isdigit() and len(n) == 9):
        return False
    weights = [3, 2, 7, 6, 5, 4, 3, 2]
    s = sum(int(d) * w for d, w in zip(n[:8], weights))
    check = 11 - (s % 11)
    check = 0 if check == 11 else check
    return check != 10 and check == int(n[8])


def fact(field, value, url, source_date=None):
    """Every fact carries evidence: where it came from, when we fetched it, and the source's own date."""
    return {"field": field, "value": value, "source_url": url,
            "source_date": source_date, "retrieved_at": now()}


async def get_json(client, url):
    r = await client.get(url, headers={"Accept": "application/json"})
    if r.status_code == 404:
        return None  # company/filings don't exist: not an error
    r.raise_for_status()
    return r.json()



# ---------------------------------------------------------------------------
# Website step: only runs on the company's OWN site (URL comes from the registry),
# only keeps pages that prove they belong to this company, respects robots.txt.
# ---------------------------------------------------------------------------
WEB_ENABLED = os.getenv("SIGNALPOST_WEB", "1") == "1"   # set SIGNALPOST_WEB=0 to skip websites entirely
UA = "signalpost-agent/0.1"
MAX_PAGES = 3  # homepage + up to 2 contact/about pages: keeps run cost and load low

# --- Run budgets (Builderr limits per 100 companies: 45 min, 2,000 outbound requests) ---
SITE_MIN_DELAY = float(os.getenv("SITE_MIN_DELAY", "1.0"))          # min seconds between requests to the same site
MAX_BACKOFF = 30.0                                                   # never wait longer than this for one Retry-After
TIME_BUDGET = float(os.getenv("SIGNALPOST_TIME_BUDGET", "2400"))    # 40 min; after that, remaining companies get registry-only
REQ_PER_COMPANY = float(os.getenv("SIGNALPOST_REQ_PER_COMPANY", "18"))  # 18 x 100 = 1,800 < 2,000 limit
STATS = {"requests": 0}
RUN = {"n": 0, "start": time.monotonic()}
HOST_LAST, HOST_LOCKS = {}, {}


async def _count_request(request):
    STATS["requests"] += 1       # httpx hook: counts every outbound request (registry, sites, robots.txt, LLM)


def web_budget_ok() -> bool:
    n = max(RUN["n"], 1)
    if time.monotonic() - RUN["start"] > TIME_BUDGET:
        return False
    return STATS["requests"] + 10 <= REQ_PER_COMPANY * n   # +10 = worst case cost of one more website step


def clean_site(raw: str) -> str:
    """Registry website fields are free text: unwrap markdown links like [www.x.no](https://www.x.no), drop junk."""
    raw = (raw or "").strip()
    m = re.search(r"\[[^\]]*\]\((https?://[^)\s]+)\)", raw)
    if m:
        raw = m.group(1)                                   # markdown link: use the target URL
    parts = raw.strip("<>\"' ").split()
    return parts[0] if parts else ""                       # if several URLs/words are listed, take the first


def site_host(u: str) -> str:
    u = clean_site(u)
    u = u if u.startswith("http") else "https://" + u
    return urlparse(u).netloc.lower().removeprefix("www.")


async def polite_get(client, url, crawl_delay=0.0, max_tries=3):
    """GET with per-site spacing (and robots Crawl-delay) and back-off on 429/503 honouring Retry-After.
    Returns a Response (possibly still 429/503 if the site keeps refusing) or None on network errors."""
    host = urlparse(url).netloc.lower()
    lock = HOST_LOCKS.setdefault(host, asyncio.Lock())
    for attempt in range(max_tries):
        async with lock:                                     # one request at a time per site, spaced out
            wait = max(SITE_MIN_DELAY, crawl_delay) - (time.monotonic() - HOST_LAST.get(host, 0.0))
            if wait > 0:
                await asyncio.sleep(wait)
            HOST_LAST[host] = time.monotonic()
        try:
            r = await client.get(url, timeout=15, follow_redirects=True)
        except httpx.HTTPError:
            return None
        if r.status_code not in (429, 503):
            return r
        try:
            delay = float(r.headers.get("retry-after", ""))
        except ValueError:
            delay = 5.0 * (attempt + 1)                      # no usable Retry-After header: back off 5s, 10s, ...
        if delay > MAX_BACKOFF or attempt == max_tries - 1:
            return r                                         # the site wants us gone: give up, caller records it
        await asyncio.sleep(delay)
    return None


# Sites that many unrelated registry entries point at (housing federations, managers, national umbrella bodies).
# Seeded from a 1,200-company sample; extend it by listing more hosts in shared_sites.txt (one per line, next to this file).
SHARED_SITE_HOSTS = {"nbbo.no", "bate.no", "usbl.no", "navn.no", "boligsamvirket.no", "abbl.no", "4h.no", "gobb.no", "advs2.no"}
_shared_file = Path(__file__).with_name("shared_sites.txt")
if _shared_file.exists():
    SHARED_SITE_HOSTS |= {l.strip().lower().removeprefix("www.") for l in
                          _shared_file.read_text(encoding="utf-8-sig").splitlines() if l.strip() and not l.startswith("#")}
SHARED_SITE_MIN = int(os.getenv("SHARED_SITE_MIN", "3"))   # this many registry entries on one site => not one company's own


async def site_is_shared(client, raw: str) -> bool:
    """True if the website is a hub used by several registry entries (so it is not this company's own site)."""
    host = site_host(raw)
    if host in SHARED_SITE_HOSTS:
        return True
    best = 0
    for v in dict.fromkeys([raw, clean_site(raw), host, "www." + host]):   # the spellings the registry might store
        try:
            r = await client.get(LIST, params={"hjemmeside": v, "size": 1})
            total = (r.json().get("page") or {}).get("totalElements", 0) if r.status_code == 200 else 0
        except (httpx.HTTPError, ValueError):
            continue
        if total < 5000:                                          # a huge total means the API ignored the filter
            best = max(best, total)
        if best >= SHARED_SITE_MIN:
            return True
    return False


async def shares_parent_site(client, ent) -> bool:
    """True if the registry lists the PARENT group's website for this subsidiary (so it is not this company's own)."""
    parent = ent.get("overordnetEnhet")
    if not parent or not ent.get("hjemmeside"):
        return False
    try:
        p = await get_json(client, ENHET.format(parent))
    except httpx.HTTPError:
        return False                                         # can't tell: the on-page matching gate still protects us
    ph = (p or {}).get("hjemmeside")
    return bool(ph) and site_host(ph) == site_host(ent["hjemmeside"])


PROVIDERS = {
    "gemini": {
        "url": "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
        "key": "GEMINI_API_KEY",
        "model": os.getenv("GEMINI_MODEL", "gemini-3.8-flash"),  # default if GEMINI_MODEL env var isn't set
    },
    # Optional fallback (free mode, no card): used only if MISTRAL_API_KEY is set.
    "mistral": {
        "url": "https://api.mistral.ai/v1/chat/completions",
        "key": "MISTRAL_API_KEY",
        "model": os.getenv("MISTRAL_MODEL", "mistral-small-latest"),
    },
}


def html_to_text(html: str) -> str:
    html = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", html)  # drop code/styles
    text = re.sub(r"(?s)<[^>]+>", " ", html)                              # drop tags
    return re.sub(r"\s+", " ", unescape(text)).strip()


def strip_www(netloc: str) -> str:
    return netloc.lower().removeprefix("www.")


def match_company(text: str, legal_name: str, orgnr: str):
    """Evidence that a page belongs to THIS company. Returns a reason string or None."""
    # Org number as 123456789 or 123 456 789, not embedded in a longer digit string.
    pat = rf"(?<!\d){orgnr[:3]}\s?{orgnr[3:6]}\s?{orgnr[6:]}(?!\d)"
    if re.search(pat, text):
        return "org number found on page"
    if legal_name and legal_name.lower() in text.lower():
        return "exact legal name found on page"
    return None


# Free LLM tiers allow only a few requests per minute, so calls are spaced out and 429s are retried.
LLM_MIN_INTERVAL = float(os.getenv("LLM_MIN_INTERVAL", "6.5"))   # seconds between LLM calls
LLM_RETRY_SLEEP = float(os.getenv("LLM_RETRY_SLEEP", "10"))      # base wait after a 429
_llm_lock = asyncio.Lock()
_llm_last = 0.0


async def _throttle():
    """Global spacer: only one LLM call starts per LLM_MIN_INTERVAL seconds, however many companies run at once."""
    global _llm_last
    async with _llm_lock:
        wait = LLM_MIN_INTERVAL - (time.monotonic() - _llm_last)
        if wait > 0:
            await asyncio.sleep(wait)
        _llm_last = time.monotonic()


def parse_llm_json(txt: str) -> dict:
    """Take everything from the first { to the last }, so fences or chatter around the JSON don't break parsing."""
    i, j = txt.find("{"), txt.rfind("}")
    return json.loads(txt[i:j + 1]) if i != -1 and j > i else {}


async def llm_extract(client, page_text: str, legal_name: str, orgnr: str) -> dict:
    """One LLM call per company on VERIFIED page text. Returns {} on failure (set SIGNALPOST_DEBUG=1 to see why)."""
    debug = os.getenv("SIGNALPOST_DEBUG") == "1"
    first = os.getenv("LLM_PROVIDER", "gemini")
    order = [first] + [p for p in PROVIDERS if p != first]   # fall back to the other provider
    prompt = (
        f"Company: {legal_name} (org no. {orgnr}). From the page text below return ONLY a JSON object with "
        '"description" (one sentence, what the company does) and "products" (list of up to 5 short strings). '
        "Use null for anything not stated on the page. Do not guess.\n\n" + page_text[:8000]
    )
    for name in order:
        cfg = PROVIDERS[name]
        key = os.getenv(cfg["key"])
        if not key:
            if debug and name == first:
                print(f"[LLM {name}] no {cfg['key']} set in this terminal", file=sys.stderr)
            continue
        for attempt in range(3):                              # retry rate limits before giving up on this provider
            await _throttle()
            try:
                r = await client.post(cfg["url"], headers={"Authorization": f"Bearer {key}"}, timeout=60,
                                      json={"model": cfg["model"], "temperature": 0,
                                            "messages": [{"role": "user", "content": prompt}]})
                if r.status_code == 429:
                    if debug:
                        print(f"[LLM {name}/{cfg['model']}] 429 rate limit, retry {attempt + 1}/3", file=sys.stderr)
                    await asyncio.sleep(LLM_RETRY_SLEEP * (attempt + 1))
                    continue
                if r.status_code != 200:
                    if debug:
                        print(f"[LLM {name}/{cfg['model']}] HTTP {r.status_code}: {r.text[:300]}", file=sys.stderr)
                    break                                     # model/key problem: retrying won't help
                data = parse_llm_json(r.json()["choices"][0]["message"]["content"])
                if data:
                    return data
                if debug:
                    print(f"[LLM {name}] reply had no JSON object", file=sys.stderr)
                break
            except Exception as ex:
                if debug:
                    print(f"[LLM {name}] {type(ex).__name__}: {ex}", file=sys.stderr)
                break
    return {}


async def fetch_site(client, raw_url: str, legal_name: str, orgnr: str):
    """Returns (facts, note). note explains gaps for the explanation field."""
    raw_url = clean_site(raw_url)
    url = raw_url if raw_url.startswith("http") else "https://" + raw_url
    host = strip_www(urlparse(url).netloc)

    # robots.txt once per site. Missing/4xx robots.txt => allowed (standard convention).
    rp = RobotFileParser()
    crawl_delay = 0.0
    rr = await polite_get(client, urljoin(url, "/robots.txt"), max_tries=1)
    if rr is not None and rr.status_code in (429, 503):
        return [], "website asked us to slow down (HTTP 429/503); skipped"
    if rr is not None and rr.status_code < 400:
        rp.parse(rr.text.splitlines())
        crawl_delay = min(float(rp.crawl_delay(UA) or 0), 10.0)   # respect Crawl-delay, capped at 10 s
    else:
        rp.parse([])

    pages, seen, queue, throttled = {}, set(), [url], False   # pages: final_url -> html
    while queue and len(pages) < MAX_PAGES:
        u = queue.pop(0)
        if u in seen or not rp.can_fetch(UA, u):
            continue
        seen.add(u)
        r = await polite_get(client, u, crawl_delay)
        if r is None and u.startswith("https://") and not pages:   # some small sites only answer on plain http
            r = await polite_get(client, "http://" + u[len("https://"):], crawl_delay)
        if r is None:
            continue
        if r.status_code in (429, 503):
            throttled = True                                  # "too many requests": stop crawling this site
            break
        if r.status_code != 200 or "text/html" not in r.headers.get("content-type", ""):
            continue
        pages[str(r.url)] = r.text
        if len(pages) == 1:  # from the homepage, queue same-site contact/about pages only
            for href in re.findall(r'href=["\']([^"\'#]+)', r.text):
                if re.search(r"kontakt|contact|om-oss|about", href, re.I):
                    full = urljoin(str(r.url), href)
                    if strip_www(urlparse(full).netloc) == host:   # never leave the company's domain
                        queue.append(full)

    if not pages:
        if throttled:
            return [], "website asked us to slow down (HTTP 429/503); skipped"
        return [], "website unreachable or blocked by robots.txt"

    # Matching gate: no evidence => record nothing from this site.
    texts = {u: html_to_text(h) for u, h in pages.items()}
    evidence_url, reason = None, None
    for u, t in texts.items():
        reason = match_company(t, legal_name, orgnr)
        if reason:
            evidence_url = u
            break
    if not reason:
        return [], "website could not be verified as this company's, so nothing was taken from it"

    home = next(iter(pages))
    facts = [fact("website_verified", reason, evidence_url)]

    # Email: regex (deterministic), accepted only if its domain matches the company's site domain.
    for u, h in pages.items():
        found = [e for e in re.findall(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+", h)
                 if strip_www(e.split("@")[1]).endswith(host)]
        if found:
            facts.append(fact("email", found[0].lower(), u))
            break

    # Description/products: LLM on the verified text only (skipped quietly when no key is set).
    data = await llm_extract(client, " ".join(texts.values()), legal_name, orgnr)
    if data.get("description"):
        facts.append(fact("description", data["description"], home))
    if data.get("products"):
        facts.append(fact("products", data["products"], home))
    return facts, (None if data else "LLM extraction failed (check key, model name or rate limit; run with SIGNALPOST_DEBUG=1)")


def accounts_facts(acc, a_url):
    """Pick the filing with the LATEST period end (the API may list several years in any order)."""
    filings = [f for f in (acc if isinstance(acc, list) else [acc]) if isinstance(f, dict)]
    if not filings:
        return [], None
    latest = max(filings, key=lambda x: (x.get("regnskapsperiode") or {}).get("tilDato") or "")
    pd = (latest.get("regnskapsperiode") or {}).get("tilDato")
    res = latest.get("resultatregnskapResultat") or {}
    drift = res.get("driftsresultat") or {}
    items = [
        ("accounts_period_end", pd),
        ("revenue_nok", (drift.get("driftsinntekter") or {}).get("sumDriftsinntekter")),
        ("operating_result_nok", drift.get("driftsresultat")),
        ("net_result_nok", res.get("aarsresultat")),
        ("total_assets_nok", (latest.get("eiendeler") or {}).get("sumEiendeler")),
        ("equity_nok", ((latest.get("egenkapitalGjeld") or {}).get("egenkapital") or {}).get("sumEgenkapital")),
    ]
    # Only keep figures that are real numbers/strings; every fact gets the filing's period end as its date.
    return [fact(k, v, a_url, pd) for k, v in items if v is not None and not isinstance(v, (dict, list))], pd


async def build_profile(client, orgnr: str) -> dict:
    facts, notes = [], []
    e_url = ENHET.format(orgnr)
    ent = await get_json(client, e_url)
    # Identity check: the returned record must carry the org number we asked for.
    if not ent or ent.get("organisasjonsnummer") != orgnr:
        return {"orgnr": orgnr, "found": False,
                "explanation": "No matching entity in Enhetsregisteret; nothing recorded to avoid attaching facts to the wrong company."}

    sd = ent.get("registreringsdatoEnhetsregisteret")
    facts.append(fact("name", ent.get("navn"), e_url, sd))
    if ent.get("organisasjonsform"):
        facts.append(fact("legal_form", ent["organisasjonsform"].get("beskrivelse"), e_url, sd))
    if ent.get("naeringskode1"):
        nk = ent["naeringskode1"]
        facts.append(fact("industry", f'{nk.get("kode")} {nk.get("beskrivelse")}', e_url, sd))
    def fmt_addr(a):
        return ", ".join(filter(None, [", ".join(a.get("adresse", [])), a.get("postnummer"), a.get("poststed")]))

    # Many associations have no forretningsadresse, so fall back to beliggenhetsadresse.
    main_addr = ent.get("forretningsadresse") or ent.get("beliggenhetsadresse")
    if main_addr:
        facts.append(fact("address", fmt_addr(main_addr), e_url, sd))
    if ent.get("postadresse") and ent["postadresse"] != main_addr:
        facts.append(fact("postal_address", fmt_addr(ent["postadresse"]), e_url, sd))
    site_is_group = False
    if ent.get("hjemmeside"):
        site_is_group = await shares_parent_site(client, ent) or await site_is_shared(client, ent["hjemmeside"])
        # A subsidiary that lists its parent's site must not publish that site (or anything from it) as its own.
        facts.append(fact("group_website" if site_is_group else "website", clean_site(ent["hjemmeside"]) or ent["hjemmeside"], e_url, sd))
        if site_is_group:
            notes.append("the registry lists a website shared with other organisations or a parent group, so no website facts are published as this company's own")
    if ent.get("antallAnsatte") is not None:
        facts.append(fact("employees", ent["antallAnsatte"], e_url))
    if ent.get("stiftelsesdato"):
        facts.append(fact("founded", ent["stiftelsesdato"], e_url))
    # Extra registry fields: more facts per company = more coverage points.
    for label, key in [("vat_registered", "registrertIMvaregisteret"),
                       ("in_business_register", "registrertIForetaksregisteret"),
                       ("in_voluntary_register", "registrertIFrivillighetsregisteret"),
                       ("last_annual_accounts_year", "sisteInnsendteAarsregnskap"),
                       ("parent_orgnr", "overordnetEnhet"),
                       ("language_form", "maalform")]:
        if ent.get(key) is not None:
            facts.append(fact(label, ent[key], e_url))
    if ent.get("institusjonellSektorkode"):
        sk = ent["institusjonellSektorkode"]
        facts.append(fact("sector", f'{sk.get("kode")} {sk.get("beskrivelse")}', e_url, sd))
    if ent.get("vedtektsfestetFormaal"):
        facts.append(fact("purpose", " ".join(ent["vedtektsfestetFormaal"]), e_url, sd))
    for flag, label in [("konkurs", "bankrupt"), ("underAvvikling", "being_dissolved"),
                        ("underTvangsavviklingEllerTvangsopplosning", "forced_dissolution")]:
        if flag in ent:
            facts.append(fact(label, ent[flag], e_url))

    # Roles (CEO, chair, ...): separate endpoint, same org number key.
    r_url = ROLLER.format(orgnr)
    try:
        roles = await get_json(client, r_url)
    except httpx.HTTPError:
        roles = None                      # keep the rest of the profile if only the roles call fails
    if roles:
        for grp in roles.get("rollegrupper", []):
            for rolle in grp.get("roller", []):
                person = rolle.get("person", {}).get("navn", {})
                pname = " ".join(filter(None, [person.get("fornavn"), person.get("mellomnavn"), person.get("etternavn")]))
                # Role holder is either a person or another company (e.g. auditor firm).
                ent_navn = (rolle.get("enhet") or {}).get("navn")
                holder = pname or (ent_navn if isinstance(ent_navn, str) else (ent_navn or [None])[0])
                if holder and not rolle.get("fratraadt"):  # only current role holders
                    facts.append(fact(f'role:{rolle.get("type", {}).get("beskrivelse")}', holder, r_url,
                                      grp.get("sistEndret")))
    else:
        notes.append("no role data")

    # Latest annual accounts.
    a_url = REGNSKAP.format(orgnr)
    try:
        acc = await get_json(client, a_url)
    except httpx.HTTPError:
        acc = None
    if acc:
        a_facts, a_end = accounts_facts(acc, a_url)
        facts.extend(a_facts)
        sist = str(ent.get("sisteInnsendteAarsregnskap") or "")
        if a_end and sist.isdigit() and int(a_end[:4]) < int(sist):
            # Be honest about staleness instead of presenting old figures as current.
            notes.append(f"Regnskapsregisteret's newest filing ends {a_end}, but Enhetsregisteret lists accounts filed for {sist}")
        if not a_facts:
            notes.append("accounts record had no usable figures")
    else:
        notes.append("no filed accounts found")

    # Website step (optional): uses the registry's own website field, never a search result.
    if WEB_ENABLED and ent.get("hjemmeside") and not site_is_group:
        if not web_budget_ok():
            notes.append("website step skipped to stay within the run's time/request budget")
        else:
            try:
                web_facts, web_note = await fetch_site(client, ent["hjemmeside"], ent.get("navn", ""), orgnr)
                facts.extend(web_facts)
                if web_note:
                    notes.append(web_note)
            except Exception as ex:  # a broken website must never kill the whole profile
                notes.append(f"website step failed: {ex!r}")

    explanation = (f"{len(facts)} facts from Enhetsregisteret/Regnskapsregisteret, "
                   f"keyed on org number {orgnr}." + (f" Gaps: {', '.join(notes)}." if notes else ""))
    return {"orgnr": orgnr, "found": True, "facts": facts, "explanation": explanation}


def fingerprint(facts):
    """Hash of values only (not timestamps) so unchanged data is detected as unchanged."""
    core = sorted((f["field"], json.dumps(f["value"], sort_keys=True, default=str)) for f in facts)
    return hashlib.sha256(json.dumps(core).encode()).hexdigest()


def fkey(f):
    """Identity of a fact = field + value, so several 'role:Styremedlem' entries don't overwrite each other."""
    return (f["field"], json.dumps(f["value"], sort_keys=True, default=str))


def values_by_field(facts):
    out = {}
    for f in facts:
        out.setdefault(f["field"], []).append(json.dumps(f["value"], sort_keys=True, default=str))
    return {k: sorted(v) for k, v in out.items()}


def merge_with_previous(new: dict, path: Path) -> dict:
    """Update logic: unchanged facts keep their original timestamp; changed fields go in a changelog."""
    new["last_checked"] = now()          # when we last verified this profile, even if nothing changed
    new["changes"] = []
    if not path.exists() or not new.get("found"):
        return new
    raw = path.read_bytes()
    try:
        old = json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError:                       # profile written by an earlier run on Windows (cp1252)
        old = json.loads(raw.decode("cp1252"))
    old_by_key = {fkey(f): f for f in old.get("facts", [])}
    for f in new["facts"]:
        o = old_by_key.get(fkey(f))
        if o:
            f["retrieved_at"] = o["retrieved_at"]   # same field AND value as before: not fresh, keep old date
    have = {f["field"] for f in new["facts"]}
    if "website_verified" in have:                           # same verified site, but the LLM failed this time:
        for f in old.get("facts", []):                       # keep the earlier extraction instead of losing it
            if f["field"] in ("description", "products") and f["field"] not in have:
                new["facts"].append(f)
    old_vals, new_vals = values_by_field(old.get("facts", [])), values_by_field(new["facts"])
    changes = []
    for fld in sorted(set(old_vals) | set(new_vals)):
        if old_vals.get(fld) != new_vals.get(fld):  # compares the whole set of values for that field
            changes.append({"field": fld,
                            "old": [json.loads(v) for v in old_vals.get(fld, [])],
                            "new": [json.loads(v) for v in new_vals.get(fld, [])],
                            "at": now()})
    new["changes"] = (old.get("changes", []) + changes)[-50:]   # keep history across runs, capped at 50
    return new


async def run(input_file, out_dir):
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    orgnrs = [l.strip() for l in Path(input_file).read_text(encoding="utf-8-sig").splitlines() if l.strip()]  # utf-8-sig tolerates a BOM from PowerShell
    sem = asyncio.Semaphore(CONCURRENCY)
    done = 0

    RUN["n"], RUN["start"], STATS["requests"] = len(orgnrs), time.monotonic(), 0
    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": "signalpost-agent/0.1"},
                                 event_hooks={"request": [_count_request]}) as client:
        async def one(n):
            nonlocal done
            if not valid_orgnr(n):
                prof = {"orgnr": n, "found": False, "explanation": "Invalid org number (failed format/checksum)."}
            else:
                async with sem:
                    for attempt in range(3):  # simple retry with backoff
                        try:
                            prof = await build_profile(client, n); break
                        except Exception as ex:      # any failure: retry, then still write a result for this company
                            if attempt == 2:
                                prof = {"orgnr": n, "found": False, "explanation": f"Fetch failed: {ex!r}"}
                            await asyncio.sleep(2 ** attempt)
                    prof = merge_with_previous(prof, out / f"{n}.json")
            (out / f"{n}.json").write_text(json.dumps(prof, ensure_ascii=False, indent=2), encoding="utf-8")
            done += 1
            if done % 10 == 0 or done == len(orgnrs):   # show progress even on small retry runs
                print(f"{done}/{len(orgnrs)}", file=sys.stderr)
        await asyncio.gather(*(one(n) for n in orgnrs))
    print(f"Wrote {len(orgnrs)} profiles to {out}")
    el = time.monotonic() - RUN["start"]
    print(f"Outbound requests: {STATS['requests']} ({STATS['requests'] / max(len(orgnrs), 1):.1f} per company; "
          f"limit 20), elapsed {el:.0f}s", file=sys.stderr)


async def discover(count, out_file):
    """Diverse sample: several legal forms x several municipalities, so not just old associations."""
    import random
    forms = ["AS", "ENK", "NUF", "ANS", "DA", "SA", "FLI", "STI", "BRL", "ASA"]
    kommuner = ["0301", "4601", "5001", "1103", "4204", "3301", "3201", "1804", "5401", "1506", "1507", "3407"]
    nums = []
    async with httpx.AsyncClient(timeout=30) as client:
        for form in forms:
            for k in kommuner:
                try:
                    r = await client.get(LIST, params={"organisasjonsform": form, "kommunenummer": k,
                                                       "size": 25, "page": 0})
                    if r.status_code != 200:
                        continue                # unknown municipality code etc: just skip it
                    nums += [e["organisasjonsnummer"] for e in r.json().get("_embedded", {}).get("enheter", [])]
                except httpx.HTTPError:
                    continue
        nums = list(dict.fromkeys(nums))        # de-duplicate, keep order
        page = 0
        while len(nums) < count and page < 100:  # top up with generic pages if the sample was too small
            r = await client.get(LIST, params={"size": 100, "page": page})
            r.raise_for_status()
            nums += [e["organisasjonsnummer"] for e in r.json().get("_embedded", {}).get("enheter", [])]
            nums = list(dict.fromkeys(nums))
            page += 1
    random.shuffle(nums)
    Path(out_file).write_text("\n".join(nums[:count]), encoding="utf-8")
    print(f"Saved {min(count, len(nums))} org numbers to {out_file}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("discover"); d.add_argument("--count", type=int, default=1200); d.add_argument("--out", default="orgnrs.txt")
    r = sub.add_parser("run"); r.add_argument("--input", required=True); r.add_argument("--out", default="profiles")
    a = p.parse_args()
    asyncio.run(discover(a.count, a.out) if a.cmd == "discover" else run(a.input, a.out))
