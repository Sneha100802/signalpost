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
import argparse, asyncio, hashlib, json, os, re, sys
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

PROVIDERS = {
    "gemini": {
        "url": "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
        "key": "GEMINI_API_KEY",
        "model": os.getenv("GEMINI_MODEL", "gemini-3-flash"),  # confirm exact Flash model name in AI Studio
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


async def llm_extract(client, page_text: str, legal_name: str, orgnr: str) -> dict:
    """One LLM call per company on VERIFIED page text. Returns {} on any failure."""
    first = os.getenv("LLM_PROVIDER", "gemini")
    order = [first] + [p for p in PROVIDERS if p != first]   # fall back to the other provider on rate limits
    prompt = (
        f"Company: {legal_name} (org no. {orgnr}). From the page text below return ONLY a JSON object with "
        '"description" (one sentence, what the company does) and "products" (list of up to 5 short strings). '
        "Use null for anything not stated on the page. Do not guess.\n\n" + page_text[:8000]  # cap = cheap + fewer 429s
    )
    for name in order:
        cfg = PROVIDERS[name]
        key = os.getenv(cfg["key"])
        if not key:
            continue  # provider not configured
        try:
            r = await client.post(cfg["url"], headers={"Authorization": f"Bearer {key}"}, timeout=60,
                                  json={"model": cfg["model"], "temperature": 0,
                                        "messages": [{"role": "user", "content": prompt}]})
            r.raise_for_status()
            txt = r.json()["choices"][0]["message"]["content"].strip()
            txt = re.sub(r"^```(?:json)?|```$", "", txt).strip()  # strip markdown fences if the model adds them
            return json.loads(txt)
        except Exception:
            continue  # 429 / bad JSON: try the next provider
    return {}


async def fetch_site(client, raw_url: str, legal_name: str, orgnr: str):
    """Returns (facts, note). note explains gaps for the explanation field."""
    url = raw_url if raw_url.startswith("http") else "https://" + raw_url
    host = strip_www(urlparse(url).netloc)

    # robots.txt once per site. 4xx/5xx or unreachable robots.txt => treated as allowed (standard convention).
    rp = RobotFileParser()
    try:
        rr = await client.get(urljoin(url, "/robots.txt"), timeout=10, follow_redirects=True)
        rp.parse(rr.text.splitlines() if rr.status_code < 400 else [])
    except httpx.HTTPError:
        rp.parse([])

    pages, seen, queue = {}, set(), [url]   # pages: final_url -> html
    while queue and len(pages) < MAX_PAGES:
        u = queue.pop(0)
        if u in seen or not rp.can_fetch(UA, u):
            continue
        seen.add(u)
        try:
            r = await client.get(u, timeout=15, follow_redirects=True)
        except httpx.HTTPError:
            continue
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

    # Description/products: LLM on the verified text only.
    data = await llm_extract(client, " ".join(texts.values()), legal_name, orgnr)
    if data.get("description"):
        facts.append(fact("description", data["description"], home))
    if data.get("products"):
        facts.append(fact("products", data["products"], home))
    return facts, (None if data else "LLM extraction unavailable (no key or rate limit)")


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
    if ent.get("hjemmeside"):
        facts.append(fact("website", ent["hjemmeside"], e_url, sd))
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
    roles = await get_json(client, r_url)
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
    if WEB_ENABLED and ent.get("hjemmeside"):
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
    old = json.loads(path.read_text())
    old_by_key = {fkey(f): f for f in old.get("facts", [])}
    for f in new["facts"]:
        o = old_by_key.get(fkey(f))
        if o:
            f["retrieved_at"] = o["retrieved_at"]   # same field AND value as before: not fresh, keep old date
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
    orgnrs = [l.strip() for l in Path(input_file).read_text().splitlines() if l.strip()]
    sem = asyncio.Semaphore(CONCURRENCY)
    done = 0

    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": "signalpost-agent/0.1"}) as client:
        async def one(n):
            nonlocal done
            if not valid_orgnr(n):
                prof = {"orgnr": n, "found": False, "explanation": "Invalid org number (failed format/checksum)."}
            else:
                async with sem:
                    for attempt in range(3):  # simple retry with backoff
                        try:
                            prof = await build_profile(client, n); break
                        except httpx.HTTPError as ex:
                            if attempt == 2:
                                prof = {"orgnr": n, "found": False, "explanation": f"Fetch failed: {ex!r}"}
                            await asyncio.sleep(2 ** attempt)
                    prof = merge_with_previous(prof, out / f"{n}.json")
            (out / f"{n}.json").write_text(json.dumps(prof, ensure_ascii=False, indent=2))
            done += 1
            if done % 50 == 0:
                print(f"{done}/{len(orgnrs)}", file=sys.stderr)
        await asyncio.gather(*(one(n) for n in orgnrs))
    print(f"Wrote {len(orgnrs)} profiles to {out}")


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
    Path(out_file).write_text("\n".join(nums[:count]))
    print(f"Saved {min(count, len(nums))} org numbers to {out_file}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("discover"); d.add_argument("--count", type=int, default=1200); d.add_argument("--out", default="orgnrs.txt")
    r = sub.add_parser("run"); r.add_argument("--input", required=True); r.add_argument("--out", default="profiles")
    a = p.parse_args()
    asyncio.run(discover(a.count, a.out) if a.cmd == "discover" else run(a.input, a.out))