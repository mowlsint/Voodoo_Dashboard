---
name: "UAS / Air Activity"
about: "Drohnen/Flugaktivität über See/Küste/Hafen – inkl. Phase-Zero Bewertung"
title: "[UAS/Air] Kurztitle"
labels: ["D:DRONE_UAS","SRC:OSINT"]
assignees: []
---

## Zeit (UTC)
YYYY-MM-DD HH:MM UTC

## Region (REG:*)
REG:NORTH_SEA / REG:BALTIC / REG:ENGLISH_CHANNEL / REG:MEDITERRANEAN / REG:BLACK_SEA / REG:GLOBAL

## Geo (lat, lon) – Dezimalgrad
53.5100, 8.1500

## Ortstext / Area (optional)
z.B. Wilhelmshaven / Außenweser / Helgoland / Fehmarnbelt

---

## Beobachtung (Was wurde gesehen/gehört?)
- Kurz und präzise: Plattform, Höhe/Entfernung, Richtung, Dauer, Wiederholungen.
- Wenn unklar: klar als unklar markieren.

## Muster / Indikatoren (PAT:*)
- [ ] PAT:LOITERING
- [ ] PAT:ROUTE_DEVIATION
- [ ] PAT:DARK_ACTIVITY
- [ ] PAT:SURVEYING
- [ ] PAT:GNSS_JAM
- [ ] PAT:GNSS_SPOOF
- [ ] PAT:AIS_GAP
- [ ] PAT:STS_SUSPECT
- [ ] anderes: __________________

## Objektbezug (OBJ:*)
- [ ] OBJ:PORT
- [ ] OBJ:WINDFARM
- [ ] OBJ:CABLE
- [ ] OBJ:PIPELINE
- [ ] OBJ:VESSEL
- [ ] OBJ:OFFSHORE
- [ ] OBJ:SEA_AREA
- [ ] OBJ:VTS_WSV

## Plattform/Typ (wenn bekannt)
- UAS/Drone: (Typ/Größe/Farbe/Lichter/Noise)
- Fluggerät: (Heli/Prop/Jet/Unknown)

---

## Bewertung (belegt / plausibel / offen)
**Belegt:**  
- …

**Plausibel:**  
- …

**Offen / Prüffelder:**  
- …

---

## Confidence & Severity (Labels setzen)
**CONF:** LOW / MED / HIGH  
**SEV:** 1 / 2 / 3 / 4  

> Hinweis: CONF ist Quellen-/Faktenlage. SEV ist Auswirkung/Dringlichkeit.

## Phase Zero / Hybrid (Labels setzen)
- [ ] P0:SUSPECT
- [ ] P0:LOW
- [ ] P0:MED
- [ ] P0:HIGH

**Kurzbegründung (1–2 Sätze):**  
- Warum (nicht) Phase Zero? Welche TTP/Proxy/Timing/Objektbezug?

---

## Quellen (Links / Hinweise)
- Quelle 1:
- Quelle 2:
- (optional) Rohnotiz:
