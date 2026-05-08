---
name: "Vessel / Track / Shadow Fleet"
about: "AIS Track, Loitering, Surveying, Shadow Fleet, Research, Warships"
title: "[Vessel/Track] Name/IMO/MMSI – Kurztitle"
labels: ["D:AIS_TRACK","SRC:AIS"]
assignees: []
---

## Zeit (UTC)
YYYY-MM-DD HH:MM UTC (Start)
YYYY-MM-DD HH:MM UTC (Ende, optional)

## Region (REG:*)
REG:NORTH_SEA / REG:BALTIC / REG:ENGLISH_CHANNEL / REG:MEDITERRANEAN / REG:BLACK_SEA / REG:GLOBAL

## Geo (lat, lon) – Dezimalgrad
54.1800, 7.8900

## Vessel
- Name:
- IMO (optional):
- MMSI (optional):
- Flag (optional):
- Callsign (optional):

## Kurs/Speed/Status (wenn vorhanden)
- COG/SOG:
- NavStatus:
- AIS-Quelle (Link):

---

## Muster / Indikatoren (PAT:*)
- [ ] PAT:LOITERING
- [ ] PAT:SURVEYING
- [ ] PAT:AIS_GAP
- [ ] PAT:DARK_ACTIVITY
- [ ] PAT:STS_SUSPECT
- [ ] PAT:ROUTE_DEVIATION
- [ ] PAT:GNSS_JAM
- [ ] PAT:GNSS_SPOOF
- [ ] anderes: __________________

## Vessel-Klasse (V:*)
- [ ] V:SHADOW_FLEET
- [ ] V:RUS_RESEARCH
- [ ] V:RUS_WARSHIP
- [ ] V:AUTH_GOV
- [ ] V:FISHING
- [ ] anderes: __________________

## Objektbezug (OBJ:*)
- [ ] OBJ:CABLE
- [ ] OBJ:PIPELINE
- [ ] OBJ:WINDFARM
- [ ] OBJ:PORT
- [ ] OBJ:VESSEL
- [ ] OBJ:OFFSHORE
- [ ] OBJ:SEA_AREA
- [ ] OBJ:VTS_WSV

---

## Kurzbeschreibung (Was fällt auf?)
- Track/Verhalten kurz beschreiben (Was ist abweichend? was ist normal?)
- Distanz zum Objekt (sofern ableitbar)
- Wiederholung / bekannte Muster / Vorfälle

## Bewertung (belegt / plausibel / offen)
**Belegt:**  
- …

**Plausibel:**  
- …

**Offen / Prüffelder:**  
- Alternativerklärungen (Wetter/Warteposition/Technik/Route)
- Welche Daten fehlen? (Owner/ISM/Inspections/Port calls)

---

## Confidence & Severity (Labels setzen)
**CONF:** LOW / MED / HIGH  
**SEV:** 1 / 2 / 3 / 4

## Phase Zero / Hybrid (Labels setzen)
- [ ] P0:SUSPECT
- [ ] P0:LOW
- [ ] P0:MED
- [ ] P0:HIGH

**Kurzbegründung:**  
- Warum gerichtetes Verhalten? (Objektbezug, Timing, Muster, Flotte)

---

## Quellen
- AIS-Link:
- weitere Quelle:
