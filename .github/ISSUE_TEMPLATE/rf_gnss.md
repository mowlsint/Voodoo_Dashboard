---
name: "RF / GNSS Anomaly"
about: "Jamming/Spoofing/Signal-Anomalien – maritime Relevanz"
title: "[RF/GNSS] Kurztitle"
labels: ["D:RF_SIGNAL","SRC:OSINT"]
assignees: []
---

## Zeit (UTC)
YYYY-MM-DD HH:MM UTC (Start)
YYYY-MM-DD HH:MM UTC (Ende, optional)

## Region (REG:*)
REG:NORTH_SEA / REG:BALTIC / REG:ENGLISH_CHANNEL / REG:MEDITERRANEAN / REG:BLACK_SEA / REG:GLOBAL

## Geo (lat, lon) – Dezimalgrad
54.1800, 7.8900

## Ortstext / Area (optional)
z.B. Helgoland Bight / Kadetrinne / Bornholm / Skagerrak

---

## Signal / Beobachtung
- Was genau? (GNSS-Jam, GNSS-Spoof, AIS-Anomalie, unbekanntes RF-Muster)
- Wer hat es gesehen? (Sensor/Receiver/SDR/Quelle)
- Dauer / Wiederholung / Muster

## Technische Details (wenn vorhanden)
- Frequenz / Band:
- Empfangsgerät / Pipeline:
- Signalstärke / SNR / Wasserfalldiagramm (Link/Attachment, falls möglich)
- Anomalie-Charakter: konstant / pulsed / sweeping / burst

## Muster / Indikatoren (PAT:*)
- [ ] PAT:GNSS_JAM
- [ ] PAT:GNSS_SPOOF
- [ ] PAT:AIS_GAP
- [ ] PAT:DARK_ACTIVITY
- [ ] PAT:ROUTE_DEVIATION
- [ ] PAT:LOITERING
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

---

## Bewertung (belegt / plausibel / offen)
**Belegt:**  
- …

**Plausibel:**  
- …

**Offen / Prüffelder:**  
- Welche Gegenhypothesen? (Wetter, Receiver-Fehler, Multipath, lokale Störer)
- Welche Zusatzdaten würden das verifizieren? (zweiter Sensor, Zeitserie, AIS-Korrelation, NAVWARN/Manöver)

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
- Warum wirkt es intentional/gerichtet? (Objektbezug, Timing, wiederkehrende Signatur)

---

## Quellen / Artefakte
- Quelle 1 (Link):
- Quelle 2 (Link):
- (optional) Screenshot/Log:
