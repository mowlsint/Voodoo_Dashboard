#!/usr/bin/env bash
set -euo pipefail

# Usage: ./scripts/setup_labels.sh owner/repo
# Example: ./scripts/setup_labels.sh mowlsint/Voodoo_Dashboard

REPO="${1:-}"
if [[ -z "$REPO" ]]; then
  echo "Usage: $0 owner/repo"
  exit 1
fi

# label|color|description
LABELS=$(cat <<'EOF'
D:AIS_TRACK|1f6feb|Track/Positions-/Verhaltensauffälligkeiten (Loitering, STS, AIS-Gap, Surveying)
D:VESSEL|1f6feb|Identität/Registrierung/Ownership/Flag/Insurance
D:RF_SIGNAL|1f6feb|RF/Funk/GNSS/NAVTEX/NAVWARN
D:DRONE_UAS|1f6feb|Drohnenaktivität (UAS) auf See/Offshore
D:AIR_ACTIVITY|1f6feb|Flugaktivität über See (MPA/ISR/Helos, Muster)
D:INCIDENT|1f6feb|Maritimer Zwischenfall (Collision/Fire/Grounding/Pollution)
D:SECURITY_CRIME|1f6feb|Security/Crime (Piracy/Boarding/Smuggling)
D:CYBER_OT|1f6feb|Cyber/OT Vorfälle (Vessel/Port/WSV/VTS)
D:INFRA_CI|1f6feb|Kritische Infrastruktur (Cable/Pipeline/Windfarm/Port)
D:NEWS_INTEL|1f6feb|News/Intel Hinweis (ohne eigene Feststellung)
D:SAR|1f6feb|Search & Rescue / Rettungs-/Medevac-Lagen
D:WEATHER_MET|1f6feb|Wetter-/Meteo-Bezug (Beobachtung, meto vessels, sensorische Auffälligkeiten)
D:SATELLITE|1f6feb|Satellitendaten (SAR/Optisch/IR) + Detektionen/Analysen

OBJ:VESSEL|8250df|Bezug zu einem Schiff/Schiffsgruppe
OBJ:PORT|8250df|Bezug Hafen/Terminal/Schleusen
OBJ:CABLE|8250df|Bezug Seekabel
OBJ:PIPELINE|8250df|Bezug Pipeline
OBJ:WINDFARM|8250df|Bezug Windpark/Offshore-Energie
OBJ:VTS_WSV|8250df|Bezug VTS/WSV/Schifffahrtsverwaltung
OBJ:WATERWAY|8250df|Bezug Wasserstraße/Fahrwasser/Schleusen/Kanäle
OBJ:OFFSHORE|8250df|Bezug Offshore-Anlage allgemein
OBJ:SEA_AREA|8250df|Bezug offenes Seegebiet/Ankerfeld/Übungsraum

V:SHADOW_FLEET|d73a4a|Schattenflotte / high-risk fleet patterns
V:SANCTIONS_EVASION|d73a4a|Sanktionsumgehung (konkret/verdächtig)

V:RUS_WARSHIP|d73a4a|Russische Kriegsschiffe
V:RUS_AUXILIARY|d73a4a|Russische Hilfs-/Logistik-/Support-Schiffe
V:RUS_RESEARCH|d73a4a|Russische Forschungs-/Vermessungsschifffahrt
V:RUS_FISHERY|d73a4a|Russische Fischerei (wenn relevant)
V:RUS_GOV|d73a4a|Russische staatliche/sonstige Gov vessels

V:AUTH_COAST_GUARD|0e8a16|Küstenwache/Coast Guard
V:AUTH_CUSTOMS|0e8a16|Zoll/Customs
V:AUTH_POLICE|0e8a16|Wasserschutz/Polizei
V:AUTH_NAVY|0e8a16|Navy (nicht Russland)
V:SAR_UNIT|0e8a16|SAR Einheit

V:FISHING|0e8a16|Fischerei/Fishing vessels
V:TANKER|0e8a16|Tanker
V:CARGO|0e8a16|Cargo/Container/Bulk
V:PASSENGER|0e8a16|Passagierfähre/Cruise
V:TUG_PILOT|0e8a16|Schlepper/Pilot
V:SERVICE_OFFSHORE|0e8a16|Offshore Service Vessels
V:OTHER|0e8a16|Sonstige

PAT:LOITERING|fbca04|Loitering/Herumliegen/Pattern-of-life auffällig
PAT:ANCHORING_ODD|fbca04|Auffällige Ankerzeit/-ort
PAT:ROUTE_DEVIATION|fbca04|Abweichung von erwarteter Route
PAT:STS_SUSPECT|fbca04|STS-Verdacht / Rendezvous-Transfer
PAT:AIS_GAP|fbca04|AIS unterbrochen/off
PAT:AIS_SPOOF|fbca04|AIS Spoofing Indizien
PAT:IDENTITY_ANOM|fbca04|Identitätsauffälligkeit (MMSI/IMO/Callsign)
PAT:RENDEZVOUS|fbca04|Wiederholtes Treffen/Annäherung
PAT:SURVEYING|fbca04|Survey/Parallelfahrten/typische Vermessungsmuster
PAT:DARK_ACTIVITY|fbca04|„dark“ Verhalten (AIS off + plausible concealment)
PAT:WEATHER_SHIELD|fbca04|Wetter als Deckung/Window (Storm cover / low visibility timing)

RF:GNSS_JAM|5319e7|GNSS Jamming Indizien
RF:GNSS_SPOOF|5319e7|GNSS Spoofing Indizien
RF:UNKNOWN_TX|5319e7|Unbekannter Sender
RF:COMMS_ANOM|5319e7|Funk-/Comms-Anomalie
RF:NAVTEX|5319e7|NAVTEX
RF:NAVWARN|5319e7|NAVWARN / Navigational Warnings

UAS:SIGHTING|c2e0c6|UAS Sichtung
UAS:INTERDICT|c2e0c6|UAS Abwehr/Interdiction
UAS:REMOTE_ID|c2e0c6|Remote-ID / Identifizierung

SAT:SAR|0aa2c0|SAR (Radar) – Wolkenunabhängig, Schiffs-/Wake-/Strukturhinweise
SAT:OPTICAL|0aa2c0|Optisch – visuelle Bestätigung (wetterabhängig)
SAT:IR_THERMAL|0aa2c0|IR/Thermal – Wärme/Hotspots (eingeschränkt je Sensor)
SAT:CHANGE_DET|0aa2c0|Change detection (neu/weg/Bewegung/Objekte)
SAT:WAKE|0aa2c0|Wake/Bewegungsspur Indizien
SAT:VESSEL_DET|0aa2c0|Vessel detection (Detektion/Count/Cluster)
SAT:CI_MONITOR|0aa2c0|CI Monitoring (Kabel/Ports/Windparks/Pipelines)

REG:NORTH_SEA|0052cc|Nordsee
REG:BALTIC_SEA|0052cc|Ostsee
REG:MED|0052cc|Mittelmeer
REG:ATLANTIC_NE|0052cc|NE Atlantik/Channel/Biscay
REG:INLAND_WATERWAYS|0052cc|Binnenwasserstraßen/Schleusen/Kanäle

SEV:1|000000|Low
SEV:2|000000|Moderate
SEV:3|000000|High
SEV:4|000000|Critical

CONF:LOW|6a737d|Faktenlage unsicher
CONF:MED|6a737d|Faktenlage plausibel
CONF:HIGH|6a737d|Faktenlage bestätigt/hoch

P0:SUSPECT|b60205|Phase Zero / Hybrid-Verdacht (Interpretationsebene)
P0:LOW|b60205|Hybrid-Deutung low
P0:MED|b60205|Hybrid-Deutung medium
P0:HIGH|b60205|Hybrid-Deutung high

SRC:OSINT|0b1f2a|OSINT/Analyse
SRC:OFFICIAL|0b1f2a|Behördlich/amtlich
SRC:MEDIA|0b1f2a|Medien
SRC:SOCIAL|0b1f2a|Social/Spotter
SRC:SENSOR|0b1f2a|Sensor/SDR/AIS Logs
SRC:PARTNER|0b1f2a|Partnerhinweis
SRC:SATELLITE|0b1f2a|Satellitenprodukt/Imagery/Derived detection
EOF
)

echo "Creating labels in $REPO ..."
echo "$LABELS" | while IFS='|' read -r name color desc; do
  [[ -z "${name// }" ]] && continue

  if gh label list -R "$REPO" --search "$name" --limit 200 | grep -q "^$name"; then
    echo " - exists: $name"
  else
    echo " - create: $name"
    gh label create "$name" -R "$REPO" --color "$color" --description "$desc" || true
  fi
done

echo "Done."