"""Circuits the Pit Wall map can draw, picked with ``PITWALL_CIRCUIT`` or ``--circuit``.

Each key has ``static/circuits/<key>.geojson``: the circuit centreline from
bacinger/f1-circuits (MIT), ordered in race direction and starting at the
start/finish line, which is what lets the page place a car from its lap fraction.
The choice is purely visual — the simulated race (and every lab) is the same
Silverstone race whichever map is shown.
"""

from __future__ import annotations

import os

CIRCUIT_ENV_VAR = "PITWALL_CIRCUIT"
DEFAULT_CIRCUIT = "silverstone"

# key -> (header label, full circuit name)
CIRCUITS: dict[str, tuple[str, str]] = {
    "abu_dhabi": ("Abu Dhabi", "Yas Marina Circuit"),
    "austin": ("Austin", "Circuit of the Americas"),
    "baku": ("Baku", "Baku City Circuit"),
    "barcelona": ("Barcelona", "Circuit de Barcelona-Catalunya"),
    "budapest": ("Budapest", "Hungaroring"),
    "buenos_aires": ("Buenos Aires", "Autódromo Oscar y Juan Gálvez"),
    "estoril": ("Estoril", "Autódromo do Estoril"),
    "hockenheim": ("Hockenheim", "Hockenheimring"),
    "imola": ("Imola", "Autodromo Enzo e Dino Ferrari"),
    "indianapolis": ("Indianapolis", "Indianapolis Motor Speedway"),
    "istanbul": ("Istanbul", "Intercity Istanbul Park"),
    "jacarepagua": ("Jacarepaguá", "Autódromo Internacional Nelson Piquet"),
    "jeddah": ("Jeddah", "Jeddah Corniche Circuit"),
    "kyalami": ("Kyalami", "Kyalami Grand Prix Circuit"),
    "las_vegas": ("Las Vegas", "Las Vegas Street Circuit"),
    "lusail": ("Lusail", "Losail International Circuit"),
    "madrid": ("Madrid", "Circuito de Madring"),
    "magny_cours": ("Magny-Cours", "Circuit de Nevers Magny-Cours"),
    "melbourne": ("Melbourne", "Albert Park Circuit"),
    "mexico_city": ("Mexico City", "Autódromo Hermanos Rodríguez"),
    "miami": ("Miami", "Miami International Autodrome"),
    "monaco": ("Monaco", "Circuit de Monaco"),
    "montreal": ("Montreal", "Circuit Gilles-Villeneuve"),
    "monza": ("Monza", "Autodromo Nazionale Monza"),
    "mugello": ("Mugello", "Autodromo Internazionale del Mugello"),
    "nurburgring": ("Nürburgring", "Nürburgring"),
    "paul_ricard": ("Paul Ricard", "Circuit Paul Ricard"),
    "portimao": ("Portimão", "Autódromo Internacional do Algarve"),
    "sakhir": ("Sakhir", "Bahrain International Circuit"),
    "sao_paulo": ("São Paulo", "Autódromo José Carlos Pace - Interlagos"),
    "sepang": ("Sepang", "Sepang International Circuit"),
    "shanghai": ("Shanghai", "Shanghai International Circuit"),
    "silverstone": ("Silverstone", "Silverstone Circuit"),
    "singapore": ("Singapore", "Marina Bay Street Circuit"),
    "sochi": ("Sochi", "Sochi Autodrom"),
    "spa_francorchamps": ("Spa-Francorchamps", "Circuit de Spa-Francorchamps"),
    "spielberg": ("Spielberg", "Red Bull Ring"),
    "suzuka": ("Suzuka", "Suzuka International Racing Course"),
    "watkins_glen": ("Watkins Glen", "Watkins Glen International"),
    "zandvoort": ("Zandvoort", "Circuit Zandvoort"),
}


def resolve_circuit(explicit: str | None = None) -> str:
    """``--circuit`` wins, then ``PITWALL_CIRCUIT``, then Silverstone."""
    key = (explicit or os.environ.get(CIRCUIT_ENV_VAR) or DEFAULT_CIRCUIT).strip().lower()
    if key not in CIRCUITS:
        raise SystemExit(
            f"Unknown circuit {key!r} (from {'--circuit' if explicit else CIRCUIT_ENV_VAR}). "
            f"Choose one of: {', '.join(sorted(CIRCUITS))}"
        )
    return key
